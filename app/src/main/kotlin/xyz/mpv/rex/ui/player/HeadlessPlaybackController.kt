package xyz.mpv.rex.ui.player

import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.util.Log
import android.util.Xml
import android.view.ViewGroup
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import xyz.mpv.rex.R
import xyz.mpv.rex.preferences.PlayerPreferences
import xyz.mpv.rex.ui.browser.miniplayer.MiniPlayerStateManager
import xyz.mpv.rex.utils.history.RecentlyPlayedOps
import xyz.mpv.rex.utils.media.MediaThumbnailUtils
import `is`.xyz.mpv.MPVLib
import `is`.xyz.mpv.MPVNode
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.xmlpull.v1.XmlPullParser
import java.io.File

/**
 * Headless MPV playback controller for direct mini player mode.
 *
 * Creates an off-window [MPVView] and plays audio-first with `vo=null`, so tapping a
 * media item starts playback in the bottom mini player bar WITHOUT ever launching
 * [PlayerActivity] — eliminating the window-transition flicker.
 *
 * When the user expands the mini player, [PlayerActivity] launches with
 * `attach_existing_session=true` and takes over the (still-running) global MPV instance.
 * At that point this controller relinquishes ownership via [detachForHandoff] WITHOUT
 * destroying MPV.
 *
 * ## Ownership Model
 * Exactly ONE of {HeadlessPlaybackController, PlayerActivity} owns the global MPV instance
 * and is responsible for [MPVLib.destroy]. `MPVLib` is a process-global native singleton, so
 * only one `MPVLib.create()` may be live at a time.
 */
class HeadlessPlaybackController(private val appContext: Context) : KoinComponent {
  private val miniPlayerStateManager: MiniPlayerStateManager by inject()
  private val playerPreferences: PlayerPreferences by inject()

  private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main)

  private var mpvView: MPVView? = null

  @Volatile
  var isSessionActive: Boolean = false
    private set

  /**
   * True while this controller owns the process-global MPV instance, including when playback
   * has been stopped and MPV is deliberately kept idle for safe reuse.
   */
  @Volatile
  var ownsNativeSession: Boolean = false
    private set

  // Metadata for handoff to PlayerActivity.
  @Volatile
  var activeUris: List<Uri> = emptyList()
    private set

  @Volatile
  var activeIndex: Int = 0
    private set

  @Volatile
  var activeTitle: String = ""
    private set

  @Volatile
  var activeLaunchSource: String = "direct_mini_player"
    private set

  @Volatile
  var activePlaylistId: Int? = null
    private set

  @Volatile
  private var shuffledIndices: List<Int> = emptyList()

  @Volatile
  private var shuffledPosition: Int = -1

  private var resumeObserver: MPVLib.EventObserver? = null
  private var headlessObserver: MPVLib.EventObserver? = null

  private fun generateShuffledIndices(startIdx: Int) {
    if (activeUris.isEmpty()) return
    val indices = activeUris.indices.filter { it != startIdx }.shuffled()
    shuffledIndices = listOf(startIdx) + indices
    shuffledPosition = 0
  }

  /**
   * Starts headless playback of [uris] beginning at [startIndex].
   *
   * @param resumePositionSec position (seconds) to resume the first file at, or 0 to start fresh.
   */
  fun startHeadless(
    uris: List<Uri>,
    startIndex: Int,
    title: String,
    artist: String = "",
    resumePositionSec: Int = 0,
    launchSource: String = "direct_mini_player",
    playlistId: Int? = null,
  ) {
    if (uris.isEmpty() || startIndex < 0 || startIndex >= uris.size) {
      Log.w(TAG, "startHeadless: invalid uris/startIndex")
      return
    }

    PlayerActivity.finishBackgroundInstance()

    activeUris = uris
    activeIndex = startIndex
    activeTitle = title
    activeLaunchSource = launchSource
    activePlaylistId = playlistId

    if (playerPreferences.shuffleEnabled.get()) {
      generateShuffledIndices(startIndex)
    } else {
      shuffledIndices = emptyList()
      shuffledPosition = -1
    }

    // Set handlers for swipe/button next/previous and close in MiniPlayer
    miniPlayerStateManager.onNextHandler = { playNext() }
    miniPlayerStateManager.onPreviousHandler = { playPrevious() }
    miniPlayerStateManager.onCloseHandler = { stop() }

    val isMpvNativeInitialized = MPVLifecycleLock.isNativeInitialized

    // A session is already running under this controller or native MPV is alive: reuse live MPV.
    if ((isSessionActive && mpvView != null) || isMpvNativeInitialized) {
      Log.d(TAG, "startHeadless: reusing live MPV instance")
      if (mpvView == null) {
        val view = createOffWindowView()
        mpvView = view
        view.attachToExistingSession()
        MPVLib.setPropertyString("vo", "null")
      }
      // BaseMPVView initializes with idle=once. A stopped headless session must stay alive so
      // the next loadfile can reuse the process-global native instance.
      MPVLib.setPropertyString("idle", "yes")
      ownsNativeSession = true
      isSessionActive = true
      setupMpvObserver()
      playItem(startIndex, resumePositionSec)
      startService(activeTitle, artist)
      return
    }

    scope.launch {
      // Prepare config/scripts off the main thread BEFORE MPV init (mpv loads scripts on init).
      withContext(Dispatchers.IO) {
        runCatching { MpvConfigSync.prepare(appContext) }
          .onFailure { e -> Log.e(TAG, "Config prepare failed", e) }
      }

      val view = createOffWindowView()
      mpvView = view

      runCatching {
        view.initialize(appContext.filesDir.path, appContext.cacheDir.path)
        // Audio-first: no surface exists off-window, so disable video output.
        MPVLib.setPropertyString("vo", "null")
        MPVLib.setPropertyString("idle", "yes")
      }.onFailure { e ->
        Log.e(TAG, "MPV initialize failed", e)
        runCatching { view.destroy() }
        mpvView = null
        return@launch
      }

      ownsNativeSession = true
      isSessionActive = true
      setupMpvObserver()
      playItem(startIndex, resumePositionSec)
      startService(activeTitle, artist)
      Log.d(TAG, "Headless session started: $title")
    }
  }

  fun onShuffleToggled(enabled: Boolean) {
    if (enabled) {
      generateShuffledIndices(activeIndex)
    } else {
      shuffledIndices = emptyList()
      shuffledPosition = -1
    }
    if (activeUris.isNotEmpty()) {
      updateStateAndMetadata(activeIndex)
    }
  }

  fun onRepeatModeChanged(repeatMode: RepeatMode) {
    if (activeUris.isNotEmpty()) {
      updateStateAndMetadata(activeIndex)
    }
  }

  fun playNext(isAutoAdvance: Boolean = false) {
    if (activeUris.isEmpty()) return
    val isShuffle = playerPreferences.shuffleEnabled.get()
    val repeatMode = playerPreferences.repeatMode.get()

    if (isShuffle) {
      if (shuffledIndices.isEmpty() || shuffledIndices.size != activeUris.size) {
        generateShuffledIndices(activeIndex)
      }
      val nextPos = shuffledPosition + 1
      if (nextPos < shuffledIndices.size) {
        shuffledPosition = nextPos
        playItem(shuffledIndices[nextPos])
      } else if (repeatMode == RepeatMode.ALL) {
        generateShuffledIndices(activeIndex)
        shuffledPosition = 0
        playItem(shuffledIndices[0])
      } else if (isAutoAdvance) {
        stop()
      }
    } else {
      val nextIndex = activeIndex + 1
      if (nextIndex in activeUris.indices) {
        playItem(nextIndex)
      } else if (repeatMode == RepeatMode.ALL) {
        playItem(0)
      } else if (isAutoAdvance) {
        stop()
      }
    }
  }

  fun playPrevious() {
    if (activeUris.isEmpty()) return
    val isShuffle = playerPreferences.shuffleEnabled.get()
    val repeatMode = playerPreferences.repeatMode.get()

    if (isShuffle) {
      if (shuffledIndices.isEmpty() || shuffledIndices.size != activeUris.size) {
        generateShuffledIndices(activeIndex)
      }
      val prevPos = shuffledPosition - 1
      if (prevPos >= 0) {
        shuffledPosition = prevPos
        playItem(shuffledIndices[prevPos])
      } else if (repeatMode == RepeatMode.ALL) {
        shuffledPosition = shuffledIndices.size - 1
        playItem(shuffledIndices.last())
      }
    } else {
      val prevIndex = activeIndex - 1
      if (prevIndex in activeUris.indices) {
        playItem(prevIndex)
      } else if (repeatMode == RepeatMode.ALL) {
        playItem(activeUris.size - 1)
      }
    }
  }

  fun playItem(index: Int, resumePositionSec: Int = 0) {
    if (index !in activeUris.indices) return
    activeIndex = index
    val isShuffle = playerPreferences.shuffleEnabled.get()
    if (isShuffle) {
      val pos = shuffledIndices.indexOf(index)
      if (pos != -1) {
        shuffledPosition = pos
      } else {
        generateShuffledIndices(index)
      }
    }

    val uri = activeUris[index]
    val title = deriveTitle(uri)
    activeTitle = title

    val playable = uri.resolveUri(appContext) ?: uri.toString()
    runCatching { MPVLib.command("loadfile", playable) }
    runCatching { MPVLib.setPropertyBoolean("pause", false) }

    scheduleResume(resumePositionSec)
    updateStateAndMetadata(index)

    RecentlyPlayedOps.recordPlaybackStart(
      uri = uri,
      fileName = title,
      launchSource = activeLaunchSource,
      playlistId = activePlaylistId,
    )
  }

  private fun updateStateAndMetadata(index: Int) {
    val currentUri = activeUris[index]
    val currentTitle = deriveTitle(currentUri)

    val isShuffle = playerPreferences.shuffleEnabled.get()
    val repeatMode = playerPreferences.repeatMode.get()

    val nextIdx: Int? = if (isShuffle) {
      if (shuffledIndices.isEmpty() || shuffledIndices.size != activeUris.size) {
        generateShuffledIndices(index)
      }
      val nextPos = shuffledPosition + 1
      if (nextPos < shuffledIndices.size) shuffledIndices[nextPos]
      else if (repeatMode == RepeatMode.ALL && shuffledIndices.isNotEmpty()) shuffledIndices[0]
      else null
    } else {
      val nextPos = index + 1
      if (nextPos < activeUris.size) nextPos
      else if (repeatMode == RepeatMode.ALL && activeUris.isNotEmpty()) 0
      else null
    }

    val prevIdx: Int? = if (isShuffle) {
      if (shuffledIndices.isEmpty() || shuffledIndices.size != activeUris.size) {
        generateShuffledIndices(index)
      }
      val prevPos = shuffledPosition - 1
      if (prevPos >= 0) shuffledIndices[prevPos]
      else if (repeatMode == RepeatMode.ALL && shuffledIndices.isNotEmpty()) shuffledIndices.last()
      else null
    } else {
      val prevPos = index - 1
      if (prevPos >= 0) prevPos
      else if (repeatMode == RepeatMode.ALL && activeUris.isNotEmpty()) activeUris.size - 1
      else null
    }

    val hasNextItem = nextIdx != null
    val hasPrevItem = prevIdx != null

    val nextUri = nextIdx?.let { activeUris.getOrNull(it) }
    val prevUri = prevIdx?.let { activeUris.getOrNull(it) }

    val nextTitle = nextUri?.let { deriveTitle(it) }
    val prevTitle = prevUri?.let { deriveTitle(it) }

    miniPlayerStateManager.updateState(
      isPlaybackActive = true,
      title = currentTitle,
      videoPath = currentUri.toString(),
      hasNext = hasNextItem,
      hasPrevious = hasPrevItem,
      nextTitle = nextTitle,
      prevTitle = prevTitle,
      nextThumbnail = null,
      prevThumbnail = null,
      shuffleEnabled = isShuffle,
      repeatMode = repeatMode,
    )

    scope.launch(Dispatchers.IO) {
      val mainThumb = MediaThumbnailUtils.extractThumbnailOrCoverArt(appContext, currentUri)
      val nextThumb = nextUri?.let { MediaThumbnailUtils.extractThumbnailOrCoverArt(appContext, it) }
      val prevThumb = prevUri?.let { MediaThumbnailUtils.extractThumbnailOrCoverArt(appContext, it) }

      MediaPlaybackService.thumbnail = mainThumb
      withContext(Dispatchers.Main) {
        miniPlayerStateManager.updateState(
          thumbnail = mainThumb,
          nextThumbnail = nextThumb,
          prevThumbnail = prevThumb,
        )
      }
    }
  }

  /** Seeks to [resumePositionSec] once the first file finishes loading, then self-detaches. */
  private fun scheduleResume(resumePositionSec: Int) {
    resumeObserver?.let { runCatching { MPVLib.removeObserver(it) } }
    resumeObserver = null
    if (resumePositionSec <= 3) return

    val observer = object : MPVLib.EventObserver {
      override fun event(eventId: Int, data: MPVNode) {
        if (eventId == MPVLib.MpvEvent.MPV_EVENT_FILE_LOADED) {
          runCatching { MPVLib.setPropertyInt("time-pos", resumePositionSec) }
          val self = this
          scope.launch {
            runCatching { MPVLib.removeObserver(self) }
            if (resumeObserver === self) resumeObserver = null
          }
        }
      }
      override fun eventProperty(property: String) {}
      override fun eventProperty(property: String, value: Long) {}
      override fun eventProperty(property: String, value: Boolean) {}
      override fun eventProperty(property: String, value: String) {}
      override fun eventProperty(property: String, value: Double) {}
      override fun eventProperty(property: String, value: MPVNode) {}
    }
    resumeObserver = observer
    runCatching { MPVLib.addObserver(observer) }
  }

  private fun deriveTitle(uri: Uri): String {
    return if (uri.scheme == "file") {
      File(uri.path ?: "").name.ifBlank { uri.lastPathSegment ?: "Media" }
    } else {
      uri.lastPathSegment ?: "Media"
    }
  }

  private fun startService(title: String, artist: String) {
    MediaPlaybackService.createNotificationChannel(appContext)
    val intent = Intent(appContext, MediaPlaybackService::class.java).apply {
      putExtra("media_title", title)
      putExtra("media_artist", artist)
      putExtra("direct_mini_player", true)
    }
    runCatching {
      if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
        appContext.startForegroundService(intent)
      } else {
        appContext.startService(intent)
      }
    }.onFailure { e -> Log.e(TAG, "Failed to start MediaPlaybackService", e) }
  }

  private fun setupMpvObserver() {
    removeMpvObserver()
    val observer = object : MPVLib.EventObserver {
      override fun eventProperty(property: String, value: Boolean) {
        if (property == "eof-reached" && value) {
          if (isSessionActive && ownsNativeSession) {
            handleEndOfFile()
          }
        }
      }
      override fun event(eventId: Int, data: MPVNode) {}
      override fun eventProperty(property: String) {}
      override fun eventProperty(property: String, value: Long) {}
      override fun eventProperty(property: String, value: String) {}
      override fun eventProperty(property: String, value: Double) {}
      override fun eventProperty(property: String, value: MPVNode) {}
    }
    headlessObserver = observer
    runCatching { MPVLib.addObserver(observer) }
  }

  private fun removeMpvObserver() {
    headlessObserver?.let { runCatching { MPVLib.removeObserver(it) } }
    headlessObserver = null
  }

  private fun handleEndOfFile() {
    val repeatMode = playerPreferences.repeatMode.get()
    if (repeatMode == RepeatMode.ONE) {
      runCatching { MPVLib.command("seek", "0", "absolute") }
      runCatching { MPVLib.setPropertyBoolean("pause", false) }
    } else {
      playNext(isAutoAdvance = true)
    }
  }

  /**
   * Relinquishes ownership of the live MPV instance to [PlayerActivity] WITHOUT tearing it down.
   * Called just before launching the full-screen player so playback continues seamlessly.
   */
  fun detachForHandoff() {
    resumeObserver?.let { runCatching { MPVLib.removeObserver(it) } }
    resumeObserver = null
    removeMpvObserver()
    miniPlayerStateManager.onNextHandler = null
    miniPlayerStateManager.onPreviousHandler = null
    miniPlayerStateManager.onCloseHandler = null
    // Drop our surface callback but keep the global MPV instance alive for PlayerActivity.
    mpvView?.let { runCatching { it.holder.removeCallback(it) } }
    mpvView = null
    ownsNativeSession = false
    isSessionActive = false
    Log.d(TAG, "Detached headless session for handoff")
  }

  /**
   * Takes ownership back after a [PlayerActivity] that inherited this controller's native
   * session exits. The instance is deliberately retained idle: destroying it while the GPU VO
   * is still releasing the activity surface can race libmpv's render thread on Android.
   */
  fun retainAfterPlayerExit() {
    resumeObserver?.let { runCatching { MPVLib.removeObserver(it) } }
    resumeObserver = null
    removeMpvObserver()
    miniPlayerStateManager.onNextHandler = null
    miniPlayerStateManager.onPreviousHandler = null
    miniPlayerStateManager.onCloseHandler = null

    runCatching { MPVLib.setPropertyString("idle", "yes") }
    runCatching { MPVLib.setPropertyBoolean("pause", true) }
    runCatching { MPVLib.command("stop") }
    runCatching { MPVLib.setPropertyString("vo", "null") }
    runCatching { MPVLib.setPropertyString("force-window", "no") }
    runCatching { MPVLib.detachSurface() }

    mpvView = null
    ownsNativeSession = true
    isSessionActive = false
    activeUris = emptyList()
    activeIndex = 0
    activeTitle = ""
    activePlaylistId = null
    shuffledIndices = emptyList()
    shuffledPosition = -1
    Log.d(TAG, "Retained inherited MPV session idle after player exit")
  }

  /**
   * Stops headless playback without destroying the process-global MPV instance.
   *
   * The service removes its MPV observer asynchronously from `onDestroy()`. Destroying MPV here
   * would let that cleanup call back into an already-freed native singleton, which can terminate
   * the process with SIGSEGV/SIGKILL. Keeping MPV idle also makes the next headless or full-screen
   * playback start reuse the existing instance safely.
   */
  fun stop() {
    resumeObserver?.let { runCatching { MPVLib.removeObserver(it) } }
    resumeObserver = null
    removeMpvObserver()
    miniPlayerStateManager.onNextHandler = null
    miniPlayerStateManager.onPreviousHandler = null
    miniPlayerStateManager.onCloseHandler = null

    if (ownsNativeSession || isSessionActive || mpvView != null) {
      // Set persistent idle before stop; idle=once may emit MPV_EVENT_SHUTDOWN here and leave
      // the retained handle unable to accept the next loadfile command.
      runCatching { MPVLib.setPropertyString("idle", "yes") }
      runCatching { MPVLib.setPropertyBoolean("pause", true) }
      runCatching { MPVLib.command("stop") }
      runCatching { MPVLib.setPropertyString("vo", "null") }
    }

    isSessionActive = false
    activeUris = emptyList()
    activeIndex = 0
    activeTitle = ""
    shuffledIndices = emptyList()
    shuffledPosition = -1
    Log.d(TAG, "Headless playback stopped; native MPV retained idle")
  }

  private fun createOffWindowView(): MPVView {
    val parser = appContext.resources.getLayout(R.layout.shorts_dummy_layout)
    var type: Int
    while (parser.next().also { type = it } != XmlPullParser.START_TAG &&
      type != XmlPullParser.END_DOCUMENT
    ) { /* advance to first tag */ }
    val attrs = Xml.asAttributeSet(parser)
    return MPVView(appContext, attrs).apply {
      layoutParams = ViewGroup.LayoutParams(
        ViewGroup.LayoutParams.MATCH_PARENT,
        ViewGroup.LayoutParams.MATCH_PARENT,
      )
    }
  }

  companion object {
    private const val TAG = "HeadlessPlayback"
  }
}
