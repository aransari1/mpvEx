package app.marlboroadvance.mpvex.domain.thumbnail

import android.content.Context
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.media.MediaMetadataRetriever
import android.os.Build
import android.util.Log
import android.util.LruCache
import androidx.core.graphics.scale // Required for the .scale() optimization
import app.marlboroadvance.mpvex.domain.media.model.Video
import app.marlboroadvance.mpvex.utils.media.MediaInfoOps
import `is`.xyz.mpv.FastThumbnails
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File
import java.io.FileOutputStream
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.max

class ThumbnailRepository(
  private val context: Context,
) {
  private val appearancePreferences by lazy {
    org.koin.java.KoinJavaComponent.get<app.marlboroadvance.mpvex.preferences.AppearancePreferences>(
      app.marlboroadvance.mpvex.preferences.AppearancePreferences::class.java
    )
  }
  private val diskCacheDimension = 1024
  private val diskJpegQuality = 100
  private val memoryCache: LruCache<String, Bitmap>
  private val diskDir: File = File(context.filesDir, "thumbnails").apply { mkdirs() }
  private val ongoingOperations = ConcurrentHashMap<String, Deferred<Bitmap?>>()

  private val repositoryScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
  private val maxconcurrentfolders = 3

  private data class FolderState(
    val signature: String,
    @Volatile var nextIndex: Int = 0,
  )

  private val folderStates = ConcurrentHashMap<String, FolderState>()
  private val folderJobs = ConcurrentHashMap<String, Job>()

  private val useMediaStoreForVideo = ConcurrentHashMap<String, Boolean>()

  private val _thumbnailReadyKeys = MutableSharedFlow<String>(extraBufferCapacity = 256)
  val thumbnailReadyKeys: SharedFlow<String> = _thumbnailReadyKeys.asSharedFlow()

  init {
    val maxMemoryKb = (Runtime.getRuntime().maxMemory() / 1024L).toInt()
    val cacheSizeKb = maxMemoryKb / 6
    memoryCache = object : LruCache<String, Bitmap>(cacheSizeKb) {
      override fun sizeOf(key: String, value: Bitmap): Int = value.byteCount / 1024
    }
  }

  suspend fun getThumbnail(
    video: Video,
    widthPx: Int,
    heightPx: Int,
  ): Bitmap? = withContext(Dispatchers.IO) {
    val key = thumbnailKey(video, widthPx, heightPx)

    if (isNetworkUrl(video.path) && !appearancePreferences.showNetworkThumbnails.get()) {
      return@withContext null
    }

    synchronized(memoryCache) { memoryCache.get(key) }?.let { return@withContext it }

    ongoingOperations[key]?.let { return@withContext it.await() }

    val deferred = async {
      try {
        loadFromDisk(video)?.let { thumbnail ->
          synchronized(memoryCache) { memoryCache.put(key, thumbnail) }
          _thumbnailReadyKeys.tryEmit(key)
          return@async thumbnail
        }

        if (isNetworkUrl(video.path) && !appearancePreferences.showNetworkThumbnails.get()) {
          return@async null
        }

        val videoKey = videoBaseKey(video)

        // Priority: External -> Embedded -> First Frame -> FastThumbnails -> MediaStore
        val thumbnail = generateFromEmbeddedOrFirstFrame(video, diskCacheDimension)
          ?: if (useMediaStoreForVideo.containsKey(videoKey)) {
            generateWithMediaStore(video, diskCacheDimension)
          } else {
            val fastResult = generateWithFastThumbnails(video, diskCacheDimension)
            if (fastResult == null) {
              useMediaStoreForVideo[videoKey] = true
              generateWithMediaStore(video, diskCacheDimension)
            } else {
              fastResult
            }
          }

        if (thumbnail == null) return@async null

        synchronized(memoryCache) { memoryCache.put(key, thumbnail) }
        _thumbnailReadyKeys.tryEmit(key)
        writeToDisk(video, thumbnail)

        thumbnail
      } finally {
        ongoingOperations.remove(key)
      }
    }

    ongoingOperations[key] = deferred
    return@withContext deferred.await()
  }

  /**
   * Logic for External Files, Embedded Covers, and First Frame fallback.
   */
  private suspend fun generateFromEmbeddedOrFirstFrame(video: Video, dimension: Int): Bitmap? {
    return withContext(Dispatchers.IO) {
      // 1. External Check (Local Only)
      if (!isNetworkUrl(video.path)) {
        try {
          val videoFile = File(video.path)
          val parentDir = videoFile.parentFile

          val externalCover = parentDir?.listFiles()?.find { file ->
            val name = file.nameWithoutExtension.lowercase()
            val fullName = file.name.lowercase()
            name == "cover" || name == "poster" || name == "folder" ||
              fullName == "${videoFile.nameWithoutExtension.lowercase()}.jpg" ||
              fullName == "${videoFile.nameWithoutExtension.lowercase()}.png"
          }

          if (externalCover != null && externalCover.exists()) {
            val options = BitmapFactory.Options().apply { inJustDecodeBounds = true }
            BitmapFactory.decodeFile(externalCover.absolutePath, options)
            options.inSampleSize = calculateInSampleSize(options, dimension, dimension)
            options.inJustDecodeBounds = false

            val bitmap = BitmapFactory.decodeFile(externalCover.absolutePath, options)
            if (bitmap != null) return@withContext rotateIfNeeded(video, bitmap)
          }
        } catch (e: Exception) {
          Log.e("ThumbnailRepository", "External cover check failed", e)
        }
      }

      // 2. Retriever for Embedded/First Frame
      val retriever = MediaMetadataRetriever()
      try {
        if (isNetworkUrl(video.path)) {
          retriever.setDataSource(video.path, HashMap<String, String>())
        } else {
          retriever.setDataSource(video.path)
        }

        // A. Embedded Check (Matches your MKV info)
        val embeddedArt = retriever.embeddedPicture
        if (embeddedArt != null) {
          val bitmap = BitmapFactory.decodeByteArray(embeddedArt, 0, embeddedArt.size)
          if (bitmap != null) return@withContext rotateIfNeeded(video, bitmap)
        }

        // B. First Frame Check
        val firstFrame = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O_MR1) {
          retriever.getScaledFrameAtTime(0, MediaMetadataRetriever.OPTION_CLOSEST_SYNC, dimension, dimension)
        } else {
          retriever.getFrameAtTime(0, MediaMetadataRetriever.OPTION_CLOSEST_SYNC)
        }

        firstFrame?.let { rotateIfNeeded(video, it) }
      } catch (e: Exception) {
        Log.e("ThumbnailRepository", "Retriever extraction failed", e)
        null
      } finally {
        try { retriever.release() } catch (e: Exception) { /* Ignore */ }
      }
    }
  }

  private fun calculateInSampleSize(options: BitmapFactory.Options, reqWidth: Int, reqHeight: Int): Int {
    val (height: Int, width: Int) = options.outHeight to options.outWidth
    var inSampleSize = 1
    if (height > reqHeight || width > reqWidth) {
      val halfHeight: Int = height / 2
      val halfWidth: Int = width / 2
      while (halfHeight / inSampleSize >= reqHeight && halfWidth / inSampleSize >= reqWidth) {
        inSampleSize *= 2
      }
    }
    return inSampleSize
  }

  suspend fun getCachedThumbnail(video: Video, widthPx: Int, heightPx: Int): Bitmap? = withContext(Dispatchers.IO) {
    if (isNetworkUrl(video.path) && !appearancePreferences.showNetworkThumbnails.get()) return@withContext null
    val key = thumbnailKey(video, widthPx, heightPx)
    synchronized(memoryCache) { memoryCache.get(key) }?.let { return@withContext it }
    loadFromDisk(video)?.let { thumbnail ->
      synchronized(memoryCache) { memoryCache.put(key, thumbnail) }
      return@withContext thumbnail
    }
    null
  }

  fun getThumbnailFromMemory(video: Video, widthPx: Int, heightPx: Int): Bitmap? {
    if (isNetworkUrl(video.path) && !appearancePreferences.showNetworkThumbnails.get()) return null
    val key = thumbnailKey(video, widthPx, heightPx)
    return synchronized(memoryCache) { memoryCache.get(key) }
  }

  fun clearThumbnailCache() {
    folderJobs.values.forEach { it.cancel() }
    folderJobs.clear()
    folderStates.clear()
    ongoingOperations.clear()
    useMediaStoreForVideo.clear()
    synchronized(memoryCache) { memoryCache.evictAll() }
    runCatching { if (diskDir.exists()) diskDir.listFiles()?.forEach { it.delete() } }
  }

  fun startFolderThumbnailGeneration(folderId: String, videos: List<Video>, widthPx: Int, heightPx: Int) {
    val filteredVideos = if (appearancePreferences.showNetworkThumbnails.get()) videos else videos.filterNot { isNetworkUrl(it.path) }
    if (filteredVideos.isEmpty()) return
    folderJobs.entries.removeAll { !it.value.isActive }
    if (folderJobs.size >= maxconcurrentfolders && !folderJobs.containsKey(folderId)) {
      folderJobs.entries.firstOrNull()?.let { (oldestId, job) ->
        job.cancel()
        folderJobs.remove(oldestId)
        folderStates.remove(oldestId)
      }
    }
    val signature = folderSignature(filteredVideos, widthPx, heightPx)
    val state = folderStates.compute(folderId) { _, existing ->
      if (existing == null || existing.signature != signature) FolderState(signature, 0) else existing
    }!!
    folderJobs.remove(folderId)?.cancel()
    folderJobs[folderId] = repositoryScope.launch {
      var i = state.nextIndex
      while (i < filteredVideos.size) {
        getThumbnail(filteredVideos[i], widthPx, heightPx)
        i++
        state.nextIndex = i
      }
    }
  }

  fun thumbnailKey(video: Video, width: Int, height: Int): String = "${videoBaseKey(video)}|$width|$height"

  private fun videoBaseKey(video: Video): String {
    return if (isNetworkUrl(video.path)) "${video.path.ifBlank { video.uri.toString() }}|network"
    else "${video.size}|${video.dateModified}|${video.duration}"
  }

  private fun keyToFileName(key: String): String {
    val digest = MessageDigest.getInstance("MD5").digest(key.toByteArray())
    return digest.joinToString("") { "%02x".format(it) } + ".jpg"
  }

  private fun diskKey(video: Video): String {
    val base = videoBaseKey(video)
    return if (isNetworkUrl(video.path)) "$base|disk|d$diskCacheDimension|pos3" else "$base|disk|d$diskCacheDimension"
  }

  private fun loadFromDisk(video: Video): Bitmap? {
    val diskFile = File(diskDir, keyToFileName(diskKey(video)))
    if (!diskFile.exists()) return null
    return runCatching {
      BitmapFactory.decodeFile(diskFile.absolutePath, BitmapFactory.Options().apply { inPreferredConfig = Bitmap.Config.ARGB_8888 })
    }.getOrNull()
  }

  private fun writeToDisk(video: Video, bitmap: Bitmap) {
    val diskFile = File(diskDir, keyToFileName(diskKey(video)))
    runCatching { FileOutputStream(diskFile).use { out -> bitmap.compress(Bitmap.CompressFormat.JPEG, diskJpegQuality, out); out.flush() } }
  }

  private suspend fun rotateIfNeeded(video: Video, bitmap: Bitmap): Bitmap {
    val rotation = MediaInfoOps.getRotation(context, video.uri, video.displayName)
    if (rotation == 0) return bitmap
    val matrix = android.graphics.Matrix().apply { postRotate(rotation.toFloat()) }
    return Bitmap.createBitmap(bitmap, 0, 0, bitmap.width, bitmap.height, matrix, true)
  }

  private suspend fun generateWithFastThumbnails(video: Video, dimension: Int): Bitmap? {
    return runCatching {
      val bmp = FastThumbnails.generateAsync(video.path.ifBlank { video.uri.toString() }, preferredPositionSeconds(video), dimension, false) ?: return@runCatching null
      rotateIfNeeded(video, bmp)
    }.getOrNull()
  }

  private suspend fun generateWithMediaStore(video: Video, dimension: Int): Bitmap? {
    if (isNetworkUrl(video.path)) return null
    return withContext(Dispatchers.IO) {
      val mediaStoreThumbnail = runCatching {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
          val contentUri = android.content.ContentUris.withAppendedId(android.provider.MediaStore.Video.Media.EXTERNAL_CONTENT_URI, video.id)
          rotateIfNeeded(video, context.contentResolver.loadThumbnail(contentUri, android.util.Size(dimension, dimension), null))
        } else {
          @Suppress("DEPRECATION")
          android.provider.MediaStore.Video.Thumbnails.getThumbnail(context.contentResolver, video.id, android.provider.MediaStore.Video.Thumbnails.MINI_KIND, null)?.let {
            val scaled = it.scale(dimension, (dimension * (it.height.toFloat() / it.width)).toInt(), true)
            if (scaled != it) it.recycle()
            rotateIfNeeded(video, scaled)
          }
        }
      }.getOrNull()

      if (mediaStoreThumbnail != null) return@withContext mediaStoreThumbnail

      runCatching {
        val file = File(video.path)
        if (!file.exists()) return@runCatching null
        val thumbnail = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
          android.media.ThumbnailUtils.createVideoThumbnail(file, android.util.Size(dimension, dimension), null)
        } else {
          @Suppress("DEPRECATION")
          android.media.ThumbnailUtils.createVideoThumbnail(video.path, android.provider.MediaStore.Video.Thumbnails.MINI_KIND)?.let {
            it.scale(dimension, (dimension * (it.height.toFloat() / it.width)).toInt(), true).also { s -> if (s != it) it.recycle() }
          }
        }
        thumbnail?.let { rotateIfNeeded(video, it) }
      }.getOrNull()
    }
  }

  private fun preferredPositionSeconds(video: Video): Double {
    val durationSec = video.duration / 1000.0
    if (isNetworkUrl(video.path)) return if (durationSec > 0.0) 2.0.coerceIn(0.0, max(0.0, durationSec - 0.1)) else 2.0
    return if (durationSec <= 0.0 || durationSec < 20.0) 0.0 else 3.0.coerceIn(0.0, max(0.0, durationSec - 0.1))
  }

  private fun isNetworkUrl(path: String): Boolean = path.startsWith("http://", true) || path.startsWith("https://", true) || path.startsWith("rtmp://", true) || path.startsWith("rtsp://", true) || path.startsWith("ftp://", true) || path.startsWith("sftp://", true)

  private fun folderSignature(videos: List<Video>, widthPx: Int, heightPx: Int): String {
    val md = MessageDigest.getInstance("MD5").apply { update("$widthPx|$heightPx|".toByteArray()) }
    for (v in videos) { md.update("${v.path}|${v.size}|${v.dateModified};".toByteArray()) }
    return md.digest().joinToString("") { "%02x".format(it) }
  }
}
