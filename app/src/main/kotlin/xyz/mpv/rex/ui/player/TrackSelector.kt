package xyz.mpv.rex.ui.player

import android.util.Log
import xyz.mpv.rex.preferences.AudioPreferences
import xyz.mpv.rex.preferences.SubtitlesPreferences
import `is`.xyz.mpv.MPVLib
import kotlinx.coroutines.delay

/**
 * Handles automatic track selection based on user preferences.
 * Combines an intelligent multi-pass Context Engine with optimized data structures.
 * Adapted from https://github.com/Chinna95P/mpv-anime-build/blob/main/scripts/track-selector.lua
 *
 * **Performance Optimization:**
 * To minimize expensive JNI calls to MPV, all track properties are read exactly once 
 * upon file load and cached into a list of `Track` objects. The selection logic 
 * evaluates this cached list.
 *
 * **State Management (Watch-Later):**
 * If a file is resumed (`hasState = true`), any previously saved track selections—or 
 * a manually saved "subtitles off" state—are strictly respected, completely bypassing 
 * the auto-selection engine.
 *
 * **Audio Selection Strategy (Highest to Lowest Priority):**
 * 1. **Preferred Clean Audio:** Matches the user's preferred language while explicitly 
 * filtering out non-main tracks (e.g., commentary, ADH, descriptions).
 * 2. **Fallback Clean Audio:** Selects the first available track that does not contain 
 * ignored keywords.
 *
 * **Subtitle Selection Strategy (Highest to Lowest Priority):**
 * Subtitle selection is highly dependent on the auto-detected media context (Anime vs. Live-Action).
 * - **Pass 00 (External Override):** Automatically prioritizes manually loaded external subtitle files.
 * - **Pass A0 (Anime Only - Native Default):** If exactly *one* subtitle track is flagged 
 * as default and it is Japanese, it is selected. This protects against muxing errors 
 * where multiple tracks are incorrectly flagged as default by the encoder.
 * - **Pass A (Anime Only - Smart Dialogue):** Prioritizes tracks matching the preferred 
 * language that contain keywords like "dialogue", "full", or "script".
 * - **Pass B (Clean Match):** Finds the preferred language but aggressively strips out 
 * secondary tracks like "signs", "songs", "lyrics", "sdh", or "forced".
 * - **Pass C (Last Resort):** Selects the first available track matching the preferred language.
 */
 
class TrackSelector(
  private val audioPreferences: AudioPreferences,
  private val subtitlesPreferences: SubtitlesPreferences,
) {
  data class TrackChoice(
    val title: String = "",
    val lang: String = "",
    val isOff: Boolean = false,
  )

  companion object {
    private const val TAG = "TrackSelector"

    var rememberedAudioChoice: TrackChoice? = null
      private set
    var rememberedSubtitleChoice: TrackChoice? = null
      private set

    fun rememberAudioTrack(title: String?, lang: String?) {
      rememberedAudioChoice = TrackChoice(
        title = (title ?: "").lowercase().trim(),
        lang = (lang ?: "").lowercase().trim(),
        isOff = false,
      )
      Log.d(TAG, "Remembered audio choice: title='${rememberedAudioChoice?.title}', lang='${rememberedAudioChoice?.lang}'")
    }

    fun rememberSubtitleTrack(title: String?, lang: String?, isOff: Boolean) {
      rememberedSubtitleChoice = TrackChoice(
        title = (title ?: "").lowercase().trim(),
        lang = (lang ?: "").lowercase().trim(),
        isOff = isOff,
      )
      Log.d(TAG, "Remembered subtitle choice: title='${rememberedSubtitleChoice?.title}', lang='${rememberedSubtitleChoice?.lang}', isOff=$isOff")
    }

    fun clearRememberedChoices() {
      rememberedAudioChoice = null
      rememberedSubtitleChoice = null
    }
  }

  // The Data Class for massively improved performance.
  private data class Track(
    val id: Int,
    val type: String,
    val lang: String,
    val title: String,
    val isDefault: Boolean,
    val forced: Boolean,
    val hearing: Boolean,
    val external: Boolean,
    val image: Boolean
  )

  suspend fun onFileLoaded(hasState: Boolean = false) {
    var attempts = 0
    val maxAttempts = 20
    
    while (attempts < maxAttempts) {
      val count = MPVLib.getPropertyInt("track-list/count") ?: 0
      if (count > 0) break
      delay(50)
      attempts++
    }

    val trackCount = MPVLib.getPropertyInt("track-list/count") ?: 0
    if (trackCount == 0) return

    // Read all tracks once
    val tracks = readTracks(trackCount)

    if (!isVideoFile(tracks)) {
      Log.d(TAG, "Smart Tracks: Audio/Image file detected. Script disabled.")
      return
    }
  
    ensureAudioTrackSelected(tracks, hasState)
    ensureSubtitleTrackSelected(tracks, hasState)
  }

  private fun readTracks(count: Int): List<Track> {
    val list = mutableListOf<Track>()
    for (i in 0 until count) {
      val id = MPVLib.getPropertyInt("track-list/$i/id") ?: continue
      val type = MPVLib.getPropertyString("track-list/$i/type") ?: continue

      list.add(
        Track(
          id = id,
          type = type,
          lang = (MPVLib.getPropertyString("track-list/$i/lang") ?: "").lowercase(),
          title = (MPVLib.getPropertyString("track-list/$i/title") ?: "").lowercase(),
          isDefault = MPVLib.getPropertyBoolean("track-list/$i/default") ?: false,
          forced = MPVLib.getPropertyBoolean("track-list/$i/forced") ?: false,
          hearing = MPVLib.getPropertyBoolean("track-list/$i/hearing-impaired") ?: false,
          external = MPVLib.getPropertyBoolean("track-list/$i/external") ?: false,
          image = (MPVLib.getPropertyBoolean("track-list/$i/image") ?: false) ||
                  (MPVLib.getPropertyBoolean("track-list/$i/albumart") ?: false)
        )
      )
    }
    return list
  }

  private fun findMatchingTrack(tracks: List<Track>, choice: TrackChoice): Track? {
    if (choice.isOff) return null

    val targetTitle = choice.title.trim().lowercase()
    val targetLang = choice.lang.trim().lowercase()

    // 1. Match both non-empty title and non-empty lang
    if (targetTitle.isNotEmpty() && targetLang.isNotEmpty()) {
      tracks.firstOrNull {
        it.title.trim().lowercase() == targetTitle &&
          (it.lang.trim().lowercase() == targetLang ||
           it.lang.startsWith(targetLang) ||
           targetLang.startsWith(it.lang))
      }?.let { return it }
    }

    // 2. Match non-empty title
    if (targetTitle.isNotEmpty()) {
      tracks.firstOrNull {
        it.title.trim().lowercase() == targetTitle
      }?.let { return it }
    }

    // 3. Match non-empty lang when title is empty
    if (targetTitle.isEmpty() && targetLang.isNotEmpty()) {
      tracks.firstOrNull {
        (it.lang.trim().lowercase() == targetLang ||
         it.lang.startsWith(targetLang) ||
         targetLang.startsWith(it.lang))
      }?.let { return it }
    }

    return null
  }

  // ==================================================
  // AUTO-DETECTION HELPERS
  // ==================================================

  private fun isVideoFile(tracks: List<Track>): Boolean {
    return tracks.any { it.type == "video" && !it.image }
  }

  private fun isAnimeFolder(path: String?): Boolean {
    if (path == null) return false
    val p = path.lowercase()
    return p.contains("/anime/") || p.contains("\\anime\\") ||
           p.contains("donghua") || p.contains("cartoon") ||
           p.contains("animation") || p.contains("3d_anime")
  }

  private fun isLiveAction(path: String?, title: String?): Boolean {
    val searchStr = "${path ?: ""} ${title ?: ""}".lowercase()
    return searchStr.contains("live action") || searchStr.contains("live-action") ||
           searchStr.contains("liveaction") || searchStr.contains("drama") ||
           searchStr.contains("real person")
  }

  private fun detectAnimeContext(tracks: List<Track>): Boolean {
    val path = MPVLib.getPropertyString("path") ?: ""
    val title = MPVLib.getPropertyString("media-title") ?: ""
    val filename = MPVLib.getPropertyString("filename") ?: ""

    val signalFolder = isAnimeFolder(path)
    val signalLiveAction = isLiveAction(path, title)
    
    val syntaxRegex = Regex("\\[.*\\]")
    val signalSyntax = syntaxRegex.containsMatchIn(title)

    val crcRegex = Regex("\\[[0-9a-fA-F]{8}\\]")
    val signalCrc = crcRegex.containsMatchIn(filename) || crcRegex.containsMatchIn(title)

    val signalAudio = tracks.any { it.type == "audio" && (it.lang == "jpn" || it.lang == "ja") }

    if (signalLiveAction) return false
    if (signalCrc) return true
    if (signalFolder || signalAudio || signalSyntax) return true
    
    return false
  }

  // ==================================================
  // 1. AUDIO SELECTION LOGIC (Multi-Pass Preserved)
  // ==================================================

  private suspend fun ensureAudioTrackSelected(tracks: List<Track>, hasState: Boolean) {
    try {
      val audioTracks = tracks.filter { it.type == "audio" }
      if (audioTracks.isEmpty()) return

      val currentAid = MPVLib.getPropertyInt("aid")
      if (hasState && currentAid != null && currentAid > 0) {
        audioTracks.firstOrNull { it.id == currentAid }?.let {
          rememberAudioTrack(it.title, it.lang)
        }
        return
      }

      // Pass 0: Try remembered audio choice from previous video/session
      if (!hasState && rememberedAudioChoice != null) {
        val choice = rememberedAudioChoice!!
        val matchedTrack = findMatchingTrack(audioTracks, choice)
        if (matchedTrack != null) {
          if (currentAid == matchedTrack.id) {
            Log.d(TAG, "Smart Audio: Matched remembered choice '${choice.title}'/'${choice.lang}' (id=${matchedTrack.id}) [Already Active. Skipping Change.]")
          } else {
            Log.d(TAG, "Smart Audio: Matched remembered choice '${choice.title}'/'${choice.lang}' (id=${matchedTrack.id}) [Applied]")
            MPVLib.setPropertyInt("aid", matchedTrack.id)
          }
          rememberAudioTrack(matchedTrack.title, matchedTrack.lang)
          return
        } else {
          Log.d(TAG, "Smart Audio: Remembered choice '${choice.title}'/'${choice.lang}' not found in new video. Falling back to normal selection.")
        }
      }

      val preferredLangs = audioPreferences.preferredLanguages.get()
        .split(",")
        .map { it.trim().lowercase() }
        .filter { it.isNotEmpty() }

      val ignoreKeywords = listOf("commentary", "description", "adh", "comment", "extra")

      // Priority 1: Preferred clean audio
      if (preferredLangs.isNotEmpty()) {
        for (prefLang in preferredLangs) {
          for (track in audioTracks) {
            if (track.lang == prefLang || track.lang.startsWith(prefLang)) {
              if (ignoreKeywords.none { track.title.contains(it) }) {
                if (currentAid == track.id) {
                  Log.d(TAG, "Smart Audio: Selected ${track.lang} (id=${track.id}) [Already Active. Skipping Change.]")
                } else {
                  Log.d(TAG, "Smart Audio: Selected ${track.lang} (id=${track.id}) [Applied]")
                  MPVLib.setPropertyInt("aid", track.id)
                }
                rememberAudioTrack(track.title, track.lang)
                return
              }
            }
          }
        }
      }

      // Priority 2: Fallback MPV default
      if (currentAid != null && currentAid > 0) {
        audioTracks.firstOrNull { it.id == currentAid }?.let {
          rememberAudioTrack(it.title, it.lang)
        }
        return
      }

      // Priority 3: First available clean audio track
      for (track in audioTracks) {
        if (ignoreKeywords.none { track.title.contains(it) }) {
          if (currentAid == track.id) {
            Log.d(TAG, "Smart Audio: Fallback (id=${track.id}) [Already Active. Skipping Change.]")
          } else {
            Log.d(TAG, "Smart Audio: Fallback (id=${track.id}) [Applied]")
            MPVLib.setPropertyInt("aid", track.id)
          }
          rememberAudioTrack(track.title, track.lang)
          return
        }
      }
    } catch (e: Exception) {
      Log.e(TAG, "Audio selection failed", e)
    }
  }

  // ==================================================
  // 2. SUBTITLE SELECTION LOGIC (Multi-Pass Preserved)
  // ==================================================

  private suspend fun ensureSubtitleTrackSelected(tracks: List<Track>, hasState: Boolean) {
    try {
      val subTracks = tracks.filter { it.type == "sub" }

      // If user has disabled subtitles by default, turn off subtitles unless we are restoring state
      if (subtitlesPreferences.disableSubtitlesByDefault.get()) {
        if (!hasState) {
          Log.d(TAG, "Smart Sub: Subtitles disabled by default (user preference). Disabling subtitle track.")
          MPVLib.setPropertyString("sid", "no")
          rememberSubtitleTrack(null, null, isOff = true)
        } else {
          Log.d(TAG, "Smart Sub: Subtitles disabled by default, but respecting saved playback state.")
        }
        return
      }

      val currentSid = MPVLib.getPropertyInt("sid") ?: 0

      // Respect manual "Subtitles Off" state in saved DB state
      if (hasState && currentSid == 0) {
        Log.d(TAG, "Smart Sub: User disabled subtitles manually. Respecting choice.")
        rememberSubtitleTrack(null, null, isOff = true)
        return
      }

      if (hasState && currentSid > 0) {
        subTracks.firstOrNull { it.id == currentSid }?.let {
          rememberSubtitleTrack(it.title, it.lang, isOff = false)
        }
        return
      }

      // Pass 0: Try remembered subtitle choice from previous video/session
      if (!hasState && rememberedSubtitleChoice != null) {
        val choice = rememberedSubtitleChoice!!
        if (choice.isOff) {
          Log.d(TAG, "Smart Sub: Remembering choice [Subtitles Off]. Disabling subtitle track.")
          MPVLib.setPropertyString("sid", "no")
          return
        }

        val matchedTrack = findMatchingTrack(subTracks, choice)
        if (matchedTrack != null) {
          if (currentSid == matchedTrack.id) {
            Log.d(TAG, "Smart Sub: Matched remembered choice '${choice.title}'/'${choice.lang}' (id=${matchedTrack.id}) [Already Active. Skipping Change.]")
          } else {
            Log.d(TAG, "Smart Sub: Matched remembered choice '${choice.title}'/'${choice.lang}' (id=${matchedTrack.id}) [Applied]")
            MPVLib.setPropertyInt("sid", matchedTrack.id)
          }
          rememberSubtitleTrack(matchedTrack.title, matchedTrack.lang, isOff = false)
          return
        } else {
          Log.d(TAG, "Smart Sub: Remembered choice '${choice.title}'/'${choice.lang}' not found in new video. Falling back to normal selection.")
        }
      }

      val isAnimeContext = detectAnimeContext(tracks)
      Log.d(TAG, "Smart Tracks: Context defined by Internal Auto-Detection -> $isAnimeContext")

      var preferredLangs = subtitlesPreferences.preferredLanguages.get()
        .split(",")
        .map { it.trim().lowercase() }
        .filter { it.isNotEmpty() }

      if (preferredLangs.isEmpty()) {
        preferredLangs = (MPVLib.getPropertyString("slang") ?: "")
          .split(",")
          .map { it.trim().lowercase() }
          .filter { it.isNotEmpty() }
      }
      if (preferredLangs.isEmpty()) preferredLangs = listOf("eng", "en")

      val ignoreSubs = listOf("signs", "songs", "lyrics", "forced", "sdh", "colored", "karaoke")

      // PASS 00: EXTERNAL TRACK OVERRIDE (Protects manually loaded subtitle files)
      for (track in subTracks) {
        if (track.external) {
          if (currentSid == track.id) {
            Log.d(TAG, "Smart Sub: External Subtitle Detected (id=${track.id}) [Already Active. Skipping Change.]")
          } else {
            Log.d(TAG, "Smart Sub: External Subtitle Detected (id=${track.id}) [Applied]")
            MPVLib.setPropertyInt("sid", track.id)
          }
          rememberSubtitleTrack(track.title, track.lang, isOff = false)
          return
        }
      }

      // PASS A0: KEEP FILE'S NATIVE DEFAULT JAPANESE SUBS FOR ANIME
      if (isAnimeContext) {
        val defaultCount = subTracks.count { it.isDefault }

        if (defaultCount == 1) {
          for (track in subTracks) {
            if (track.isDefault) {
              if (track.lang == "jpn" || track.lang == "ja" || track.lang == "jp") {
                if (currentSid == track.id) {
                  Log.d(TAG, "Smart Sub: Native File Default Japanese Sub (id=${track.id}) [Already Active. Skipping Change.]")
                } else {
                  Log.d(TAG, "Smart Sub: Native File Default Japanese Sub (id=${track.id}) [Applied]")
                  MPVLib.setPropertyInt("sid", track.id)
                }
                rememberSubtitleTrack(track.title, track.lang, isOff = false)
                return
              }
            }
          }
        } else if (defaultCount > 1) {
          Log.d(TAG, "Smart Sub: Multiple default tracks detected (Muxing error). Ignoring.")
        }
      }

      // PASS A: SMART ANIME DIALOGUE
      if (isAnimeContext) {
        for (prefLang in preferredLangs) {
          for (track in subTracks) {
            if (track.lang == prefLang || track.lang.startsWith(prefLang)) {
              if (track.title.contains("dialogue") || track.title.contains("full") || track.title.contains("script")) {
                if (currentSid == track.id) {
                  Log.d(TAG, "Smart Sub: Anime Dialogue matched (id=${track.id}) [Already Active. Skipping Change.]")
                } else {
                  Log.d(TAG, "Smart Sub: Anime Dialogue matched (id=${track.id}) [Applied]")
                  MPVLib.setPropertyInt("sid", track.id)
                }
                rememberSubtitleTrack(track.title, track.lang, isOff = false)
                return
              }
            }
          }
        }
      }

      // PASS B: CLEAN LANGUAGE MATCH
      for (prefLang in preferredLangs) {
        for (track in subTracks) {
          if (track.lang == prefLang || track.lang.startsWith(prefLang)) {
            if (ignoreSubs.none { track.title.contains(it) } && !track.forced && !track.hearing) {
              if (currentSid == track.id) {
                Log.d(TAG, "Smart Sub: Clean Match (id=${track.id}) [Already Active. Skipping Change.]")
              } else {
                Log.d(TAG, "Smart Sub: Clean Match (id=${track.id}) [Applied]")
                MPVLib.setPropertyInt("sid", track.id)
              }
              rememberSubtitleTrack(track.title, track.lang, isOff = false)
              return
            }
          }
        }
      }
      
      // PASS C: LAST RESORT MATCHING
      for (prefLang in preferredLangs) {
        for (track in subTracks) {
          if (track.lang == prefLang || track.lang.startsWith(prefLang)) {
            if (currentSid == track.id) {
              Log.d(TAG, "Smart Sub: Fallback Match (id=${track.id}) [Already Active. Skipping Change.]")
            } else {
              Log.d(TAG, "Smart Sub: Fallback Match (id=${track.id}) [Applied]")
              MPVLib.setPropertyInt("sid", track.id)
            }
            rememberSubtitleTrack(track.title, track.lang, isOff = false)
            return
          }
        }
      }

    } catch (e: Exception) {
      Log.e(TAG, "Subtitle selection failed", e)
    }
  }
}

