package xyz.mpv.rex.utils.media

import android.content.Context
import android.net.Uri
import androidx.core.net.toUri
import xyz.mpv.rex.preferences.BrowserPreferences
import xyz.mpv.rex.preferences.FolderSortType
import xyz.mpv.rex.repository.MediaFileRepository
import xyz.mpv.rex.utils.sort.SortUtils
import xyz.mpv.rex.utils.storage.FileFilterUtils
import xyz.mpv.rex.utils.storage.FileTypeUtils
import xyz.mpv.rex.utils.storage.MediaScanPolicy
import xyz.mpv.rex.utils.storage.VideoScanUtils
import java.io.File
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject

/**
 * Shared utility operations for auto-generating playlists from local folders or media library.
 * Used by both PlayerActivity and MediaUtils (for direct mini player playback).
 */
object FolderPlaylistOps : KoinComponent {
  private val browserPreferences: BrowserPreferences by inject()

  /**
   * Generates a playlist of URIs in the same folder as [currentPath].
   *
   * @return Pair of (uris, initialIndex) or null if single/invalid file.
   */
  suspend fun generateFolderPlaylist(
    context: Context,
    currentPath: String,
    launchSource: String? = null,
  ): Pair<List<Uri>, Int>? {
    runCatching {
      val currentFile = resolveLocalFile(context, currentPath) ?: return null
      if (!currentFile.exists()) return null

      val parentFolder = currentFile.parentFile ?: return null
      val scanPolicy = MediaScanPolicy(
        includeNoMediaContent = browserPreferences.includeNoMediaContent.get(),
      )
      if (!scanPolicy.includeNoMediaContent &&
        FileFilterUtils.isWithinNoMediaBoundary(parentFolder)
      ) {
        return null
      }

      val showAudio = browserPreferences.showAudioFiles.get()
      val files = parentFolder.listFiles { file ->
        file.isFile &&
          !FileFilterUtils.shouldSkipFile(file) &&
          (FileTypeUtils.isVideoFile(file) || (showAudio && FileTypeUtils.isAudioFile(file)))
      } ?: return null

      val lSource = launchSource ?: ""
      val siblingFiles = if (lSource == "video_list" || lSource == "recently_played_button" || lSource == "first_video_button") {
        val videoSortType = browserPreferences.videoSortType.get()
        val videoSortOrder = browserPreferences.videoSortOrder.get()
        val bucketId = parentFolder.absolutePath.replace("\\", "/")
        val videosInFolder = VideoScanUtils.getVideosInFolder(context, bucketId, scanPolicy)
        val sortedVideos = SortUtils.sortVideos(videosInFolder, videoSortType, videoSortOrder)
        sortedVideos.mapNotNull { video -> files.find { it.absolutePath == video.path } }
      } else if (lSource == "tree_mode") {
        val folderSortType = browserPreferences.folderSortType.get()
        val folderSortOrder = browserPreferences.folderSortOrder.get()
        val videosInFolder = VideoScanUtils.getVideosInFolder(context, parentFolder.absolutePath, scanPolicy)
        val sortedVideos = when (folderSortType) {
          FolderSortType.Title -> videosInFolder.sortedWith { t1, t2 -> SortUtils.NaturalOrderComparator.DEFAULT.compare(t1.displayName, t2.displayName) }
          FolderSortType.Duration -> videosInFolder.sortedBy { it.duration }
          FolderSortType.Date -> videosInFolder.sortedBy { File(it.path).lastModified() }
          FolderSortType.Size -> videosInFolder.sortedBy { it.size }
          FolderSortType.VideoCount -> videosInFolder.sortedBy { it.duration }
        }
        val orderedVideos = if (folderSortOrder.isAscending) sortedVideos else sortedVideos.reversed()
        orderedVideos.mapNotNull { video -> files.find { it.absolutePath == video.path } }
      } else {
        files.sortedWith { f1, f2 -> SortUtils.NaturalOrderComparator.DEFAULT.compare(f1.name, f2.name) }
      }

      if (siblingFiles.size <= 1) return null

      val newPlaylist = siblingFiles.map { it.toUri() }
      val newIndex = siblingFiles.indexOfFirst { it.absolutePath == currentFile.absolutePath }

      if (newIndex != -1) {
        return Pair(newPlaylist, newIndex)
      }
    }
    return null
  }

  private fun resolveLocalFile(context: Context, value: String): File? {
    val uri = runCatching { Uri.parse(value) }.getOrNull()
    if (uri?.scheme != "content") return File(value)

    return runCatching {
      context.contentResolver.query(
        uri,
        arrayOf(android.provider.MediaStore.MediaColumns.DATA),
        null,
        null,
        null,
      )?.use { cursor ->
        val dataColumn = cursor.getColumnIndex(android.provider.MediaStore.MediaColumns.DATA)
        if (dataColumn >= 0 && cursor.moveToFirst()) {
          cursor.getString(dataColumn)?.let(::File)
        } else {
          null
        }
      }
    }.getOrNull()
  }

  /**
   * Generates a playlist of URIs from the media library.
   *
   * @return Pair of (uris, initialIndex) or null if single/invalid file.
   */
  suspend fun generateMediaLibraryPlaylist(
    context: Context,
    currentPath: String,
  ): Pair<List<Uri>, Int>? {
    runCatching {
      val allVideos = MediaFileRepository.getAllVideos(context)
      val videoSortType = browserPreferences.videoSortType.get()
      val videoSortOrder = browserPreferences.videoSortOrder.get()

      var filteredVideos = allVideos
      if (!browserPreferences.showAudioFiles.get()) {
        filteredVideos = allVideos.filterNot { it.isAudio }
      }

      val sortedVideos = SortUtils.sortVideos(filteredVideos, videoSortType, videoSortOrder)
      if (sortedVideos.size <= 1) return null

      val newPlaylist = sortedVideos.map { it.uri }
      val newIndex = sortedVideos.indexOfFirst { it.path == currentPath || it.uri.toString() == currentPath }

      if (newIndex != -1) {
        return Pair(newPlaylist, newIndex)
      }
    }
    return null
  }
}
