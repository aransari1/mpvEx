package xyz.mpv.rex.utils.media

import android.content.Context
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.media.MediaMetadataRetriever
import android.net.Uri
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import xyz.mpv.rex.domain.media.model.Video
import xyz.mpv.rex.domain.thumbnail.ThumbnailRepository
import xyz.mpv.rex.domain.thumbnail.isMostlySolidThumbnail
import xyz.mpv.rex.utils.storage.FileTypeUtils
import java.io.File

/**
 * Shared utility operations for loading media thumbnails and cover art.
 */
object MediaThumbnailUtils : KoinComponent {
  private val thumbnailRepository: ThumbnailRepository by inject()

  /**
   * Extracts album cover art or video frame thumbnail for a given [uri].
   */
  suspend fun extractThumbnailOrCoverArt(
    context: Context,
    uri: Uri,
  ): Bitmap? = withContext(Dispatchers.IO) {
    runCatching {
      val video = createVideoForUri(context, uri)

      // 1. Primary: Use ThumbnailRepository
      val repoThumbnail = thumbnailRepository.getThumbnail(video, 256, 256)
      if (repoThumbnail != null && !repoThumbnail.isRecycled && !isMostlySolidThumbnail(repoThumbnail)) {
        return@withContext repoThumbnail
      }

      // 2. Fallback for audio or embedded cover art via MediaMetadataRetriever
      val retriever = MediaMetadataRetriever()
      try {
        if (uri.scheme == "file") {
          val path = uri.path
          if (path != null && File(path).exists()) {
            retriever.setDataSource(path)
          } else {
            retriever.setDataSource(context, uri)
          }
        } else {
          retriever.setDataSource(context, uri)
        }
        val picture = retriever.embeddedPicture
        if (picture != null) {
          val bitmap = BitmapFactory.decodeByteArray(picture, 0, picture.size)
          if (bitmap != null && !bitmap.isRecycled) {
            return@withContext bitmap
          }
        }
      } finally {
        runCatching { retriever.release() }
      }
    }
    null
  }

  suspend fun createVideoForUri(context: Context, uri: Uri): Video = withContext(Dispatchers.IO) {
    val path = when (uri.scheme) {
      "file" -> uri.path ?: uri.toString()
      "content" -> {
        val resolvedPath = runCatching {
          context.contentResolver.query(uri, arrayOf(android.provider.MediaStore.MediaColumns.DATA), null, null, null)?.use { cursor ->
            if (cursor.moveToFirst()) {
              val idx = cursor.getColumnIndex(android.provider.MediaStore.MediaColumns.DATA)
              if (idx != -1) cursor.getString(idx) else null
            } else null
          }
        }.getOrNull()
        resolvedPath ?: uri.toString()
      }
      else -> uri.toString()
    }

    val file = if (path.startsWith("/")) File(path) else null
    val size = file?.length() ?: 0L
    val dateModified = file?.lastModified() ?: 0L
    val isAudio = FileTypeUtils.isAudioFile(file ?: File(path))
    val name = if (uri.scheme == "file") File(path).name else (uri.lastPathSegment ?: "Media")
    val parentFolder = file?.parentFile

    Video(
      id = path.hashCode().toLong(),
      title = name,
      displayName = name,
      path = path,
      uri = uri,
      duration = 0L,
      durationFormatted = "",
      size = size,
      sizeFormatted = "",
      dateModified = dateModified,
      dateAdded = 0L,
      mimeType = if (isAudio) "audio/*" else "video/*",
      bucketId = parentFolder?.absolutePath?.replace("\\", "/") ?: "",
      bucketDisplayName = parentFolder?.name ?: "",
      width = 0,
      height = 0,
      fps = 0f,
      resolution = "",
      isAudio = isAudio,
    )
  }
}
