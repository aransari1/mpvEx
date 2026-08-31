package xyz.mpv.rex.feature.webshare

import android.content.Context
import android.net.Uri
import android.os.Environment
import android.webkit.MimeTypeMap
import fi.iki.elonen.NanoHTTPD
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.net.URLDecoder
import java.net.URLEncoder
import java.util.HashMap
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.concurrent.thread

/**
 * Embedded NanoHTTPD server for sharing local files with nearby devices over Wi-Fi / Hotspot.
 * Supports partial content (HTTP 206) for video/audio seeking, resumable downloads, and uploads.
 */
class WebShareServer(
  port: Int,
  private val files: List<ShareableFile>,
  private val token: String? = null,
  private val context: Context? = null,
) : NanoHTTPD(port) {

  data class ShareableFile(
    val id: String,
    val displayName: String,
    val size: Long,
    val file: File? = null,
    val uri: Uri? = null,
    val durationFormatted: String? = null,
  )

  private val fileMap = files.associateBy { it.id }

  override fun serve(session: IHTTPSession): Response {
    val uri = session.uri
    val params = session.parameters
    val clientToken = params["t"]?.firstOrNull() ?: params["token"]?.firstOrNull()

    // Validate security token if token is enabled
    if (!token.isNullOrEmpty() && clientToken != token && uri != "/favicon.ico") {
      return newFixedLengthResponse(
        Response.Status.FORBIDDEN,
        MIME_PLAINTEXT,
        "Access Denied: Invalid or missing security token."
      )
    }

    return try {
      when {
        uri == "/" -> serveIndexPage()
        uri == "/upload" && session.method == Method.POST -> handleUpload(session)
        uri.startsWith("/download/") -> {
          val fileId = uri.removePrefix("/download/")
          serveFile(session, fileId, isAttachment = true)
        }
        uri.startsWith("/stream/") -> {
          val fileId = uri.removePrefix("/stream/")
          serveFile(session, fileId, isAttachment = false)
        }
        uri == "/download-all" -> serveZipArchive()
        else -> newFixedLengthResponse(Response.Status.NOT_FOUND, MIME_PLAINTEXT, "404 Not Found")
      }
    } catch (e: Exception) {
      newFixedLengthResponse(
        Response.Status.INTERNAL_ERROR,
        MIME_PLAINTEXT,
        "500 Internal Server Error: ${e.message}"
      )
    }
  }

  private fun serveIndexPage(): Response {
    val items = files.map { item ->
      WebShareHtmlTemplate.SharedFileItem(
        id = item.id,
        name = item.displayName,
        size = item.size,
        formattedSize = formatSize(item.size),
        durationFormatted = item.durationFormatted,
        mimeType = getMimeType(item.displayName),
      )
    }
    val html = WebShareHtmlTemplate.renderHtml(items, token)
    return newFixedLengthResponse(Response.Status.OK, "text/html; charset=UTF-8", html)
  }

  private fun serveFile(session: IHTTPSession, fileId: String, isAttachment: Boolean): Response {
    val shareable = fileMap[fileId] ?: return newFixedLengthResponse(Response.Status.NOT_FOUND, MIME_PLAINTEXT, "File not found")
    val fileLength = shareable.size
    val mimeType = if (isAttachment) "application/octet-stream" else getMimeType(shareable.displayName)
    val headers = session.headers
    val rangeHeader = headers["range"]

    var startFrom: Long = 0
    var endAt: Long = (fileLength - 1).coerceAtLeast(0)

    if (rangeHeader != null && rangeHeader.startsWith("bytes=")) {
      val range = rangeHeader.removePrefix("bytes=").trim()
      val dashIdx = range.indexOf('-')
      if (dashIdx != -1) {
        val startStr = range.substring(0, dashIdx)
        val endStr = range.substring(dashIdx + 1)
        if (startStr.isNotEmpty()) {
          startFrom = startStr.toLongOrNull() ?: 0L
        }
        if (endStr.isNotEmpty()) {
          endAt = endStr.toLongOrNull() ?: (fileLength - 1)
        }
      }
    }

    if (fileLength > 0 && startFrom >= fileLength) {
      val res = newFixedLengthResponse(Response.Status.RANGE_NOT_SATISFIABLE, MIME_PLAINTEXT, "")
      res.addHeader("Content-Range", "bytes */$fileLength")
      return res
    }

    endAt = endAt.coerceAtMost((fileLength - 1).coerceAtLeast(0))
    val contentLength = (endAt - startFrom + 1).coerceAtLeast(0)

    val inputStream = openInputStream(shareable) ?: return newFixedLengthResponse(
      Response.Status.NOT_FOUND,
      MIME_PLAINTEXT,
      "File unreadable on device"
    )

    if (startFrom > 0) {
      inputStream.skip(startFrom)
    }

    val status = if (rangeHeader != null) Response.Status.PARTIAL_CONTENT else Response.Status.OK
    val response = newFixedLengthResponse(status, mimeType, inputStream, contentLength)

    response.addHeader("Accept-Ranges", "bytes")
    if (rangeHeader != null) {
      response.addHeader("Content-Range", "bytes $startFrom-$endAt/$fileLength")
    }

    if (isAttachment) {
      val encodedName = URLEncoder.encode(shareable.displayName, "UTF-8").replace("+", "%20")
      response.addHeader(
        "Content-Disposition",
        "attachment; filename=\"${shareable.displayName.replace("\"", "")}\"; filename*=UTF-8''$encodedName"
      )
    }

    return response
  }

  private fun serveZipArchive(): Response {
    val pipedOut = PipedOutputStream()
    val pipedIn = PipedInputStream(pipedOut, 64 * 1024)

    thread(name = "REXPlayer-WebShare-ZipStream") {
      try {
        ZipOutputStream(pipedOut.buffered()).use { zipOut ->
          val buffer = ByteArray(64 * 1024)
          for (shareable in files) {
            val inStream = openInputStream(shareable) ?: continue
            inStream.use { fileIn ->
              val entry = ZipEntry(shareable.displayName)
              entry.size = shareable.size
              zipOut.putNextEntry(entry)

              var read: Int
              while (fileIn.read(buffer).also { read = it } != -1) {
                zipOut.write(buffer, 0, read)
              }
              zipOut.closeEntry()
            }
          }
          zipOut.flush()
        }
      } catch (e: Exception) {
        // Stream closed by receiver
      }
    }

    val response = newChunkedResponse(Response.Status.OK, "application/zip", pipedIn)
    response.addHeader("Content-Disposition", "attachment; filename=\"REX_Player_shared_files.zip\"")
    return response
  }

  private fun openInputStream(shareable: ShareableFile): InputStream? {
    return try {
      if (shareable.file != null && shareable.file.exists()) {
        FileInputStream(shareable.file)
      } else if (shareable.uri != null && context != null) {
        context.contentResolver.openInputStream(shareable.uri)
      } else {
        null
      }
    } catch (e: Exception) {
      null
    }
  }

  private fun getMimeType(fileName: String): String {
    val ext = fileName.substringAfterLast('.', "").lowercase()
    return MimeTypeMap.getSingleton().getMimeTypeFromExtension(ext) ?: when (ext) {
      "mp4", "m4v" -> "video/mp4"
      "mkv" -> "video/x-matroska"
      "webm" -> "video/webm"
      "avi" -> "video/x-msvideo"
      "mov" -> "video/quicktime"
      "flv" -> "video/x-flv"
      "ts" -> "video/mp2t"
      "mp3" -> "audio/mpeg"
      "aac" -> "audio/aac"
      "flac" -> "audio/flac"
      "ogg", "oga" -> "audio/ogg"
      "m4a" -> "audio/mp4"
      "opus" -> "audio/opus"
      "wav" -> "audio/wav"
      else -> "application/octet-stream"
    }
  }

  private fun handleUpload(session: IHTTPSession): Response {
    return try {
      val downloadsDir = Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS)
      val rexPlayerDir = File(downloadsDir, "REX Player")
      if (!rexPlayerDir.exists()) {
        rexPlayerDir.mkdirs()
      }

      val contentType = session.headers["content-type"] ?: ""
      val savedFiles = mutableListOf<File>()

      if (contentType.contains("multipart/form-data")) {
        val filesMap = HashMap<String, String>()
        session.parseBody(filesMap)
        for ((key, tempPath) in filesMap) {
          if (key == "postData" || key == "content") continue
          val tempFile = File(tempPath)
          if (tempFile.exists()) {
            val originalName = session.parameters[key]?.firstOrNull()
              ?: session.parameters["name"]?.firstOrNull()
              ?: session.parameters["filename"]?.firstOrNull()
              ?: tempFile.name
            val destFile = getUniqueDestinationFile(rexPlayerDir, originalName)
            tempFile.copyTo(destFile, overwrite = true)
            tempFile.delete()
            onFileSuccessfullySaved(destFile)
            savedFiles.add(destFile)
          }
        }
      } else {
        val rawName = session.parameters["name"]?.firstOrNull()
          ?: session.parameters["filename"]?.firstOrNull()
          ?: session.headers["x-filename"]
          ?: session.headers["x-file-name"]
          ?: "received_${System.currentTimeMillis()}"

        val decodedName = try {
          URLDecoder.decode(rawName, "UTF-8")
        } catch (e: Exception) {
          rawName
        }

        val contentLength = session.headers["content-length"]?.toLongOrNull() ?: -1L
        val destFile = getUniqueDestinationFile(rexPlayerDir, decodedName)

        FileOutputStream(destFile).use { fos ->
          val buffer = ByteArray(64 * 1024)
          var remaining = if (contentLength >= 0) contentLength else Long.MAX_VALUE
          val inputStream = session.inputStream
          while (remaining > 0) {
            val readLen = Math.min(buffer.size.toLong(), remaining).toInt()
            val read = inputStream.read(buffer, 0, readLen)
            if (read == -1) break
            fos.write(buffer, 0, read)
            if (contentLength >= 0) {
              remaining -= read
            }
          }
          fos.flush()
        }
        onFileSuccessfullySaved(destFile)
        savedFiles.add(destFile)
      }

      val firstSaved = savedFiles.firstOrNull()
      val json = "{\"success\":true,\"count\":${savedFiles.size},\"filename\":\"${escapeJson(firstSaved?.name ?: "")}\",\"size\":${firstSaved?.length() ?: 0}}"
      val response = newFixedLengthResponse(Response.Status.OK, "application/json; charset=UTF-8", json)
      response.addHeader("Access-Control-Allow-Origin", "*")
      response
    } catch (e: Exception) {
      android.util.Log.e("WebShareServer", "Failed to handle file upload", e)
      newFixedLengthResponse(Response.Status.INTERNAL_ERROR, "application/json; charset=UTF-8", "{\"success\":false,\"error\":\"${escapeJson(e.message ?: "Upload failed")}\"}")
    }
  }

  private fun getUniqueDestinationFile(dir: File, rawFilename: String): File {
    var cleanName = File(rawFilename).name.trim()
      .replace(Regex("[/\\\\?%*:|\"<>]"), "_")
      .ifEmpty { "received_${System.currentTimeMillis()}" }

    val dotIndex = cleanName.lastIndexOf('.')
    val baseName = if (dotIndex > 0) cleanName.substring(0, dotIndex) else cleanName
    val extension = if (dotIndex > 0) cleanName.substring(dotIndex) else ""

    var targetFile = File(dir, cleanName)
    var counter = 1
    while (targetFile.exists()) {
      targetFile = File(dir, "$baseName ($counter)$extension")
      counter++
    }
    return targetFile
  }

  private fun onFileSuccessfullySaved(file: File) {
    context?.let { ctx ->
      try {
        android.media.MediaScannerConnection.scanFile(
          ctx.applicationContext,
          arrayOf(file.absolutePath),
          null
        ) { path, uri ->
          android.util.Log.d("WebShareServer", "MediaScanner indexed: $path -> $uri")
        }
        WebShareManager.onFileReceived(ctx.applicationContext, file)
      } catch (e: Exception) {
        android.util.Log.e("WebShareServer", "Failed to invoke MediaScanner", e)
      }
    }
  }

  private fun escapeJson(text: String): String {
    return text.replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\b", "\\b")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
  }

  private fun formatSize(size: Long): String {
    if (size <= 0) return "0 B"
    val units = arrayOf("B", "KB", "MB", "GB", "TB")
    val digitGroups = (Math.log10(size.toDouble()) / Math.log10(1024.0)).toInt().coerceIn(0, units.size - 1)
    return java.text.DecimalFormat("#,##0.#").format(size / Math.pow(1024.0, digitGroups.toDouble())) + " " + units[digitGroups]
  }
}
