package xyz.mpv.rex.utils.media

import android.content.Context
import android.media.MediaCodec
import android.media.MediaExtractor
import android.media.MediaFormat
import android.media.MediaMuxer
import android.media.MediaScannerConnection
import android.net.Uri
import android.os.Environment
import android.util.Log
import `is`.xyz.mpv.MPVLib
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.nio.ByteBuffer

object VideoClipper {
    private const val TAG = "VideoClipper"
    private const val DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024 // 2 MB

    suspend fun cutClip(
        context: Context,
        inputPath: String,
        outputFile: File,
        startMs: Long,
        endMs: Long
    ): Result<File> = withContext(Dispatchers.IO) {
        val startUs = startMs * 1000L
        val endUs = endMs * 1000L

        outputFile.parentFile?.mkdirs()

        val isMkv = inputPath.endsWith(".mkv", ignoreCase = true) || outputFile.name.endsWith(".mkv", ignoreCase = true)

        // If file is MKV, try mpv dump-cache first since libmpv natively supports Matroska muxer
        if (isMkv) {
            val dumpResult = tryDumpCache(startMs, endMs, outputFile)
            if (dumpResult.isSuccess && outputFile.exists() && outputFile.length() > 100 * 1024) {
                Log.d(TAG, "Clip dumped via mpv dump-cache: ${outputFile.absolutePath}")
                scanFile(context, outputFile)
                return@withContext Result.success(outputFile)
            }
            Log.w(TAG, "mpv dump-cache skipped/failed for MKV, falling back to MediaMuxer MP4...")
        }

        // Output file for MediaMuxer MUST have .mp4 or .webm extension because MediaMuxer creates MP4/WebM containers
        val finalOutputFile = if (isMkv && outputFile.name.endsWith(".mkv", ignoreCase = true)) {
            File(outputFile.parentFile, outputFile.name.removeSuffix(".mkv") + ".mp4")
        } else {
            outputFile
        }

        // Try Android native MediaExtractor + MediaMuxer stream copy with unified keyframe timestamp offset
        val nativeResult = runCatching {
            cutWithMediaMuxer(context, inputPath, finalOutputFile, startUs, endUs)
        }

        if (nativeResult.isSuccess && finalOutputFile.exists() && finalOutputFile.length() > 0) {
            Log.d(TAG, "Clip cut successfully via MediaMuxer: ${finalOutputFile.absolutePath}")
            scanFile(context, finalOutputFile)
            return@withContext Result.success(finalOutputFile)
        }

        Log.w(TAG, "MediaMuxer stream copy failed, trying mpv dump-cache fallback...", nativeResult.exceptionOrNull())

        // Secondary fallback to mpv dump-cache
        val dumpResult = tryDumpCache(startMs, endMs, finalOutputFile)
        if (dumpResult.isSuccess && finalOutputFile.exists() && finalOutputFile.length() > 0) {
            Log.d(TAG, "Clip dumped via mpv dump-cache fallback: ${finalOutputFile.absolutePath}")
            scanFile(context, finalOutputFile)
            return@withContext Result.success(finalOutputFile)
        }

        val error = nativeResult.exceptionOrNull()
            ?: dumpResult.exceptionOrNull()
            ?: Exception("Failed to generate clip file")

        if (finalOutputFile.exists() && finalOutputFile.length() == 0L) {
            finalOutputFile.delete()
        }

        Result.failure(error)
    }

    private fun tryDumpCache(startMs: Long, endMs: Long, outputFile: File): Result<Unit> {
        val startSec = startMs / 1000.0
        val endSec = endMs / 1000.0
        return runCatching {
            MPVLib.command(
                "dump-cache",
                String.format(java.util.Locale.US, "%.3f", startSec),
                String.format(java.util.Locale.US, "%.3f", endSec),
                outputFile.absolutePath
            )
        }
    }

    private fun cutWithMediaMuxer(
        context: Context,
        inputPath: String,
        outputFile: File,
        startUs: Long,
        endUs: Long
    ) {
        val extractor = MediaExtractor()
        if (inputPath.startsWith("content://")) {
            extractor.setDataSource(context, Uri.parse(inputPath), null)
        } else {
            extractor.setDataSource(inputPath)
        }

        val trackCount = extractor.trackCount
        val muxerFormat = when {
            outputFile.name.endsWith(".webm", ignoreCase = true) -> MediaMuxer.OutputFormat.MUXER_OUTPUT_WEBM
            else -> MediaMuxer.OutputFormat.MUXER_OUTPUT_MPEG_4
        }
        val muxer = MediaMuxer(outputFile.absolutePath, muxerFormat)
        val indexMap = HashMap<Int, Int>()
        var maxBufferSize = DEFAULT_BUFFER_SIZE
        var videoTrackIndex = -1

        for (i in 0 until trackCount) {
            val format = extractor.getTrackFormat(i)
            val mime = format.getString(MediaFormat.KEY_MIME) ?: continue
            if (mime.startsWith("video/") || mime.startsWith("audio/")) {
                extractor.selectTrack(i)
                val newTrackIndex = muxer.addTrack(format)
                indexMap[i] = newTrackIndex

                if (mime.startsWith("video/") && videoTrackIndex == -1) {
                    videoTrackIndex = i
                }

                if (format.containsKey(MediaFormat.KEY_MAX_INPUT_SIZE)) {
                    val size = format.getInteger(MediaFormat.KEY_MAX_INPUT_SIZE)
                    if (size > maxBufferSize) maxBufferSize = size
                }
            }
        }

        if (indexMap.isEmpty()) {
            extractor.release()
            muxer.release()
            throw IllegalStateException("No extractable video or audio tracks found")
        }

        if (videoTrackIndex != -1) {
            extractor.selectTrack(videoTrackIndex)
        }
        extractor.seekTo(startUs, MediaExtractor.SEEK_TO_PREVIOUS_SYNC)

        var baseTimeUs = extractor.sampleTime
        if (baseTimeUs < 0) baseTimeUs = startUs

        muxer.start()

        val buffer = ByteBuffer.allocateDirect(maxBufferSize)
        val bufferInfo = MediaCodec.BufferInfo()

        while (true) {
            val trackIndex = extractor.sampleTrackIndex
            if (trackIndex < 0) break

            val sampleTime = extractor.sampleTime
            if (sampleTime > endUs) break

            val muxerTrackIndex = indexMap[trackIndex]
            if (muxerTrackIndex != null && sampleTime >= baseTimeUs) {
                bufferInfo.size = extractor.readSampleData(buffer, 0)
                if (bufferInfo.size > 0) {
                    bufferInfo.presentationTimeUs = sampleTime - baseTimeUs
                    bufferInfo.offset = 0
                    val flags = extractor.sampleFlags
                    var muxerFlags = 0
                    if ((flags and MediaExtractor.SAMPLE_FLAG_SYNC) != 0) {
                        muxerFlags = muxerFlags or MediaCodec.BUFFER_FLAG_KEY_FRAME
                    }
                    bufferInfo.flags = muxerFlags

                    muxer.writeSampleData(muxerTrackIndex, buffer, bufferInfo)
                }
            }

            extractor.advance()
        }

        try {
            muxer.stop()
        } finally {
            muxer.release()
            extractor.release()
        }
    }

    private fun scanFile(context: Context, file: File) {
        MediaScannerConnection.scanFile(
            context.applicationContext,
            arrayOf(file.absolutePath),
            null
        ) { _, _ ->
            MediaLibraryEvents.notifyChanged()
        }
    }

    fun getOutputClipFile(inputPath: String, startMs: Long, endMs: Long): File {
        val moviesDir = Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_MOVIES)
        val clipsDir = File(moviesDir, "REX Player")
        if (!clipsDir.exists()) {
            clipsDir.mkdirs()
        }

        val originalName = Uri.parse(inputPath).lastPathSegment
            ?.substringAfterLast('/')
            ?.substringBeforeLast('.')
            ?.takeIf { it.isNotBlank() } ?: "video"

        val extension = when {
            inputPath.endsWith(".webm", ignoreCase = true) -> "webm"
            inputPath.endsWith(".mkv", ignoreCase = true) -> "mkv"
            else -> "mp4"
        }

        val startSec = startMs / 1000
        val endSec = endMs / 1000
        val fileName = "${originalName}_clip_${startSec}s-${endSec}s.$extension"

        return File(clipsDir, fileName)
    }
}
