package xyz.mpv.rex.feature.webshare

import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.ImageBitmap
import androidx.compose.ui.graphics.toArgb
import android.graphics.Bitmap

/**
 * A lightweight, self-contained QR Code matrix generator in pure Kotlin.
 * Supports Byte-mode encoding with Reed-Solomon Error Correction.
 * Renders directly to Android Bitmap / Compose ImageBitmap.
 */
object QrCodeGenerator {

  fun generateQrBitmap(content: String, sizePx: Int = 512): Bitmap {
    val matrix = encodeToMatrix(content)
    val matrixSize = matrix.size
    val scale = (sizePx / matrixSize).coerceAtLeast(1)
    val actualSize = matrixSize * scale

    val pixels = IntArray(actualSize * actualSize)
    val black = Color.Black.toArgb()
    val white = Color.White.toArgb()

    for (y in 0 until actualSize) {
      val my = y / scale
      val rowOffset = y * actualSize
      for (x in 0 until actualSize) {
        val mx = x / scale
        val isBlack = my < matrixSize && mx < matrixSize && matrix[my][mx]
        pixels[rowOffset + x] = if (isBlack) black else white
      }
    }

    val bitmap = Bitmap.createBitmap(actualSize, actualSize, Bitmap.Config.ARGB_8888)
    bitmap.setPixels(pixels, 0, actualSize, 0, 0, actualSize, actualSize)
    return bitmap
  }

  // --- Core QR Encoding Logic ---

  private fun encodeToMatrix(text: String): Array<BooleanArray> {
    val bytes = text.toByteArray(Charsets.ISO_8859_1)
    val numBytes = bytes.size

    // Determine minimal QR Version (Byte mode, Error Correction Level M)
    val (version, totalCodewords, dataCodewords, ecBlocks) = selectVersion(numBytes)
    val dimension = 17 + version * 4

    // 1. Bit Buffer & Header
    val bitBuffer = mutableListOf<Int>()
    appendBits(bitBuffer, 0b0100, 4) // Byte mode indicator
    val countBits = if (version <= 9) 8 else 16
    appendBits(bitBuffer, numBytes, countBits) // Character count indicator

    for (b in bytes) {
      appendBits(bitBuffer, b.toInt() and 0xFF, 8)
    }

    // Terminator
    val capacityBits = dataCodewords * 8
    val terminatorLength = (capacityBits - bitBuffer.size).coerceIn(0, 4)
    appendBits(bitBuffer, 0, terminatorLength)

    // Pad to byte boundary
    while (bitBuffer.size % 8 != 0) {
      bitBuffer.add(0)
    }

    // Pad bytes
    val padBytes = intArrayOf(0xEC, 0x11)
    var padIdx = 0
    while (bitBuffer.size < capacityBits) {
      appendBits(bitBuffer, padBytes[padIdx % 2], 8)
      padIdx++
    }

    // Convert bits to bytes
    val dataBytes = IntArray(dataCodewords)
    for (i in 0 until dataCodewords) {
      var byteVal = 0
      for (b in 0 until 8) {
        byteVal = (byteVal shl 1) or bitBuffer[i * 8 + b]
      }
      dataBytes[i] = byteVal
    }

    // 2. Generate Error Correction Codewords
    val ecCodewordsPerBlock = (totalCodewords - dataCodewords) / ecBlocks
    val finalCodewords = generateInterleavedCodewords(dataBytes, dataCodewords, totalCodewords, ecBlocks, ecCodewordsPerBlock)

    // 3. Populate Matrix with Patterns & Data
    val matrix = Array(dimension) { BooleanArray(dimension) }
    val isFunction = Array(dimension) { BooleanArray(dimension) }

    // Finder patterns
    placeFinderPattern(matrix, isFunction, 0, 0)
    placeFinderPattern(matrix, isFunction, dimension - 7, 0)
    placeFinderPattern(matrix, isFunction, 0, dimension - 7)

    // Timing patterns
    for (i in 8 until dimension - 8) {
      matrix[6][i] = (i % 2 == 0)
      isFunction[6][i] = true
      matrix[i][6] = (i % 2 == 0)
      isFunction[i][6] = true
    }

    // Alignment patterns (for version >= 2)
    if (version >= 2) {
      val alignPositions = getAlignmentPatternPositions(version)
      for (r in alignPositions) {
        for (c in alignPositions) {
          if (!isFunction[r][c]) {
            placeAlignmentPattern(matrix, isFunction, r - 2, c - 2)
          }
        }
      }
    }

    // Reserve format information areas
    for (i in 0..8) {
      isFunction[8][i] = true
      isFunction[i][8] = true
      isFunction[8][dimension - 1 - i] = true
      isFunction[dimension - 1 - i][8] = true
    }
    isFunction[dimension - 8][8] = true

    // Place data bits with best mask (Mask 0: (row + col) % 2 == 0)
    placeDataBits(matrix, isFunction, finalCodewords, dimension)

    // Place Format Information (Level M, Mask 0 -> 0b10000 xor 0b101010000010010 = 0x5412)
    placeFormatInfo(matrix, dimension, 0x5412)

    // Add quiet zone (4 modules on all sides)
    val quietZone = 4
    val finalDim = dimension + quietZone * 2
    val result = Array(finalDim) { BooleanArray(finalDim) }
    for (r in 0 until dimension) {
      for (c in 0 until dimension) {
        result[r + quietZone][c + quietZone] = matrix[r][c]
      }
    }

    return result
  }

  private fun selectVersion(numBytes: Int): VersionInfo {
    // Capacity for Level M Error Correction
    val table = listOf(
      VersionInfo(1, 26, 16, 1),
      VersionInfo(2, 44, 28, 1),
      VersionInfo(3, 70, 44, 1),
      VersionInfo(4, 100, 64, 2),
      VersionInfo(5, 134, 86, 2),
      VersionInfo(6, 172, 108, 4),
      VersionInfo(7, 196, 124, 4),
      VersionInfo(8, 242, 154, 4),
      VersionInfo(9, 292, 182, 5),
      VersionInfo(10, 346, 216, 5),
    )
    for (v in table) {
      val maxDataBytes = v.dataCodewords - 3 // Overhead for mode & count
      if (numBytes <= maxDataBytes) return v
    }
    return table.last()
  }

  private data class VersionInfo(val version: Int, val totalCodewords: Int, val dataCodewords: Int, val ecBlocks: Int)

  private fun appendBits(buffer: MutableList<Int>, value: Int, count: Int) {
    for (i in count - 1 downTo 0) {
      buffer.add((value ushr i) and 1)
    }
  }

  private fun placeFinderPattern(matrix: Array<BooleanArray>, isFunc: Array<BooleanArray>, row: Int, col: Int) {
    for (r in -1..7) {
      for (c in -1..7) {
        val mr = row + r
        val mc = col + c
        if (mr in matrix.indices && mc in matrix.indices) {
          val isBlack = (r in 0..6 && (c == 0 || c == 6)) ||
                        (c in 0..6 && (r == 0 || r == 6)) ||
                        (r in 2..4 && c in 2..4)
          matrix[mr][mc] = isBlack
          isFunc[mr][mc] = true
        }
      }
    }
  }

  private fun placeAlignmentPattern(matrix: Array<BooleanArray>, isFunc: Array<BooleanArray>, row: Int, col: Int) {
    for (r in 0..4) {
      for (c in 0..4) {
        val isBlack = (r == 0 || r == 4 || c == 0 || c == 4 || (r == 2 && c == 2))
        matrix[row + r][col + c] = isBlack
        isFunc[row + r][col + c] = true
      }
    }
  }

  private fun getAlignmentPatternPositions(version: Int): IntArray = when (version) {
    2 -> intArrayOf(6, 18)
    3 -> intArrayOf(6, 22)
    4 -> intArrayOf(6, 26)
    5 -> intArrayOf(6, 30)
    6 -> intArrayOf(6, 34)
    7 -> intArrayOf(6, 22, 38)
    8 -> intArrayOf(6, 24, 42)
    9 -> intArrayOf(6, 26, 46)
    10 -> intArrayOf(6, 28, 50)
    else -> intArrayOf(6, 18)
  }

  private fun placeDataBits(matrix: Array<BooleanArray>, isFunc: Array<BooleanArray>, data: IntArray, dimension: Int) {
    var bitIdx = 0
    val totalBits = data.size * 8
    var upwards = true
    var c = dimension - 1

    while (c > 0) {
      if (c == 6) c-- // Skip vertical timing pattern
      val rows = if (upwards) (dimension - 1 downTo 0) else (0 until dimension)
      for (r in rows) {
        for (colOffset in 0..1) {
          val currentCol = c - colOffset
          if (!isFunc[r][currentCol]) {
            var bit = 0
            if (bitIdx < totalBits) {
              val byteVal = data[bitIdx / 8]
              bit = (byteVal ushr (7 - (bitIdx % 8))) and 1
              bitIdx++
            }
            // Apply Mask 0: (row + col) % 2 == 0
            val mask = (r + currentCol) % 2 == 0
            matrix[r][currentCol] = if (mask) (bit xor 1) == 1 else (bit == 1)
          }
        }
      }
      upwards = !upwards
      c -= 2
    }
  }

  private fun placeFormatInfo(matrix: Array<BooleanArray>, dim: Int, formatBits: Int) {
    val positions = listOf(
      Pair(8, 0), Pair(8, 1), Pair(8, 2), Pair(8, 3), Pair(8, 4), Pair(8, 5), Pair(8, 7), Pair(8, 8),
      Pair(7, 8), Pair(5, 8), Pair(4, 8), Pair(3, 8), Pair(2, 8), Pair(1, 8), Pair(0, 8)
    )
    for (i in 0 until 15) {
      val bit = (formatBits ushr (14 - i)) and 1 == 1
      val p1 = positions[i]
      matrix[p1.first][p1.second] = bit

      if (i < 8) {
        matrix[dim - 1 - i][8] = bit
      } else {
        matrix[8][dim - 15 + i] = bit
      }
    }
  }

  // --- Reed-Solomon Error Correction ---

  private fun generateInterleavedCodewords(
    data: IntArray,
    dataLen: Int,
    totalLen: Int,
    ecBlocks: Int,
    ecLenPerBlock: Int
  ): IntArray {
    val blockDataLen = dataLen / ecBlocks
    val dataBlocks = Array(ecBlocks) { IntArray(blockDataLen) }
    val ecBlocksList = Array(ecBlocks) { IntArray(ecLenPerBlock) }

    for (b in 0 until ecBlocks) {
      for (i in 0 until blockDataLen) {
        dataBlocks[b][i] = data[b * blockDataLen + i]
      }
      ecBlocksList[b] = calculateReedSolomon(dataBlocks[b], ecLenPerBlock)
    }

    val result = IntArray(totalLen)
    var idx = 0
    for (i in 0 until blockDataLen) {
      for (b in 0 until ecBlocks) {
        result[idx++] = dataBlocks[b][i]
      }
    }
    for (i in 0 until ecLenPerBlock) {
      for (b in 0 until ecBlocks) {
        result[idx++] = ecBlocksList[b][i]
      }
    }
    return result
  }

  private fun calculateReedSolomon(data: IntArray, ecLength: Int): IntArray {
    val genPoly = generatorPolynomial(ecLength)
    val result = IntArray(data.size + ecLength)
    System.arraycopy(data, 0, result, 0, data.size)

    for (i in data.indices) {
      val coef = result[i]
      if (coef != 0) {
        val factor = logTable[coef]
        for (j in genPoly.indices) {
          result[i + j] = result[i + j] xor expTable[(genPoly[j] + factor) % 255]
        }
      }
    }
    return result.copyOfRange(data.size, result.size)
  }

  private val expTable = IntArray(256)
  private val logTable = IntArray(256)

  init {
    var x = 1
    for (i in 0 until 255) {
      expTable[i] = x
      logTable[x] = i
      x = x shl 1
      if (x >= 256) x = x xor 0x11D
    }
    expTable[255] = expTable[0]
  }

  private fun generatorPolynomial(degree: Int): IntArray {
    var poly = intArrayOf(1)
    for (i in 0 until degree) {
      val next = IntArray(poly.size + 1)
      for (j in poly.indices) {
        next[j] = next[j] xor expTable[(logTable[poly[j]] + i) % 255]
        next[j + 1] = next[j + 1] xor poly[j]
      }
      poly = next
    }
    return poly.map { logTable[it] }.toIntArray()
  }
}
