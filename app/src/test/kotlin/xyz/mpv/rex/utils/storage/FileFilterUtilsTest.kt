package xyz.mpv.rex.utils.storage

import java.io.File
import java.nio.file.Files
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class FileFilterUtilsTest {
  @Test
  fun noMediaFolder_isControlledIndependentlyByPolicy() {
    withTempDirectory { root ->
      val folder = File(root, "Private").apply { mkdirs() }
      File(folder, ".nomedia").createNewFile()

      assertTrue(FileFilterUtils.shouldSkipFolder(folder))
      assertFalse(
        FileFilterUtils.shouldSkipFolder(
          folder,
          MediaScanPolicy(includeNoMediaContent = true),
        ),
      )
    }
  }

  @Test
  fun systemAndDotHiddenFolders_remainSkippedWhenNoMediaIsEnabled() {
    withTempDirectory { root ->
      val hidden = File(root, ".private").apply { mkdirs() }
      val cache = File(root, "cache").apply { mkdirs() }
      val policy = MediaScanPolicy(includeNoMediaContent = true)

      assertTrue(FileFilterUtils.shouldSkipFolder(hidden, policy))
      assertTrue(FileFilterUtils.shouldSkipFolder(cache, policy))
    }
  }

  @Test
  fun noMediaBoundary_isInheritedByDescendants() {
    withTempDirectory { root ->
      val boundary = File(root, "Private").apply { mkdirs() }
      File(boundary, ".nomedia").createNewFile()
      val descendant = File(boundary, "Clips/More").apply { mkdirs() }

      assertTrue(FileFilterUtils.isWithinNoMediaBoundary(descendant))
    }
  }

  private fun withTempDirectory(block: (File) -> Unit) {
    val directory = Files.createTempDirectory("nomedia-filter-test").toFile()
    try {
      block(directory)
    } finally {
      directory.deleteRecursively()
    }
  }
}
