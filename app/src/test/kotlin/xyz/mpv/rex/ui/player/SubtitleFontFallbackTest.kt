package xyz.mpv.rex.ui.player

import org.junit.Assert.assertEquals
import org.junit.Test

class SubtitleFontFallbackTest {

  private fun resolveFontToApply(preferredFont: String): String {
    return if (preferredFont.isNotBlank()) preferredFont else "subfont, Roboto, sans-serif"
  }

  @Test
  fun resolveFontToApply_blankFontPreference_returnsFallbackFontChain() {
    val result1 = resolveFontToApply("")
    val result2 = resolveFontToApply("   ")

    assertEquals("subfont, Roboto, sans-serif", result1)
    assertEquals("subfont, Roboto, sans-serif", result2)
  }

  @Test
  fun resolveFontToApply_customFontPreference_preservesCustomFont() {
    val result = resolveFontToApply("Open Sans")

    assertEquals("Open Sans", result)
  }
}
