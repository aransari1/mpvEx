package xyz.mpv.rex.utils.media

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class M3UParserTest {

  @Test
  fun parseContent_simpleM3u_returnsSuccessWithItems() {
    val m3uContent = """
      #EXTM3U
      #EXTINF:123 tvg-logo="http://example.com/logo.png" group-title="Movies",Sample Movie
      http://example.com/movie.mp4
      #EXTINF:-1,Sample Live Stream
      http://example.com/live.m3u8
    """.trimIndent()

    val result = M3UParser.parseContent(m3uContent, "sample_playlist.m3u")

    assertTrue(result is M3UParseResult.Success)
    val success = result as M3UParseResult.Success
    assertEquals("sample playlist", success.playlistName)
    assertEquals(2, success.items.size)

    val item1 = success.items[0]
    assertEquals("http://example.com/movie.mp4", item1.url)
    assertEquals("Sample Movie", item1.title)
    assertEquals(123, item1.duration)
    assertEquals("http://example.com/logo.png", item1.tvgLogo)
    assertEquals("Movies", item1.groupTitle)

    val item2 = success.items[1]
    assertEquals("http://example.com/live.m3u8", item2.url)
    assertEquals("Sample Live Stream", item2.title)
    assertEquals(-1, item2.duration)
  }

  @Test
  fun parseContent_hlsStreamPlaylist_returnsError() {
    val hlsContent = """
      #EXTM3U
      #EXT-X-VERSION:3
      #EXT-X-TARGETDURATION:10
      #EXTINF:10.0,
      segment1.ts
    """.trimIndent()

    val result = M3UParser.parseContent(hlsContent)

    assertTrue(result is M3UParseResult.Error)
    val error = result as M3UParseResult.Error
    assertEquals("HLS stream playlist", error.message)
  }

  @Test
  fun parseContent_emptyPlaylist_returnsError() {
    val emptyContent = ""

    val result = M3UParser.parseContent(emptyContent)

    assertTrue(result is M3UParseResult.Error)
    val error = result as M3UParseResult.Error
    assertEquals("Playlist is empty", error.message)
  }

  @Test
  fun parseContent_relativeUrls_resolvesWithBaseUrl() {
    val content = """
      #EXTM3U
      #EXTINF:60,Relative Video
      video2.mp4
    """.trimIndent()

    val result = M3UParser.parseContent(content, "http://example.com/playlists/list.m3u")

    assertTrue(result is M3UParseResult.Success)
    val success = result as M3UParseResult.Success
    assertEquals(1, success.items.size)
    assertEquals("http://example.com/playlists/video2.mp4", success.items[0].url)
  }
}
