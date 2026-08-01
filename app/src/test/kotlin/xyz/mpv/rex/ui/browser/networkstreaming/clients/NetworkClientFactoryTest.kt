package xyz.mpv.rex.ui.browser.networkstreaming.clients

import org.junit.Assert.assertTrue
import org.junit.Test
import xyz.mpv.rex.domain.network.NetworkConnection
import xyz.mpv.rex.domain.network.NetworkProtocol

class NetworkClientFactoryTest {

  @Test
  fun createClient_smbProtocol_returnsSmbClient() {
    val connection = NetworkConnection(
      id = 1,
      name = "Test SMB",
      host = "192.168.1.100",
      protocol = NetworkProtocol.SMB,
      port = 445
    )

    val client = NetworkClientFactory.createClient(connection)

    assertTrue(client is SmbClient)
  }

  @Test
  fun createClient_ftpProtocol_returnsFtpClient() {
    val connection = NetworkConnection(
      id = 2,
      name = "Test FTP",
      host = "ftp.example.com",
      protocol = NetworkProtocol.FTP,
      port = 21
    )

    val client = NetworkClientFactory.createClient(connection)

    assertTrue(client is FtpClient)
  }

  @Test
  fun createClient_webdavProtocol_returnsWebDavClient() {
    val connection = NetworkConnection(
      id = 3,
      name = "Test WebDAV",
      host = "webdav.example.com",
      protocol = NetworkProtocol.WEBDAV,
      port = 443
    )

    val client = NetworkClientFactory.createClient(connection)

    assertTrue(client is WebDavClient)
  }
}
