package xyz.mpv.rex.feature.webshare

import android.content.Context
import android.content.Intent
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import java.net.Inet4Address
import java.net.NetworkInterface
import java.util.UUID

object WebShareManager {

  private const val PREFS_NAME = "web_share_prefs"
  private const val KEY_REQUIRE_TOKEN = "require_security_token"

  enum class NetworkType {
    HOTSPOT,
    WIFI,
    NONE
  }

  data class WebShareState(
    val isRunning: Boolean = false,
    val ipAddress: String? = null,
    val port: Int = 8080,
    val token: String? = null,
    val isTokenEnabled: Boolean = false,
    val serverUrl: String? = null,
    val files: List<WebShareServer.ShareableFile> = emptyList(),
    val networkType: NetworkType = NetworkType.NONE,
    val receivedFiles: List<java.io.File> = emptyList(),
    val latestReceivedFile: String? = null,
  )

  private val _state = MutableStateFlow(WebShareState())
  val state: StateFlow<WebShareState> = _state.asStateFlow()

  internal var activeServer: WebShareServer? = null

  fun isTokenEnabledByDefault(context: Context): Boolean {
    return context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
      .getBoolean(KEY_REQUIRE_TOKEN, false)
  }

  fun startSharing(
    context: Context,
    files: List<WebShareServer.ShareableFile>,
    enableToken: Boolean = isTokenEnabledByDefault(context),
  ) {
    if (files.isEmpty()) return

    val (ip, networkType) = getLocalIpAddress(context)
    val port = findAvailablePort(8080)
    val token = if (enableToken) UUID.randomUUID().toString().substring(0, 6) else null
    val baseHost = ip ?: "localhost"
    val fullUrl = if (token != null) "http://$baseHost:$port/?t=$token" else "http://$baseHost:$port/"

    _state.value = WebShareState(
      isRunning = true,
      ipAddress = ip,
      port = port,
      token = token,
      isTokenEnabled = enableToken,
      serverUrl = fullUrl,
      files = files,
      networkType = networkType
    )

    val serviceIntent = Intent(context, WebShareService::class.java).apply {
      action = WebShareService.ACTION_START
    }
    context.startForegroundService(serviceIntent)
  }

  fun setTokenEnabled(context: Context, enabled: Boolean) {
    // Persist preference so it stays on for future sessions
    context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
      .edit()
      .putBoolean(KEY_REQUIRE_TOKEN, enabled)
      .apply()

    val current = _state.value
    if (!current.isRunning) return

    val token = if (enabled) UUID.randomUUID().toString().substring(0, 6) else null
    val baseHost = current.ipAddress ?: "localhost"
    val port = current.port
    val fullUrl = if (token != null) "http://$baseHost:$port/?t=$token" else "http://$baseHost:$port/"

    _state.value = current.copy(
      token = token,
      isTokenEnabled = enabled,
      serverUrl = fullUrl
    )

    // Restart server on same port with updated token setting
    try {
      activeServer?.stop()
      activeServer = WebShareServer(
        port = port,
        files = current.files,
        token = token,
        context = context.applicationContext
      )
      activeServer?.start()

      val updateIntent = Intent(context, WebShareService::class.java).apply {
        action = WebShareService.ACTION_UPDATE
      }
      context.startService(updateIntent)
    } catch (e: Exception) {
      android.util.Log.e("WebShareManager", "Failed to update token on server", e)
    }
  }

  fun stopSharing(context: Context) {
    val serviceIntent = Intent(context, WebShareService::class.java).apply {
      action = WebShareService.ACTION_STOP
    }
    context.startService(serviceIntent)

    activeServer?.stop()
    activeServer = null

    _state.value = WebShareState(isRunning = false)
  }

  internal fun onServiceStopped() {
    activeServer?.stop()
    activeServer = null
    _state.value = WebShareState(isRunning = false)
  }

  fun onFileReceived(context: Context, file: java.io.File) {
    val current = _state.value
    val updated = current.receivedFiles + file
    _state.value = current.copy(
      receivedFiles = updated,
      latestReceivedFile = file.name
    )

    // Notify service to update notification with received file status
    val updateIntent = Intent(context, WebShareService::class.java).apply {
      action = WebShareService.ACTION_UPDATE
    }
    context.startService(updateIntent)
  }

  internal fun updateServerState(server: WebShareServer?) {
    activeServer = server
  }

  fun refreshNetworkState(context: Context) {
    val current = _state.value
    if (!current.isRunning) return
    val (ip, networkType) = getLocalIpAddress(context)
    val port = current.port
    val token = current.token
    val baseHost = ip ?: "localhost"
    val fullUrl = if (token != null) "http://$baseHost:$port/?t=$token" else "http://$baseHost:$port/"

    val ipChanged = current.ipAddress != ip || current.networkType != networkType
    _state.value = current.copy(
      ipAddress = ip,
      networkType = networkType,
      serverUrl = fullUrl
    )

    if (ipChanged) {
      val updateIntent = Intent(context, WebShareService::class.java).apply {
        action = WebShareService.ACTION_UPDATE
      }
      context.startService(updateIntent)
    }
  }

  private fun getLocalIpAddress(context: Context): Pair<String?, NetworkType> {
    try {
      val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as? android.net.ConnectivityManager
      val isWifiConnected = cm?.activeNetwork?.let { network ->
        cm.getNetworkCapabilities(network)?.hasTransport(android.net.NetworkCapabilities.TRANSPORT_WIFI)
      } == true

      val interfaces = NetworkInterface.getNetworkInterfaces()?.toList() ?: emptyList()

      // Ignore cellular (rmnet, ccmni, pdp), VPN (tun), and loopback interfaces
      val localInterfaces = interfaces.filter { intf ->
        intf.isUp && !intf.isLoopback &&
          !intf.name.startsWith("rmnet") &&
          !intf.name.startsWith("ccmni") &&
          !intf.name.startsWith("tun") &&
          !intf.name.startsWith("pdp") &&
          !intf.name.startsWith("dummy")
      }

      // 1. Check for Hotspot (common interface names: ap0, softap, swlan0, wlan1, etc. or IP 192.168.43.1 / 192.168.49.1)
      for (intf in localInterfaces) {
        val name = intf.name.lowercase()
        for (addr in intf.inetAddresses) {
          if (addr is Inet4Address && !addr.isLoopbackAddress) {
            val ip = addr.hostAddress ?: continue
            if (ip.startsWith("192.168.43.") || ip.startsWith("192.168.49.") || ip.startsWith("192.168.50.") ||
              name.contains("ap") || name.contains("softap")) {
              return Pair(ip, NetworkType.HOTSPOT)
            }
          }
        }
      }

      // 2. Check for Wi-Fi if Wi-Fi is actually connected in Android
      if (isWifiConnected) {
        for (intf in localInterfaces) {
          if (intf.name.lowercase().contains("wlan")) {
            for (addr in intf.inetAddresses) {
              if (addr is Inet4Address && !addr.isLoopbackAddress) {
                val ip = addr.hostAddress
                if (ip != null) {
                  return Pair(ip, NetworkType.WIFI)
                }
              }
            }
          }
        }
      }

      // 3. Fallback to any local LAN interface (e.g. eth0, wlan0)
      for (intf in localInterfaces) {
        for (addr in intf.inetAddresses) {
          if (addr is Inet4Address && !addr.isLoopbackAddress) {
            val ip = addr.hostAddress ?: continue
            if (isWifiConnected || ip.startsWith("192.168.") || ip.startsWith("10.") || ip.startsWith("172.")) {
              return Pair(ip, if (isWifiConnected) NetworkType.WIFI else NetworkType.HOTSPOT)
            }
          }
        }
      }
    } catch (e: Exception) {
      // Ignore
    }

    return Pair(null, NetworkType.NONE)
  }

  private fun findAvailablePort(startPort: Int): Int {
    for (port in startPort..(startPort + 10)) {
      try {
        java.net.ServerSocket(port).use {
          return port
        }
      } catch (e: Exception) {
        // Port taken, try next
      }
    }
    return startPort
  }
}
