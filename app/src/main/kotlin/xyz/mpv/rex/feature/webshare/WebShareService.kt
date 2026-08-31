package xyz.mpv.rex.feature.webshare

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.net.wifi.WifiManager
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import androidx.core.app.NotificationCompat
import xyz.mpv.rex.MainActivity
import xyz.mpv.rex.R

class WebShareService : Service() {

  companion object {
    const val ACTION_START = "xyz.mpv.rex.feature.webshare.ACTION_START"
    const val ACTION_STOP = "xyz.mpv.rex.feature.webshare.ACTION_STOP"
    const val ACTION_UPDATE = "xyz.mpv.rex.feature.webshare.ACTION_UPDATE"
    private const val NOTIFICATION_ID = 4092
    private const val CHANNEL_ID = "web_share_service_channel"
  }

  private var wakeLock: PowerManager.WakeLock? = null
  private var wifiLock: WifiManager.WifiLock? = null
  private var server: WebShareServer? = null

  override fun onBind(intent: Intent?): IBinder? = null

  override fun onCreate() {
    super.onCreate()
    createNotificationChannel()
  }

  override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
    when (intent?.action) {
      ACTION_STOP -> {
        stopServerAndService()
        return START_NOT_STICKY
      }
      ACTION_START -> {
        startServerAndService()
      }
      ACTION_UPDATE -> {
        updateNotificationState()
      }
    }
    return START_NOT_STICKY
  }

  private fun startServerAndService() {
    val state = WebShareManager.state.value
    if (state.files.isEmpty()) {
      stopSelf()
      return
    }

    acquireLocks()

    try {
      if (server == null) {
        server = WebShareServer(
          port = state.port,
          files = state.files,
          token = state.token,
          context = applicationContext
        )
        server?.start()
        WebShareManager.updateServerState(server)
      }
    } catch (e: Exception) {
      android.util.Log.e("WebShareService", "Failed to start WebShareServer on port ${state.port}", e)
      stopSelf()
      return
    }

    val notification = buildNotification(state)
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
      startForeground(
        NOTIFICATION_ID,
        notification,
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
          ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC
        } else {
          0
        }
      )
    } else {
      startForeground(NOTIFICATION_ID, notification)
    }
  }

  private fun updateNotificationState() {
    val state = WebShareManager.state.value
    if (!state.isRunning) {
      stopServerAndService()
      return
    }
    val notification = buildNotification(state)
    val manager = getSystemService(Context.NOTIFICATION_SERVICE) as? NotificationManager
    manager?.notify(NOTIFICATION_ID, notification)
  }

  private fun stopServerAndService() {
    try {
      server?.stop()
      server = null
      WebShareManager.updateServerState(null)
    } catch (e: Exception) {
      // Ignore
    }

    releaseLocks()
    stopForeground(STOP_FOREGROUND_REMOVE)
    stopSelf()
    WebShareManager.onServiceStopped()
  }

  private fun buildNotification(state: WebShareManager.WebShareState): Notification {
    val openIntent = Intent(this, WebShareActivity::class.java).apply {
      flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP
    }
    val openPendingIntent = PendingIntent.getActivity(
      this,
      0,
      openIntent,
      PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
    )

    val stopIntent = Intent(this, WebShareService::class.java).apply {
      action = ACTION_STOP
    }
    val stopPendingIntent = PendingIntent.getService(
      this,
      1,
      stopIntent,
      PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
    )

    val fileCount = state.files.size
    val receivedCount = state.receivedFiles.size
    val subText = if (receivedCount > 0) {
      "$fileCount shared • $receivedCount received"
    } else {
      if (fileCount == 1) "Sharing 1 file" else "Sharing $fileCount files"
    }
    val contentText = state.latestReceivedFile?.let { "Received: $it" } ?: (state.serverUrl ?: "Local Web Share running")

    return NotificationCompat.Builder(this, CHANNEL_ID)
      .setSmallIcon(R.drawable.baseline_share_24)
      .setContentTitle("REX Player Web Share")
      .setContentText(contentText)
      .setSubText(subText)
      .setOngoing(true)
      .setContentIntent(openPendingIntent)
      .addAction(android.R.drawable.ic_menu_close_clear_cancel, "Stop Sharing", stopPendingIntent)
      .setPriority(NotificationCompat.PRIORITY_LOW)
      .setCategory(NotificationCompat.CATEGORY_SERVICE)
      .build()
  }

  private fun createNotificationChannel() {
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
      val channel = NotificationChannel(
        CHANNEL_ID,
        "Local Web Share",
        NotificationManager.IMPORTANCE_LOW
      ).apply {
        description = "Active notifications when sharing files over local Wi-Fi / Hotspot"
        setShowBadge(false)
      }
      val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
      manager.createNotificationChannel(channel)
    }
  }

  @Suppress("DEPRECATION")
  private fun acquireLocks() {
    try {
      val pm = getSystemService(Context.POWER_SERVICE) as PowerManager
      wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "REXPlayer:WebShareWakeLock").apply {
        acquire(4 * 60 * 60 * 1000L) // 4 hours timeout
      }

      val wm = applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
      wifiLock = wm.createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF, "REXPlayer:WebShareWifiLock").apply {
        acquire()
      }
    } catch (e: Exception) {
      // Ignore lock failures
    }
  }

  private fun releaseLocks() {
    try {
      wakeLock?.let {
        if (it.isHeld) it.release()
      }
      wakeLock = null

      wifiLock?.let {
        if (it.isHeld) it.release()
      }
      wifiLock = null
    } catch (e: Exception) {
      // Ignore
    }
  }

  override fun onDestroy() {
    stopServerAndService()
    super.onDestroy()
  }
}
