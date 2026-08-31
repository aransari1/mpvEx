package xyz.mpv.rex.feature.webshare

import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.SystemBarStyle
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import xyz.mpv.rex.ui.theme.MpvexTheme

/**
 * Handles incoming system-wide share intents (ACTION_SEND and ACTION_SEND_MULTIPLE)
 * from external apps (e.g. Gallery, File Manager, WhatsApp, Downloads)
 * to share files immediately via mpvRex Web Share.
 */
class WebShareActivity : ComponentActivity() {

  override fun onCreate(savedInstanceState: Bundle?) {
    enableEdgeToEdge(
      statusBarStyle = SystemBarStyle.auto(
        android.graphics.Color.TRANSPARENT,
        android.graphics.Color.TRANSPARENT,
      ),
      navigationBarStyle = SystemBarStyle.auto(
        android.graphics.Color.TRANSPARENT,
        android.graphics.Color.TRANSPARENT,
      )
    )
    super.onCreate(savedInstanceState)
    handleIntent(intent)
  }

  override fun onNewIntent(intent: Intent) {
    super.onNewIntent(intent)
    setIntent(intent)
    handleIntent(intent)
  }

  private fun handleIntent(intent: Intent?) {
    val incomingUris = extractUrisFromIntent(intent)
    val isRunning = WebShareManager.state.value.isRunning

    if (incomingUris.isEmpty() && !isRunning) {
      Toast.makeText(this, "No active Web Share session", Toast.LENGTH_SHORT).show()
      finish()
      return
    }

    setContent {
      MpvexTheme {
        Box(
          modifier = Modifier
            .fillMaxSize()
            .background(Color.Transparent),
          contentAlignment = Alignment.BottomCenter
        ) {
          WebShareSheet(
            uris = incomingUris,
            onDismiss = { finish() }
          )
        }
      }
    }
  }

  @Suppress("DEPRECATION")
  private fun extractUrisFromIntent(intent: Intent?): List<Uri> {
    if (intent == null) return emptyList()
    val action = intent.action
    val result = mutableListOf<Uri>()

    when (action) {
      Intent.ACTION_SEND -> {
        val streamUri = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
          intent.getParcelableExtra(Intent.EXTRA_STREAM, Uri::class.java)
        } else {
          intent.getParcelableExtra(Intent.EXTRA_STREAM)
        }
        if (streamUri != null) {
          result.add(streamUri)
        } else if (intent.data != null) {
          result.add(intent.data!!)
        }
      }
      Intent.ACTION_SEND_MULTIPLE -> {
        val streamUris = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
          intent.getParcelableArrayListExtra(Intent.EXTRA_STREAM, Uri::class.java)
        } else {
          intent.getParcelableArrayListExtra(Intent.EXTRA_STREAM)
        }
        if (streamUris != null) {
          result.addAll(streamUris)
        }
      }
    }

    // Also check clipData if extra stream is empty
    if (result.isEmpty() && intent.clipData != null) {
      val clipData = intent.clipData!!
      for (i in 0 until clipData.itemCount) {
        val item = clipData.getItemAt(i)
        val uri = item.uri
        if (uri != null) {
          result.add(uri)
        }
      }
    }

    return result
  }
}
