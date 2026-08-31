package xyz.mpv.rex.feature.webshare

import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.widget.Toast
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.animation.Crossfade
import androidx.compose.animation.core.tween
import androidx.compose.foundation.Image
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Check
import androidx.compose.material.icons.filled.Close
import androidx.compose.material.icons.filled.ContentCopy
import androidx.compose.material.icons.filled.Lock
import androidx.compose.material.icons.filled.Notifications
import androidx.compose.material.icons.filled.Warning
import androidx.core.content.ContextCompat
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.Surface
import androidx.compose.material3.Switch
import androidx.compose.material3.SwitchDefaults
import androidx.compose.material3.Text
import androidx.compose.material3.rememberModalBottomSheetState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import xyz.mpv.rex.domain.media.model.Video
import xyz.mpv.rex.utils.media.MediaFormatter
import java.io.File

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun WebShareSheet(
  videos: List<Video> = emptyList(),
  files: List<File> = emptyList(),
  uris: List<Uri> = emptyList(),
  onDismiss: () -> Unit,
) {
  val context = LocalContext.current
  val sheetState = rememberModalBottomSheetState(skipPartiallyExpanded = true)
  val shareState by WebShareManager.state.collectAsState()
  var copied by remember { mutableStateOf(false) }

  var hasNotificationPermission by remember {
    mutableStateOf(
      if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
        ContextCompat.checkSelfPermission(
          context,
          android.Manifest.permission.POST_NOTIFICATIONS
        ) == PackageManager.PERMISSION_GRANTED
      } else {
        true
      }
    )
  }

  val notificationPermissionLauncher = rememberLauncherForActivityResult(
    contract = ActivityResultContracts.RequestPermission(),
    onResult = { isGranted ->
      hasNotificationPermission = isGranted
      if (isGranted) {
        val updateIntent = Intent(context, WebShareService::class.java).apply {
          action = WebShareService.ACTION_UPDATE
        }
        context.startService(updateIntent)
      }
    }
  )

  // 1. Prepare shareable files on initial composition
  LaunchedEffect(videos, files, uris) {
    val shareables = mutableListOf<WebShareServer.ShareableFile>()

    for (video in videos) {
      val f = File(video.path)
      if (f.exists()) {
        shareables.add(
          WebShareServer.ShareableFile(
            id = video.id.toString(),
            file = f,
            size = f.length(),
            displayName = video.displayName,
            durationFormatted = MediaFormatter.formatDuration(video.duration)
          )
        )
      }
    }

    for (file in files) {
      if (file.exists()) {
        shareables.add(
          WebShareServer.ShareableFile(
            id = file.name.hashCode().toString(),
            file = file,
            size = file.length(),
            displayName = file.name
          )
        )
      }
    }

    for (uri in uris) {
      var displayName = "shared_file_${System.currentTimeMillis()}"
      var size = 0L
      try {
        context.contentResolver.query(uri, null, null, null, null)?.use { cursor ->
          val nameIndex = cursor.getColumnIndex(android.provider.OpenableColumns.DISPLAY_NAME)
          val sizeIndex = cursor.getColumnIndex(android.provider.OpenableColumns.SIZE)
          if (cursor.moveToFirst()) {
            if (nameIndex != -1) displayName = cursor.getString(nameIndex) ?: displayName
            if (sizeIndex != -1) size = cursor.getLong(sizeIndex)
          }
        }
      } catch (e: Exception) {
        // Ignore
      }
      if (size <= 0L) {
        try {
          context.contentResolver.openAssetFileDescriptor(uri, "r")?.use { afd ->
            size = afd.length
          }
        } catch (e: Exception) {
          // Ignore
        }
      }
      shareables.add(
        WebShareServer.ShareableFile(
          id = uri.toString().hashCode().toString(),
          displayName = displayName,
          size = size,
          uri = uri
        )
      )
    }

    if (shareables.isNotEmpty()) {
      WebShareManager.startSharing(context, shareables)
    }
  }

  // Periodic network refresh while sheet is open
  LaunchedEffect(Unit) {
    while (true) {
      kotlinx.coroutines.delay(2000L)
      WebShareManager.refreshNetworkState(context)
    }
  }

  // Reset copied indicator back to copy icon after 2 seconds
  LaunchedEffect(copied) {
    if (copied) {
      kotlinx.coroutines.delay(2000L)
      copied = false
    }
  }

  // If reopening an existing share and sharing was stopped, dismiss sheet
  LaunchedEffect(shareState.isRunning) {
    if (!shareState.isRunning && (videos.isEmpty() && files.isEmpty() && uris.isEmpty())) {
      onDismiss()
    }
  }

  val totalSize = remember(shareState.files) {
    shareState.files.sumOf { it.size }
  }
  val totalSizeFormatted = remember(totalSize) {
    MediaFormatter.formatFileSize(totalSize)
  }

  val qrBitmap = remember(shareState.serverUrl) {
    shareState.serverUrl?.let { url ->
      try {
        QrCodeGenerator.generateQrBitmap(url, sizePx = 400).asImageBitmap()
      } catch (e: Exception) {
        null
      }
    }
  }

  ModalBottomSheet(
    onDismissRequest = {
      // Dismiss UI only — background service continues running
      onDismiss()
    },
    sheetState = sheetState,
    containerColor = MaterialTheme.colorScheme.surface,
    shape = RoundedCornerShape(topStart = 24.dp, topEnd = 24.dp),
  ) {
    Column(
      modifier = Modifier
        .fillMaxWidth()
        .padding(horizontal = 20.dp)
        .padding(bottom = 24.dp)
        .verticalScroll(rememberScrollState()),
      horizontalAlignment = Alignment.CenterHorizontally,
    ) {
      // Header
      Column(
        modifier = Modifier.fillMaxWidth(),
        horizontalAlignment = Alignment.Start,
      ) {
        Text(
          text = "Web Share",
          style = MaterialTheme.typography.titleLarge,
          fontWeight = FontWeight.Bold,
          color = MaterialTheme.colorScheme.onSurface,
        )
        Text(
          text = "${shareState.files.size} ${if (shareState.files.size == 1) "file" else "files"} • $totalSizeFormatted",
          style = MaterialTheme.typography.bodySmall,
          color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
      }

      Spacer(modifier = Modifier.height(14.dp))

      // 1. QR Code Card (Enlarged to 172.dp)
      if (qrBitmap != null) {
        Surface(
          shape = RoundedCornerShape(16.dp),
          color = Color.White,
          shadowElevation = 3.dp,
          modifier = Modifier.size(172.dp),
        ) {
          Box(
            contentAlignment = Alignment.Center,
            modifier = Modifier.padding(8.dp),
          ) {
            Image(
              bitmap = qrBitmap,
              contentDescription = "Scan QR Code",
              modifier = Modifier.size(156.dp),
            )
          }
        }
      }

      // Notification Permission Banner (placed below QR code if not granted on Android 13+)
      if (!hasNotificationPermission && Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
        Spacer(modifier = Modifier.height(10.dp))
        Surface(
          shape = RoundedCornerShape(12.dp),
          color = MaterialTheme.colorScheme.primaryContainer.copy(alpha = 0.45f),
          modifier = Modifier.fillMaxWidth(),
        ) {
          Row(
            modifier = Modifier
              .padding(horizontal = 14.dp, vertical = 10.dp)
              .fillMaxWidth(),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.SpaceBetween,
          ) {
            Row(
              verticalAlignment = Alignment.CenterVertically,
              horizontalArrangement = Arrangement.spacedBy(10.dp),
              modifier = Modifier.weight(1f),
            ) {
              Icon(
                imageVector = Icons.Filled.Notifications,
                contentDescription = null,
                modifier = Modifier.size(22.dp),
                tint = MaterialTheme.colorScheme.primary,
              )
              Column {
                Text(
                  text = "Enable Notifications",
                  style = MaterialTheme.typography.bodyMedium,
                  fontWeight = FontWeight.SemiBold,
                  color = MaterialTheme.colorScheme.onSurface,
                )
                Text(
                  text = "Keep sharing in background & reopen anytime",
                  style = MaterialTheme.typography.bodySmall,
                  color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
              }
            }

            Button(
              onClick = {
                notificationPermissionLauncher.launch(android.Manifest.permission.POST_NOTIFICATIONS)
              },
              shape = RoundedCornerShape(8.dp),
              contentPadding = PaddingValues(horizontal = 12.dp, vertical = 6.dp),
            ) {
              Text(
                text = "Allow",
                style = MaterialTheme.typography.labelMedium,
                fontWeight = FontWeight.Bold,
              )
            }
          }
        }
      }

      Spacer(modifier = Modifier.height(12.dp))

      // 2. Require Security Token Row (Placed below QR Code)
      Surface(
        shape = RoundedCornerShape(12.dp),
        color = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.35f),
        modifier = Modifier.fillMaxWidth(),
      ) {
        Row(
          modifier = Modifier
            .padding(horizontal = 14.dp, vertical = 8.dp)
            .fillMaxWidth(),
          verticalAlignment = Alignment.CenterVertically,
          horizontalArrangement = Arrangement.SpaceBetween,
        ) {
          Row(
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(10.dp),
            modifier = Modifier.weight(1f),
          ) {
            Icon(
              imageVector = Icons.Filled.Lock,
              contentDescription = null,
              modifier = Modifier.size(20.dp),
              tint = if (shareState.isTokenEnabled) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant,
            )
            Column {
              Text(
                text = "Require Security Token",
                style = MaterialTheme.typography.bodyMedium,
                fontWeight = FontWeight.SemiBold,
                color = MaterialTheme.colorScheme.onSurface,
              )
              Text(
                text = if (shareState.isTokenEnabled) "Requires ?t= token to view files" else "Open access (fast & easy)",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
              )
            }
          }

          Switch(
            checked = shareState.isTokenEnabled,
            onCheckedChange = { enabled ->
              WebShareManager.setTokenEnabled(context, enabled)
            },
            thumbContent = {
              Crossfade(
                targetState = shareState.isTokenEnabled,
                animationSpec = tween(durationMillis = 200),
                label = "SwitchIconAnimation"
              ) { isChecked ->
                if (isChecked) {
                  Icon(
                    imageVector = Icons.Filled.Check,
                    contentDescription = null,
                    modifier = Modifier.size(SwitchDefaults.IconSize),
                  )
                } else {
                  Icon(
                    imageVector = Icons.Filled.Close,
                    contentDescription = null,
                    modifier = Modifier.size(SwitchDefaults.IconSize),
                  )
                }
              }
            }
          )
        }
      }

      // 3. Received Files Banner (if any files received from other devices)
      if (shareState.receivedFiles.isNotEmpty()) {
        Spacer(modifier = Modifier.height(10.dp))
        Surface(
          shape = RoundedCornerShape(12.dp),
          color = MaterialTheme.colorScheme.secondaryContainer.copy(alpha = 0.5f),
          modifier = Modifier.fillMaxWidth(),
        ) {
          Column(
            modifier = Modifier
              .padding(horizontal = 14.dp, vertical = 10.dp)
              .fillMaxWidth(),
          ) {
            Row(
              verticalAlignment = Alignment.CenterVertically,
              horizontalArrangement = Arrangement.spacedBy(8.dp),
            ) {
              Text(
                text = "📥 Received (${shareState.receivedFiles.size})",
                style = MaterialTheme.typography.bodyMedium,
                fontWeight = FontWeight.Bold,
                color = MaterialTheme.colorScheme.onSecondaryContainer,
              )
              Text(
                text = "• Download/REX Player",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSecondaryContainer.copy(alpha = 0.75f),
              )
            }
            Spacer(modifier = Modifier.height(4.dp))
            Text(
              text = shareState.receivedFiles.takeLast(3).joinToString("\n") { "• ${it.name}" },
              style = MaterialTheme.typography.bodySmall,
              color = MaterialTheme.colorScheme.onSecondaryContainer,
              maxLines = 3,
              overflow = TextOverflow.Ellipsis,
            )
          }
        }
      }

      Spacer(modifier = Modifier.height(10.dp))

      // 3. URL Display & Copy Pill
      shareState.serverUrl?.let { url ->
        Surface(
          shape = RoundedCornerShape(10.dp),
          color = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.5f),
          modifier = Modifier.fillMaxWidth(),
        ) {
          Row(
            modifier = Modifier.padding(horizontal = 12.dp, vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.SpaceBetween,
          ) {
            Text(
              text = url,
              style = MaterialTheme.typography.bodyMedium,
              fontWeight = FontWeight.Bold,
              color = MaterialTheme.colorScheme.primary,
              maxLines = 2,
              softWrap = true,
              modifier = Modifier.weight(1f).padding(end = 8.dp),
            )

            IconButton(
              onClick = {
                val clipboard = context.getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
                clipboard.setPrimaryClip(ClipData.newPlainText("Web Share Link", url))
                copied = true
                Toast.makeText(context, "Link copied to clipboard", Toast.LENGTH_SHORT).show()
              },
              modifier = Modifier.size(32.dp),
            ) {
              Icon(
                imageVector = if (copied) Icons.Default.Check else Icons.Default.ContentCopy,
                contentDescription = "Copy Link",
                tint = if (copied) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.size(18.dp),
              )
            }
          }
        }
      }

      // No network warning banner if applicable
      if (shareState.networkType == WebShareManager.NetworkType.NONE) {
        Spacer(modifier = Modifier.height(6.dp))
        Row(
          verticalAlignment = Alignment.CenterVertically,
          horizontalArrangement = Arrangement.spacedBy(6.dp),
        ) {
          Icon(
            imageVector = Icons.Filled.Warning,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.error,
            modifier = Modifier.size(16.dp),
          )
          Text(
            text = "Turn on Hotspot or connect to Wi-Fi to share",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.error,
          )
        }
      }

      Spacer(modifier = Modifier.height(10.dp))

      // 4. Guidance Steps
      Column(
        modifier = Modifier
          .fillMaxWidth()
          .padding(horizontal = 4.dp),
        verticalArrangement = Arrangement.spacedBy(4.dp),
      ) {
        Row(
          verticalAlignment = Alignment.CenterVertically,
          horizontalArrangement = Arrangement.spacedBy(8.dp),
        ) {
          Surface(
            shape = RoundedCornerShape(4.dp),
            color = MaterialTheme.colorScheme.primary.copy(alpha = 0.12f),
            modifier = Modifier.size(18.dp),
          ) {
            Box(contentAlignment = Alignment.Center) {
              Text(
                text = "1",
                style = MaterialTheme.typography.labelSmall,
                fontWeight = FontWeight.Bold,
                color = MaterialTheme.colorScheme.primary,
              )
            }
          }
          Text(
            text = "Connect receiving phone to this Hotspot or Wi-Fi",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
          )
        }

        Row(
          verticalAlignment = Alignment.CenterVertically,
          horizontalArrangement = Arrangement.spacedBy(8.dp),
        ) {
          Surface(
            shape = RoundedCornerShape(4.dp),
            color = MaterialTheme.colorScheme.primary.copy(alpha = 0.12f),
            modifier = Modifier.size(18.dp),
          ) {
            Box(contentAlignment = Alignment.Center) {
              Text(
                text = "2",
                style = MaterialTheme.typography.labelSmall,
                fontWeight = FontWeight.Bold,
                color = MaterialTheme.colorScheme.primary,
              )
            }
          }
          Text(
            text = "Scan QR code or type URL in any browser",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
          )
        }
      }

      Spacer(modifier = Modifier.height(14.dp))

      // 5. Stop Sharing Button
      Button(
        onClick = {
          WebShareManager.stopSharing(context)
          onDismiss()
        },
        colors = ButtonDefaults.buttonColors(
          containerColor = MaterialTheme.colorScheme.errorContainer,
          contentColor = MaterialTheme.colorScheme.onErrorContainer,
        ),
        shape = RoundedCornerShape(10.dp),
        modifier = Modifier.fillMaxWidth().height(44.dp),
      ) {
        Text(
          text = "Stop Sharing",
          fontWeight = FontWeight.SemiBold,
          fontSize = 14.sp,
        )
      }
    }
  }
}
