package xyz.mpv.rex.ui.preferences

import android.content.Intent
import android.widget.Toast
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.ArrowBack
import androidx.compose.material.icons.outlined.CleaningServices
import androidx.compose.material.icons.outlined.CreateNewFolder
import androidx.compose.material.icons.outlined.Folder
import androidx.compose.material.icons.outlined.Refresh
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import xyz.mpv.rex.R
import xyz.mpv.rex.database.repository.HybridMediaIndexRepository
import xyz.mpv.rex.database.repository.VideoMetadataCacheRepository
import xyz.mpv.rex.preferences.BrowserPreferences
import xyz.mpv.rex.preferences.FoldersPreferences
import xyz.mpv.rex.preferences.preference.collectAsState
import xyz.mpv.rex.presentation.Screen
import xyz.mpv.rex.ui.utils.LocalBackStack
import xyz.mpv.rex.utils.media.MediaLibraryEvents
import xyz.mpv.rex.utils.media.OpenDocumentTreeContract
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable
import me.zhanghai.compose.preference.Preference
import me.zhanghai.compose.preference.ProvidePreferenceLocals
import me.zhanghai.compose.preference.SwitchPreference
import org.koin.compose.koinInject

@Serializable
object MediaLibraryPreferencesScreen : Screen {
  @OptIn(ExperimentalMaterial3Api::class)
  @Composable
  override fun Content() {
    val browserPreferences = koinInject<BrowserPreferences>()
    val foldersPreferences = koinInject<FoldersPreferences>()
    val hybridMediaIndex = koinInject<HybridMediaIndexRepository>()
    val metadataCache = koinInject<VideoMetadataCacheRepository>()

    val backstack = LocalBackStack.current
    val context = LocalContext.current
    val scope = rememberCoroutineScope()

    val includeNoMediaContent by browserPreferences.includeNoMediaContent.collectAsState()
    val showAudioFiles by browserPreferences.showAudioFiles.collectAsState()
    val libraryScanRoots by foldersPreferences.libraryScanRoots.collectAsState()

    Scaffold(
      topBar = {
        TopAppBar(
          title = {
            Text(
              text = stringResource(R.string.pref_media_library_title),
              style = MaterialTheme.typography.headlineSmall,
              fontWeight = FontWeight.ExtraBold,
              color = MaterialTheme.colorScheme.primary,
            )
          },
          navigationIcon = {
            IconButton(onClick = backstack::removeLastOrNull) {
              Icon(
                Icons.AutoMirrored.Outlined.ArrowBack,
                contentDescription = null,
                tint = MaterialTheme.colorScheme.secondary,
              )
            }
          },
        )
      },
    ) { padding ->
      val navBarHeight = xyz.mpv.rex.ui.browser.LocalNavigationBarHeight.current
      ProvidePreferenceLocals {
        LazyColumn(
          modifier = Modifier
            .fillMaxSize()
            .padding(padding),
          contentPadding = androidx.compose.foundation.layout.PaddingValues(bottom = navBarHeight + 16.dp),
        ) {
          // Content & Discovery Section
          item {
            PreferenceSectionHeader(title = stringResource(R.string.pref_category_content_discovery))
          }

          item {
            PreferenceCard {
              SwitchPreference(
                value = includeNoMediaContent,
                onValueChange = { newValue ->
                  browserPreferences.includeNoMediaContent.set(newValue)
                  MediaLibraryEvents.notifyChanged()
                  scope.launch(Dispatchers.IO) {
                    runCatching { hybridMediaIndex.ensureFresh(force = true, userInitiated = true) }
                  }
                },
                title = { Text(text = stringResource(R.string.pref_include_no_media_content_title)) },
                summary = {
                  Text(
                    text = stringResource(R.string.pref_include_no_media_content_summary),
                    color = MaterialTheme.colorScheme.outline,
                  )
                },
              )

              PreferenceDivider()

              SwitchPreference(
                value = showAudioFiles,
                onValueChange = { newValue ->
                  browserPreferences.showAudioFiles.set(newValue)
                  MediaLibraryEvents.notifyChanged()
                },
                title = { Text(text = stringResource(R.string.pref_show_audio_files_title)) },
                summary = {
                  Text(
                    text = stringResource(R.string.pref_show_audio_files_summary),
                    color = MaterialTheme.colorScheme.outline,
                  )
                },
              )
            }
          }

          // Storage & Exclusions Section
          item {
            PreferenceSectionHeader(title = stringResource(R.string.pref_category_storage_exclusions))
          }

          item {
            PreferenceCard {
              Preference(
                title = { Text(text = stringResource(R.string.pref_folders_title)) },
                summary = {
                  Text(
                    text = stringResource(R.string.pref_folders_summary),
                    color = MaterialTheme.colorScheme.outline,
                  )
                },
                onClick = { backstack.add(FoldersPreferencesScreen) },
              )

              PreferenceDivider()

              Preference(
                title = { Text(text = stringResource(R.string.pref_library_roots_title)) },
                summary = {
                  Text(
                    text = if (libraryScanRoots.isEmpty()) {
                      stringResource(R.string.pref_library_roots_empty_title)
                    } else {
                      stringResource(R.string.pref_library_root_count, libraryScanRoots.size)
                    },
                    color = MaterialTheme.colorScheme.outline,
                  )
                },
                onClick = { backstack.add(LibraryRootsPreferencesScreen) },
              )
            }
          }

          // Library Maintenance Section
          item {
            PreferenceSectionHeader(title = stringResource(R.string.pref_category_library_maintenance))
          }

          item {
            PreferenceCard {
              Preference(
                title = { Text(text = stringResource(R.string.pref_rescan_library_title)) },
                summary = {
                  Text(
                    text = stringResource(R.string.pref_rescan_library_summary),
                    color = MaterialTheme.colorScheme.outline,
                  )
                },
                onClick = {
                  scope.launch(Dispatchers.IO) {
                    runCatching { hybridMediaIndex.ensureFresh(force = true, userInitiated = true) }
                  }
                  Toast.makeText(context, context.getString(R.string.pref_rescan_started_toast), Toast.LENGTH_SHORT).show()
                },
              )

              PreferenceDivider()

              Preference(
                title = { Text(text = stringResource(R.string.pref_clear_metadata_cache_title)) },
                summary = {
                  Text(
                    text = stringResource(R.string.pref_clear_metadata_cache_summary),
                    color = MaterialTheme.colorScheme.outline,
                  )
                },
                onClick = {
                  scope.launch(Dispatchers.IO) {
                    runCatching { metadataCache.clearAll() }
                  }
                  Toast.makeText(context, context.getString(R.string.pref_cache_cleared_toast), Toast.LENGTH_SHORT).show()
                },
              )
            }
          }
        }
      }
    }
  }
}
