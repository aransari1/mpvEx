package xyz.mpv.rex

import android.app.Application
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.database.ContentObserver
import android.os.Handler
import android.os.Looper
import android.provider.MediaStore
import androidx.core.content.ContextCompat
import xyz.mpv.rex.database.repository.VideoMetadataCacheRepository
import xyz.mpv.rex.database.repository.HybridMediaIndexRepository
import xyz.mpv.rex.di.DatabaseModule
import xyz.mpv.rex.di.FileManagerModule
import xyz.mpv.rex.di.PreferencesModule
import xyz.mpv.rex.presentation.crash.CrashActivity
import xyz.mpv.rex.presentation.crash.GlobalExceptionHandler
import xyz.mpv.rex.utils.media.MediaLibraryEvents
import `is`.xyz.mpv.FastThumbnails
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.merge
import org.koin.android.ext.android.inject
import org.koin.android.ext.koin.androidContext
import org.koin.core.context.startKoin
import org.koin.core.annotation.KoinExperimentalAPI

@OptIn(KoinExperimentalAPI::class, kotlinx.coroutines.FlowPreview::class)
class App : Application() {
  private val applicationScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
  private val metadataCache: VideoMetadataCacheRepository by inject()
  private val hybridMediaIndex: HybridMediaIndexRepository by inject()
  private val advancedPreferences: xyz.mpv.rex.preferences.AdvancedPreferences by inject()
  private val mediaStoreInvalidations = MutableSharedFlow<Unit>(extraBufferCapacity = 1)
  private val rootInvalidations = MutableSharedFlow<Unit>(extraBufferCapacity = 1)

  override fun attachBaseContext(base: Context) {
    super.attachBaseContext(xyz.mpv.rex.utils.locale.LocaleHelper.wrapContext(base))
  }

  override fun onCreate() {
    super.onCreate()

    // Initialize Koin
    startKoin {
      androidContext(this@App)
      modules(
        PreferencesModule,
        DatabaseModule,
        FileManagerModule,
        xyz.mpv.rex.di.domainModule,
      )
    }

    Thread.setDefaultUncaughtExceptionHandler(GlobalExceptionHandler(applicationContext, CrashActivity::class.java))

    FastThumbnails.initialize(this)

    // Sync MediaInfoActivity status with user preference
    advancedPreferences.syncMediaInfoActivityStatus(this)

    registerHybridIndexObservers()

    applicationScope.launch {
      runCatching { hybridMediaIndex.ensureFresh() }
    }
    applicationScope.launch {
      mediaStoreInvalidations
        .debounce(1_000)
        .collect {
          runCatching { hybridMediaIndex.refreshMediaStore() }
        }
    }
    applicationScope.launch {
      rootInvalidations
        .debounce(1_000)
        .collect {
          runCatching { hybridMediaIndex.ensureFresh(force = true) }
        }
    }

    // Perform cache maintenance on app startup (non-blocking)
    applicationScope.launch {
      runCatching {
        metadataCache.performMaintenance()
      }
    }
  }

  private fun registerHybridIndexObservers() {
    val observer = object : ContentObserver(Handler(Looper.getMainLooper())) {
      override fun onChange(selfChange: Boolean) {
        mediaStoreInvalidations.tryEmit(Unit)
      }
    }
    contentResolver.registerContentObserver(
      MediaStore.Video.Media.EXTERNAL_CONTENT_URI,
      true,
      observer,
    )
    contentResolver.registerContentObserver(
      MediaStore.Audio.Media.EXTERNAL_CONTENT_URI,
      true,
      observer,
    )

    val storageReceiver = object : BroadcastReceiver() {
      override fun onReceive(context: Context?, intent: Intent?) {
        rootInvalidations.tryEmit(Unit)
      }
    }
    val storageFilter = IntentFilter().apply {
      addAction(Intent.ACTION_MEDIA_MOUNTED)
      addAction(Intent.ACTION_MEDIA_UNMOUNTED)
      addAction(Intent.ACTION_MEDIA_REMOVED)
      addDataScheme("file")
    }
    ContextCompat.registerReceiver(
      this,
      storageReceiver,
      storageFilter,
      ContextCompat.RECEIVER_EXPORTED,
    )
  }
}
