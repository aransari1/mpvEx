package xyz.mpv.rex.ui.player

import android.util.Log
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first

/**
 * Non-blocking synchronization manager for libmpv native teardown and creation.
 *
 * Ensures that if a previous PlayerActivity instance is executing native `MPVLib.destroy()`,
 * a newly launched PlayerActivity instance cleanly awaits native teardown completion
 * using Kotlin StateFlows without blocking the Android UI main thread.
 */
object MPVLifecycleLock {
  private const val TAG = "MPVLifecycleLock"
  private val _isTearingDown = MutableStateFlow(false)
  val isTearingDown = _isTearingDown.asStateFlow()

  /**
   * Called when MPV teardown begins in PlayerActivity.cleanupMPV().
   */
  fun onTeardownStart() {
    _isTearingDown.value = true
    Log.d(TAG, "Native MPV teardown started")
  }

  /**
   * Called when MPV teardown completes in PlayerActivity.cleanupMPV().
   */
  fun onTeardownComplete() {
    _isTearingDown.value = false
    Log.d(TAG, "Native MPV teardown completed")
  }

  /**
   * Non-blockingly suspends until any ongoing native MPV teardown finishes.
   */
  suspend fun awaitTeardown() {
    if (!_isTearingDown.value) return

    Log.d(TAG, "Awaiting native MPV teardown completion...")
    _isTearingDown.first { !it }
    Log.d(TAG, "Native MPV teardown wait finished")
  }
}
