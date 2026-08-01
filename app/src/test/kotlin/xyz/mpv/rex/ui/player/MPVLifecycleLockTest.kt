package xyz.mpv.rex.ui.player

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class MPVLifecycleLockTest {

  @Test
  fun teardownLifecycle_stateTransitions_correct() = runTest {
    // Initial state should be false
    assertFalse(MPVLifecycleLock.isTearingDown.value)

    // Signal teardown start
    MPVLifecycleLock.onTeardownStart()
    assertTrue(MPVLifecycleLock.isTearingDown.value)

    // Launch async awaiter
    val awaitJob = async {
      MPVLifecycleLock.awaitTeardown()
    }

    assertFalse(awaitJob.isCompleted)

    // Signal teardown complete
    MPVLifecycleLock.onTeardownComplete()
    assertFalse(MPVLifecycleLock.isTearingDown.value)

    // Awaiter should now finish
    awaitJob.await()
    assertTrue(awaitJob.isCompleted)
  }
}
