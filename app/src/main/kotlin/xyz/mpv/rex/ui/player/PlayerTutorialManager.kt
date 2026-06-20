package xyz.mpv.rex.ui.player

import xyz.mpv.rex.preferences.GesturePreferences

class PlayerTutorialManager(
  private val gesturePreferences: GesturePreferences
) {
  // 1. Speed Lock Tutorial Logic
  fun shouldShowSpeedLockHint(): Boolean {
    val count = gesturePreferences.speedLockHintShownCount.get()
    val lockedBefore = gesturePreferences.hasLockedSpeedBefore.get()
    return count < 5 && !lockedBefore
  }

  fun incrementSpeedLockHintCount() {
    val current = gesturePreferences.speedLockHintShownCount.get()
    gesturePreferences.speedLockHintShownCount.set(current + 1)
  }

  fun markSpeedLockCompleted() {
    gesturePreferences.hasLockedSpeedBefore.set(true)
  }

  // 2. Subtitle Drag Tutorial Logic
  fun shouldShowSubtitleDragHint(): Boolean {
    val count = gesturePreferences.subtitlePosHintShownCount.get()
    val draggedBefore = gesturePreferences.hasDraggedSubtitleBefore.get()
    return count < 3 && !draggedBefore
  }

  fun incrementSubtitleDragHintCount() {
    val current = gesturePreferences.subtitlePosHintShownCount.get()
    gesturePreferences.subtitlePosHintShownCount.set(current + 1)
  }

  fun markSubtitleDragCompleted() {
    gesturePreferences.hasDraggedSubtitleBefore.set(true)
  }
}
