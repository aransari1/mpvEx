package xyz.mpv.rex.ui.browser.components

import androidx.compose.animation.core.animateDpAsState
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.tween
import androidx.compose.foundation.background
import androidx.compose.foundation.gestures.awaitEachGesture
import androidx.compose.foundation.gestures.awaitFirstDown
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.offset
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyListState
import androidx.compose.foundation.lazy.grid.LazyGridState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.MaterialTheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.derivedStateOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.alpha
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.IntOffset
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlin.math.roundToInt

@Composable
fun FastScrollbar(
  modifier: Modifier = Modifier,
  listState: LazyListState? = null,
  gridState: LazyGridState? = null,
  thumbColor: Color = MaterialTheme.colorScheme.primary,
  thumbThickness: Dp = 4.dp,
  thumbExpandedThickness: Dp = 8.dp,
  thumbMinHeight: Dp = 56.dp,
  autoHide: Boolean = true,
  hideDelayMillis: Long = 1500L,
) {
  val coroutineScope = rememberCoroutineScope()
  val density = LocalDensity.current
  var isDragging by remember { mutableStateOf(false) }
  var dragOffsetY by remember { mutableFloatStateOf(0f) }

  // Check if scrolling is in progress
  val isScrolling = listState?.isScrollInProgress == true || gridState?.isScrollInProgress == true

  // Auto-hide logic
  var showThumb by remember { mutableStateOf(false) }

  LaunchedEffect(isScrolling, isDragging) {
    if (isScrolling || isDragging) {
      showThumb = true
    } else if (autoHide) {
      delay(hideDelayMillis)
      showThumb = false
    }
  }

  val alpha by animateFloatAsState(
    targetValue = if (!autoHide || showThumb || isDragging) 1f else 0f,
    animationSpec = tween(durationMillis = 250),
    label = "scrollbar_alpha"
  )

  val currentThumbThickness by animateDpAsState(
    targetValue = if (isDragging) thumbExpandedThickness else thumbThickness,
    animationSpec = tween(durationMillis = 150),
    label = "scrollbar_thickness"
  )

  val touchTargetWidth = 48.dp

  BoxWithConstraints(modifier = modifier.width(touchTargetWidth)) {
    val trackHeightPx = constraints.maxHeight.toFloat()
    if (trackHeightPx <= 0f) return@BoxWithConstraints

    val thumbMinHeightPx = with(density) { thumbMinHeight.toPx() }
    val thumbMaxHeightPx = trackHeightPx * 0.35f
    val effectiveThumbHeightPx = thumbMinHeightPx.coerceIn(thumbMinHeightPx, thumbMaxHeightPx.coerceAtLeast(thumbMinHeightPx))
    val maxScrollableDistancePx = (trackHeightPx - effectiveThumbHeightPx).coerceAtLeast(0f)

    // Calculate continuous, smooth scroll progress [0f, 1f]
    val scrollProgress by remember(listState, gridState, trackHeightPx) {
      derivedStateOf {
        when {
          listState != null -> {
            val layoutInfo = listState.layoutInfo
            val totalItems = layoutInfo.totalItemsCount
            val visibleItems = layoutInfo.visibleItemsInfo

            if (totalItems == 0 || visibleItems.isEmpty()) {
              0f
            } else if (!listState.canScrollBackward) {
              0f
            } else if (!listState.canScrollForward) {
              1f
            } else {
              val firstIndex = listState.firstVisibleItemIndex
              val firstOffset = listState.firstVisibleItemScrollOffset
              val avgItemHeight = visibleItems.map { it.size }.average().coerceAtLeast(1.0)
              val viewportHeight = (layoutInfo.viewportEndOffset - layoutInfo.viewportStartOffset - layoutInfo.afterContentPadding - layoutInfo.beforeContentPadding).coerceAtLeast(1)
              val estimatedTotalHeight = (totalItems * avgItemHeight).coerceAtLeast(viewportHeight.toDouble())
              val maxScrollPx = (estimatedTotalHeight - viewportHeight).coerceAtLeast(1.0)
              val currentScrollPx = (firstIndex * avgItemHeight + firstOffset).coerceIn(0.0, maxScrollPx)
              (currentScrollPx / maxScrollPx).toFloat().coerceIn(0f, 1f)
            }
          }
          gridState != null -> {
            val layoutInfo = gridState.layoutInfo
            val totalItems = layoutInfo.totalItemsCount
            val visibleItems = layoutInfo.visibleItemsInfo

            if (totalItems == 0 || visibleItems.isEmpty()) {
              0f
            } else if (!gridState.canScrollBackward) {
              0f
            } else if (!gridState.canScrollForward) {
              1f
            } else {
              val columns = (visibleItems.groupBy { it.row }.values.firstOrNull()?.size ?: 1).coerceAtLeast(1)
              val totalRows = (totalItems + columns - 1) / columns
              val firstIndex = gridState.firstVisibleItemIndex
              val firstRow = firstIndex / columns
              val firstOffset = gridState.firstVisibleItemScrollOffset
              val avgRowHeight = visibleItems.map { it.size.height }.average().coerceAtLeast(1.0)
              val viewportHeight = (layoutInfo.viewportEndOffset - layoutInfo.viewportStartOffset - layoutInfo.afterContentPadding - layoutInfo.beforeContentPadding).coerceAtLeast(1)
              val estimatedTotalHeight = (totalRows * avgRowHeight).coerceAtLeast(viewportHeight.toDouble())
              val maxScrollPx = (estimatedTotalHeight - viewportHeight).coerceAtLeast(1.0)
              val currentScrollPx = (firstRow * avgRowHeight + firstOffset).coerceIn(0.0, maxScrollPx)
              (currentScrollPx / maxScrollPx).toFloat().coerceIn(0f, 1f)
            }
          }
          else -> 0f
        }
      }
    }

    suspend fun scrollToProgress(targetProgress: Float) {
      val progress = targetProgress.coerceIn(0f, 1f)
      if (listState != null) {
        val layoutInfo = listState.layoutInfo
        val totalItems = layoutInfo.totalItemsCount
        if (totalItems <= 0) return
        if (progress <= 0.001f) {
          listState.scrollToItem(0, 0)
        } else if (progress >= 0.999f) {
          listState.scrollToItem((totalItems - 1).coerceAtLeast(0), 0)
        } else {
          val visibleItems = layoutInfo.visibleItemsInfo
          val avgItemHeight = if (visibleItems.isNotEmpty()) {
            visibleItems.map { it.size }.average().coerceAtLeast(1.0)
          } else 100.0
          val targetExact = progress * (totalItems - 1)
          val itemIndex = targetExact.toInt().coerceIn(0, totalItems - 1)
          val fraction = targetExact - itemIndex
          val offsetPx = (fraction * avgItemHeight).toInt()
          listState.scrollToItem(itemIndex, offsetPx)
        }
      } else if (gridState != null) {
        val layoutInfo = gridState.layoutInfo
        val totalItems = layoutInfo.totalItemsCount
        if (totalItems <= 0) return
        if (progress <= 0.001f) {
          gridState.scrollToItem(0, 0)
        } else if (progress >= 0.999f) {
          gridState.scrollToItem((totalItems - 1).coerceAtLeast(0), 0)
        } else {
          val visibleItems = layoutInfo.visibleItemsInfo
          val columns = (visibleItems.groupBy { it.row }.values.firstOrNull()?.size ?: 1).coerceAtLeast(1)
          val totalRows = (totalItems + columns - 1) / columns
          val avgRowHeight = if (visibleItems.isNotEmpty()) {
            visibleItems.map { it.size.height }.average().coerceAtLeast(1.0)
          } else 100.0
          val targetRowExact = progress * (totalRows - 1)
          val targetRow = targetRowExact.toInt().coerceIn(0, totalRows - 1)
          val fraction = targetRowExact - targetRow
          val targetItemIndex = (targetRow * columns).coerceIn(0, totalItems - 1)
          val offsetPx = (fraction * avgRowHeight).toInt()
          gridState.scrollToItem(targetItemIndex, offsetPx)
        }
      }
    }

    val currentThumbOffsetY = if (isDragging) {
      dragOffsetY
    } else {
      scrollProgress * maxScrollableDistancePx
    }

    val currentThumbOffsetYState by androidx.compose.runtime.rememberUpdatedState(currentThumbOffsetY)
    val maxScrollableDistancePxState by androidx.compose.runtime.rememberUpdatedState(maxScrollableDistancePx)
    val effectiveThumbHeightPxState by androidx.compose.runtime.rememberUpdatedState(effectiveThumbHeightPx)
    val effectiveThumbHeightDp = with(density) { effectiveThumbHeightPx.toDp() }
    val touchMarginPx = with(density) { 16.dp.toPx() }

    Box(
      modifier = Modifier
        .fillMaxHeight()
        .width(touchTargetWidth)
        .align(Alignment.CenterEnd)
        .pointerInput(listState, gridState) {
          awaitEachGesture {
            val down = awaitFirstDown(requireUnconsumed = false)
            val thumbPos = currentThumbOffsetYState
            val thumbHeight = effectiveThumbHeightPxState
            val maxDist = maxScrollableDistancePxState
            val thumbTop = thumbPos - touchMarginPx
            val thumbBottom = thumbPos + thumbHeight + touchMarginPx

            // Only trigger fast scroll drag if the touch lands on or near the thumb indicator
            if (maxDist > 0f && down.position.y in thumbTop..thumbBottom) {
              down.consume()
              isDragging = true

              val touchOffsetInsideThumb = (down.position.y - thumbPos).coerceIn(0f, thumbHeight)
              var currentY = (down.position.y - touchOffsetInsideThumb).coerceIn(0f, maxDist)
              dragOffsetY = currentY
              var progress = currentY / maxDist
              coroutineScope.launch { scrollToProgress(progress) }

              while (true) {
                val event = awaitPointerEvent()
                val change = event.changes.firstOrNull { it.pressed } ?: break
                change.consume()
                currentY = (change.position.y - touchOffsetInsideThumb).coerceIn(0f, maxDist)
                dragOffsetY = currentY
                progress = currentY / maxDist
                coroutineScope.launch { scrollToProgress(progress) }
              }
              isDragging = false
            }
          }
        }
    ) {
      if (alpha > 0f && maxScrollableDistancePx > 0f) {
        Box(
          modifier = Modifier
            .offset { IntOffset(x = 0, y = currentThumbOffsetY.roundToInt()) }
            .align(Alignment.TopEnd)
            .padding(end = 4.dp)
            .size(width = currentThumbThickness, height = effectiveThumbHeightDp)
            .alpha(alpha)
            .clip(RoundedCornerShape(currentThumbThickness / 2))
            .background(if (isDragging) thumbColor else thumbColor.copy(alpha = 0.65f))
        )
      }
    }
  }
}
