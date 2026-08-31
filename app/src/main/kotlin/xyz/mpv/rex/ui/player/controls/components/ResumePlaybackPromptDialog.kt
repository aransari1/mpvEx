package xyz.mpv.rex.ui.player.controls.components

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import `is`.xyz.mpv.Utils
import xyz.mpv.rex.R
import xyz.mpv.rex.ui.player.ResumePromptData

@OptIn(ExperimentalLayoutApi::class)
@Composable
fun ResumePlaybackPromptDialog(
  promptData: ResumePromptData,
  onConfirmResume: (Int) -> Unit,
  onRestart: () -> Unit,
  onDismiss: () -> Unit,
  modifier: Modifier = Modifier,
) {
  AlertDialog(
    onDismissRequest = onDismiss,
    title = {
      Text(
        text = stringResource(R.string.player_resume_prompt_title),
        style = MaterialTheme.typography.headlineSmall,
        fontWeight = FontWeight.Bold,
      )
    },
    text = {
      Column(
        modifier = Modifier.fillMaxWidth(),
        verticalArrangement = Arrangement.spacedBy(8.dp),
      ) {
        val prettyPos = Utils.prettyTime(promptData.position)
        val message = if (promptData.duration > 0) {
          stringResource(
            R.string.player_resume_prompt_message_with_duration,
            prettyPos,
            Utils.prettyTime(promptData.duration),
          )
        } else {
          stringResource(R.string.player_resume_prompt_message, prettyPos)
        }
        Text(
          text = message,
          style = MaterialTheme.typography.bodyLarge,
          color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
      }
    },
    confirmButton = {
      FlowRow(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.spacedBy(8.dp, Alignment.End),
        verticalArrangement = Arrangement.spacedBy(8.dp),
      ) {
        OutlinedButton(
          onClick = onRestart,
          shape = RoundedCornerShape(12.dp),
        ) {
          Text(stringResource(R.string.player_resume_action_start_over))
        }
        Button(
          onClick = { onConfirmResume(promptData.position) },
          shape = RoundedCornerShape(12.dp),
        ) {
          Text(stringResource(R.string.player_resume_action_resume))
        }
      }
    },
    dismissButton = null,
    modifier = modifier,
  )
}
