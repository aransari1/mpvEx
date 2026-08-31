package xyz.mpv.rex.database.entities

import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey

@Entity(
  tableName = "hybrid_media_index",
  indices = [
    Index("parentIdentity"),
    Index("sourceRoot"),
    Index("available"),
  ],
)
data class HybridMediaEntity(
  @PrimaryKey val identity: String,
  val sourceType: String,
  val sourceRoot: String,
  val location: String,
  val parentIdentity: String,
  val parentDisplayName: String,
  val displayName: String,
  val mimeType: String,
  val size: Long,
  val dateModified: Long,
  val isAudio: Boolean,
  val isNoMedia: Boolean,
  val duration: Long = 0,
  val width: Int = 0,
  val height: Int = 0,
  val rotation: Int = 0,
  val metadataState: String = "PENDING",
  val available: Boolean = true,
  val lastSeenGeneration: Long,
)

@Entity(tableName = "hybrid_media_roots")
data class HybridMediaRootEntity(
  @PrimaryKey val identity: String,
  val sourceType: String,
  val location: String,
  val displayName: String,
  val available: Boolean = true,
  val lastGeneration: Long = 0,
  val lastCompletedAt: Long = 0,
  val lastError: String? = null,
)
