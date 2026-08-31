package xyz.mpv.rex.database.dao

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import xyz.mpv.rex.database.entities.HybridMediaEntity
import xyz.mpv.rex.database.entities.HybridMediaRootEntity

@Dao
interface HybridMediaDao {
  @Insert(onConflict = OnConflictStrategy.REPLACE)
  suspend fun upsertMedia(items: List<HybridMediaEntity>)

  @Insert(onConflict = OnConflictStrategy.REPLACE)
  suspend fun upsertRoot(root: HybridMediaRootEntity)

  @Query(
    """
    SELECT * FROM hybrid_media_index
    WHERE available = 1
      AND (:includeNoMedia OR isNoMedia = 0)
    ORDER BY displayName COLLATE NOCASE
    """,
  )
  suspend fun getAvailableMedia(includeNoMedia: Boolean): List<HybridMediaEntity>

  @Query("SELECT COUNT(*) FROM hybrid_media_index WHERE available = 1")
  suspend fun getAvailableCount(): Int

  @Query("SELECT COUNT(*) FROM hybrid_media_index WHERE available = 1 AND isNoMedia = 1")
  suspend fun getNoMediaCount(): Int

  @Query(
    """
    SELECT * FROM hybrid_media_index
    WHERE available = 1
      AND (:includeNoMedia OR isNoMedia = 0)
      AND (displayName LIKE '%' || :query || '%' OR parentDisplayName LIKE '%' || :query || '%')
    ORDER BY displayName COLLATE NOCASE
    """,
  )
  suspend fun searchMedia(query: String, includeNoMedia: Boolean): List<HybridMediaEntity>

  @Query(
    """
    SELECT * FROM hybrid_media_index
    WHERE available = 1
      AND (:includeNoMedia OR isNoMedia = 0)
      AND (location = :parentPath OR location LIKE :parentPathPrefix OR parentIdentity = :parentPath OR parentIdentity LIKE :parentPathPrefix)
      AND (displayName LIKE '%' || :query || '%' OR parentDisplayName LIKE '%' || :query || '%')
    ORDER BY displayName COLLATE NOCASE
    """,
  )
  suspend fun searchMediaInPath(query: String, parentPath: String, parentPathPrefix: String, includeNoMedia: Boolean): List<HybridMediaEntity>

  @Query("SELECT * FROM hybrid_media_roots")
  suspend fun getRoots(): List<HybridMediaRootEntity>

  @Query("SELECT * FROM hybrid_media_roots WHERE identity = :identity LIMIT 1")
  suspend fun getRoot(identity: String): HybridMediaRootEntity?

  @Query(
    """
    DELETE FROM hybrid_media_index
    WHERE sourceRoot = :sourceRoot
      AND lastSeenGeneration != :generation
    """,
  )
  suspend fun deleteStaleForCompletedGeneration(sourceRoot: String, generation: Long)

  @Query("UPDATE hybrid_media_index SET available = 0 WHERE sourceRoot = :sourceRoot")
  suspend fun markMediaUnavailable(sourceRoot: String)

  @Query(
    """
    UPDATE hybrid_media_roots
    SET available = 0, lastError = :error
    WHERE identity = :identity
    """,
  )
  suspend fun markRootUnavailable(identity: String, error: String?)

  @Query(
    """
    UPDATE hybrid_media_roots
    SET available = 1,
        lastGeneration = :generation,
        lastCompletedAt = :completedAt,
        lastError = NULL
    WHERE identity = :identity
    """,
  )
  suspend fun markRootComplete(identity: String, generation: Long, completedAt: Long)

  @Query(
    """
    SELECT * FROM hybrid_media_index
    WHERE metadataState = 'PENDING' AND available = 1
    LIMIT :limit
    """,
  )
  suspend fun getPendingMetadataItems(limit: Int = 50): List<HybridMediaEntity>

  @Query(
    """
    UPDATE hybrid_media_index
    SET duration = :duration,
        width = :width,
        height = :height,
        rotation = :rotation,
        metadataState = :metadataState
    WHERE identity = :identity
    """,
  )
  suspend fun updateMediaMetadata(
    identity: String,
    duration: Long,
    width: Int,
    height: Int,
    rotation: Int,
    metadataState: String,
  )

  @Query("DELETE FROM hybrid_media_index")
  suspend fun clearMedia()

  @Query("DELETE FROM hybrid_media_roots")
  suspend fun clearRoots()

  @Query("DELETE FROM hybrid_media_index WHERE location IN (:paths) OR identity IN (:paths)")
  suspend fun deleteByLocations(paths: List<String>)

  @Query(
    """
    DELETE FROM hybrid_media_index
    WHERE location = :path
       OR location LIKE :prefixPattern ESCAPE '\'
       OR parentIdentity = :path
       OR parentIdentity LIKE :prefixPattern ESCAPE '\'
    """,
  )
  suspend fun deleteByPathPrefix(path: String, prefixPattern: String)
}

