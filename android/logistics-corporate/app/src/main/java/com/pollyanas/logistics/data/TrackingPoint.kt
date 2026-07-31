package com.pollyanas.logistics.data

import androidx.room.Dao
import androidx.room.Database
import androidx.room.Entity
import androidx.room.Insert
import androidx.room.PrimaryKey
import androidx.room.Query
import androidx.room.Room
import androidx.room.RoomDatabase
import android.content.Context
import net.zetetic.database.sqlcipher.SupportOpenHelperFactory

@Entity(tableName = "tracking_points")
data class TrackingPoint(
    @PrimaryKey val eventId: String,
    val routeId: Long,
    val sequenceNumber: Long,
    val latitude: Double,
    val longitude: Double,
    val accuracyMeters: Float?,
    val speedKmh: Float?,
    val batteryPercent: Int?,
    val capturedAt: String,
    val queuedAt: Long,
    val attempts: Int = 0,
    val state: String = "PENDING",
    val rejectionReason: String? = null,
)

@Dao
interface TrackingPointDao {
    @Insert suspend fun insert(point: TrackingPoint)

    @Query("SELECT * FROM tracking_points WHERE state = 'PENDING' ORDER BY queuedAt, sequenceNumber LIMIT 1")
    suspend fun oldestPending(): TrackingPoint?

    @Query("DELETE FROM tracking_points WHERE eventId = :eventId")
    suspend fun acknowledge(eventId: String)

    @Query("UPDATE tracking_points SET attempts = attempts + 1 WHERE eventId = :eventId")
    suspend fun recordRetry(eventId: String)

    @Query("UPDATE tracking_points SET state = 'REJECTED', rejectionReason = :reason WHERE eventId = :eventId")
    suspend fun quarantine(eventId: String, reason: String)

    @Query("SELECT COUNT(*) FROM tracking_points WHERE state = 'PENDING'")
    suspend fun pendingCount(): Int
}

@Database(entities = [TrackingPoint::class], version = 1, exportSchema = false)
abstract class AppDatabase : RoomDatabase() {
    abstract fun trackingPoints(): TrackingPointDao

    companion object {
        fun create(context: Context): AppDatabase {
            System.loadLibrary("sqlcipher")
            val factory = SupportOpenHelperFactory(SecureSessionStore(context).databasePassphrase())
            return Room.databaseBuilder(
                context.applicationContext,
                AppDatabase::class.java,
                "pollyanas_tracking.db",
            ).openHelperFactory(factory).build()
        }
    }
}
