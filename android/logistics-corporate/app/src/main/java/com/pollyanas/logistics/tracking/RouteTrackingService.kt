package com.pollyanas.logistics.tracking

import android.Manifest
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Intent
import android.content.pm.PackageManager
import android.location.Location
import android.os.BatteryManager
import android.os.IBinder
import androidx.core.app.ActivityCompat
import androidx.core.app.NotificationCompat
import androidx.core.app.ServiceCompat
import androidx.core.content.getSystemService
import androidx.lifecycle.LifecycleService
import androidx.lifecycle.lifecycleScope
import com.google.android.gms.location.FusedLocationProviderClient
import com.google.android.gms.location.LocationCallback
import com.google.android.gms.location.LocationRequest
import com.google.android.gms.location.LocationResult
import com.google.android.gms.location.LocationServices
import com.google.android.gms.location.Priority
import com.pollyanas.logistics.MainActivity
import com.pollyanas.logistics.PollyanasApplication
import com.pollyanas.logistics.R
import com.pollyanas.logistics.data.SecureSessionStore
import com.pollyanas.logistics.data.TrackingApi
import com.pollyanas.logistics.data.TrackingPoint
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.time.Instant
import java.util.UUID

class RouteTrackingService : LifecycleService() {
    private lateinit var fusedLocation: FusedLocationProviderClient
    private lateinit var sessions: SecureSessionStore
    private val dao by lazy { (application as PollyanasApplication).database.trackingPoints() }
    private var sending = false

    private val callback = object : LocationCallback() {
        override fun onLocationResult(result: LocationResult) {
            result.lastLocation?.let(::persistAndFlush)
        }
    }

    override fun onCreate() {
        super.onCreate()
        sessions = SecureSessionStore(this)
        fusedLocation = LocationServices.getFusedLocationProviderClient(this)
        createNotificationChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        super.onStartCommand(intent, flags, startId)
        if (intent?.action == ACTION_STOP) {
            stopTracking(clearRoute = true)
            return Service.START_NOT_STICKY
        }
        val routeId = intent?.getLongExtra(EXTRA_ROUTE_ID, -1L)?.takeIf { it > 0 } ?: sessions.activeRouteId
        if (routeId == null || sessions.accessToken.isNullOrBlank()) {
            stopTracking(clearRoute = false)
            return Service.START_NOT_STICKY
        }
        sessions.activeRouteId = routeId
        ServiceCompat.startForeground(
            this,
            NOTIFICATION_ID,
            notification("Seguimiento activo · ruta $routeId"),
            android.content.pm.ServiceInfo.FOREGROUND_SERVICE_TYPE_LOCATION,
        )
        lifecycleScope.launch {
            val routeStillActive = withContext(Dispatchers.IO) {
                TrackingApi(sessions).activeRouteMatches(routeId)
            }
            if (!routeStillActive) {
                stopTracking(clearRoute = true)
                return@launch
            }
            startLocationUpdates()
            flushQueue()
        }
        return Service.START_STICKY
    }

    private fun startLocationUpdates() {
        if (ActivityCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED) {
            stopTracking(clearRoute = false)
            return
        }
        val request = LocationRequest.Builder(Priority.PRIORITY_HIGH_ACCURACY, INTERVAL_MS)
            .setMinUpdateIntervalMillis(INTERVAL_MS)
            .setMaxUpdateDelayMillis(INTERVAL_MS)
            .build()
        fusedLocation.removeLocationUpdates(callback)
        fusedLocation.requestLocationUpdates(request, callback, mainLooper)
    }

    private fun persistAndFlush(location: Location) {
        val routeId = sessions.activeRouteId ?: return
        val nextSequence = sessions.sequenceNumber + 1
        sessions.sequenceNumber = nextSequence
        val battery = getSystemService<BatteryManager>()
            ?.getIntProperty(BatteryManager.BATTERY_PROPERTY_CAPACITY)
            ?.takeIf { it in 0..100 }
        val point = TrackingPoint(
            eventId = UUID.randomUUID().toString(),
            routeId = routeId,
            sequenceNumber = nextSequence,
            latitude = location.latitude,
            longitude = location.longitude,
            accuracyMeters = location.accuracy.takeIf { location.hasAccuracy() },
            speedKmh = (location.speed * 3.6f).takeIf { location.hasSpeed() },
            batteryPercent = battery,
            capturedAt = Instant.ofEpochMilli(location.time).toString(),
            queuedAt = System.currentTimeMillis(),
        )
        lifecycleScope.launch {
            dao.insert(point)
            flushQueue()
        }
    }

    private suspend fun flushQueue() {
        if (sending) return
        sending = true
        try {
            while (true) {
                val point = dao.oldestPending() ?: break
                val result = withContext(Dispatchers.IO) { TrackingApi(sessions).send(point) }
                when {
                    result.accepted -> dao.acknowledge(point.eventId)
                    result.status == 401 || result.status == 403 -> {
                        dao.recordRetry(point.eventId)
                        stopTracking(clearRoute = false)
                        break
                    }
                    result.permanentRejection -> {
                        dao.quarantine(point.eventId, "HTTP_${result.status}")
                        if (point.routeId == sessions.activeRouteId && (
                                result.body.contains("estatus En ruta", ignoreCase = true)
                                    || result.body.contains("ruta activa", ignoreCase = true)
                            )) {
                            stopTracking(clearRoute = true)
                            break
                        }
                    }
                    else -> {
                        dao.recordRetry(point.eventId)
                        break
                    }
                }
            }
            updateNotification()
        } finally {
            sending = false
        }
    }

    private suspend fun updateNotification() {
        val pending = dao.pendingCount()
        getSystemService<NotificationManager>()?.notify(
            NOTIFICATION_ID,
            notification(if (pending == 0) "Ubicación enviada" else "$pending ubicaciones pendientes"),
        )
    }

    private fun stopTracking(clearRoute: Boolean) {
        fusedLocation.removeLocationUpdates(callback)
        if (clearRoute) {
            sessions.activeRouteId = null
            sessions.sequenceNumber = 0
        }
        stopForeground(STOP_FOREGROUND_REMOVE)
        stopSelf()
    }

    private fun createNotificationChannel() {
        getSystemService<NotificationManager>()?.createNotificationChannel(
            NotificationChannel(CHANNEL_ID, "Seguimiento de ruta", NotificationManager.IMPORTANCE_LOW).apply {
                description = "Notificación permanente mientras una ruta está activa"
                setShowBadge(false)
            }
        )
    }

    private fun notification(text: String) = NotificationCompat.Builder(this, CHANNEL_ID)
        .setSmallIcon(android.R.drawable.ic_menu_mylocation)
        .setContentTitle("Pollyana's · Ruta en seguimiento")
        .setContentText(text)
        .setOngoing(true)
        .setOnlyAlertOnce(true)
        .setContentIntent(
            PendingIntent.getActivity(
                this, 0, Intent(this, MainActivity::class.java),
                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
            )
        )
        .build()

    override fun onDestroy() {
        fusedLocation.removeLocationUpdates(callback)
        super.onDestroy()
    }

    companion object {
        const val ACTION_START = "com.pollyanas.logistics.START_ROUTE_TRACKING"
        const val ACTION_STOP = "com.pollyanas.logistics.STOP_ROUTE_TRACKING"
        const val EXTRA_ROUTE_ID = "route_id"
        private const val CHANNEL_ID = "route_tracking"
        private const val NOTIFICATION_ID = 4501
        private const val INTERVAL_MS = 45_000L
    }
}
