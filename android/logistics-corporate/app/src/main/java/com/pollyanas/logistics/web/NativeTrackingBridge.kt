package com.pollyanas.logistics.web

import android.Manifest
import android.app.Activity
import android.content.Intent
import android.content.pm.PackageManager
import android.webkit.JavascriptInterface
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import com.pollyanas.logistics.data.SecureSessionStore
import com.pollyanas.logistics.tracking.RouteTrackingService

class NativeTrackingBridge(private val activity: Activity) {
    private val sessions = SecureSessionStore(activity)

    @JavascriptInterface
    fun storeTokens(access: String, refresh: String) {
        if (access.length !in 20..8192 || refresh.length > 8192) return
        sessions.storeTokens(access, refresh)
    }

    @JavascriptInterface
    fun startRouteTracking(routeIdValue: String) {
        val routeId = routeIdValue.toLongOrNull()?.takeIf { it > 0 } ?: return
        activity.runOnUiThread {
            sessions.activeRouteId = routeId
            if (!hasForegroundLocation()) {
                ActivityCompat.requestPermissions(
                    activity,
                    arrayOf(Manifest.permission.ACCESS_FINE_LOCATION, Manifest.permission.ACCESS_COARSE_LOCATION),
                    REQUEST_FOREGROUND_LOCATION,
                )
                return@runOnUiThread
            }
            if (android.os.Build.VERSION.SDK_INT >= 33
                && ContextCompat.checkSelfPermission(activity, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
                ActivityCompat.requestPermissions(
                    activity, arrayOf(Manifest.permission.POST_NOTIFICATIONS), REQUEST_NOTIFICATIONS
                )
            }
            ContextCompat.startForegroundService(
                activity,
                Intent(activity, RouteTrackingService::class.java)
                    .setAction(RouteTrackingService.ACTION_START)
                    .putExtra(RouteTrackingService.EXTRA_ROUTE_ID, routeId),
            )
        }
    }

    @JavascriptInterface
    fun stopRouteTracking() {
        activity.runOnUiThread {
            activity.startService(
                Intent(activity, RouteTrackingService::class.java).setAction(RouteTrackingService.ACTION_STOP)
            )
        }
    }

    @JavascriptInterface
    fun clearSession() {
        stopRouteTracking()
        sessions.clear()
    }

    private fun hasForegroundLocation() = ContextCompat.checkSelfPermission(
        activity, Manifest.permission.ACCESS_FINE_LOCATION
    ) == PackageManager.PERMISSION_GRANTED

    companion object {
        const val REQUEST_FOREGROUND_LOCATION = 450
        const val REQUEST_BACKGROUND_LOCATION = 451
        const val REQUEST_NOTIFICATIONS = 452
    }
}
