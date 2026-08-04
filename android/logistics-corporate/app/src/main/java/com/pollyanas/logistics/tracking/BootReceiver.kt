package com.pollyanas.logistics.tracking

import android.Manifest
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import androidx.core.content.ContextCompat
import com.pollyanas.logistics.data.SecureSessionStore

class BootReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action !in ALLOWED_ACTIONS) return
        val sessions = SecureSessionStore(context)
        val routeId = sessions.activeRouteId ?: return
        val backgroundGranted = ContextCompat.checkSelfPermission(
            context, Manifest.permission.ACCESS_BACKGROUND_LOCATION
        ) == PackageManager.PERMISSION_GRANTED
        if (!backgroundGranted || sessions.accessToken.isNullOrBlank()) return
        ContextCompat.startForegroundService(
            context,
            Intent(context, RouteTrackingService::class.java)
                .setAction(RouteTrackingService.ACTION_START)
                .putExtra(RouteTrackingService.EXTRA_ROUTE_ID, routeId),
        )
    }

    companion object {
        private val ALLOWED_ACTIONS = setOf(
            Intent.ACTION_BOOT_COMPLETED,
            Intent.ACTION_MY_PACKAGE_REPLACED,
        )
    }
}
