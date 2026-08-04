package com.pollyanas.logistics

import android.Manifest
import android.annotation.SuppressLint
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.provider.Settings
import android.webkit.CookieManager
import android.webkit.GeolocationPermissions
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import com.pollyanas.logistics.web.NativeTrackingBridge
import com.pollyanas.logistics.data.SecureSessionStore
import com.pollyanas.logistics.tracking.RouteTrackingService

class MainActivity : AppCompatActivity() {
    private lateinit var webView: WebView
    private var pendingGeolocationOrigin: String? = null
    private var pendingGeolocationCallback: GeolocationPermissions.Callback? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        configureWebView()
        setContentView(webView)
        if (savedInstanceState == null) webView.loadUrl("${BuildConfig.ERP_ORIGIN}/logistica/app/")
        else webView.restoreState(savedInstanceState)
    }

    @SuppressLint("SetJavaScriptEnabled", "AddJavascriptInterface")
    private fun configureWebView() {
        WebView.setWebContentsDebuggingEnabled(BuildConfig.DEBUG)
        webView = WebView(this).apply {
            settings.javaScriptEnabled = true
            settings.domStorageEnabled = true
            settings.allowFileAccess = false
            settings.allowContentAccess = false
            settings.setSupportMultipleWindows(false)
            settings.userAgentString = "${settings.userAgentString} PollyanasCorporate/1.0"
            CookieManager.getInstance().setAcceptCookie(true)
            addJavascriptInterface(NativeTrackingBridge(this@MainActivity), "PollyanasNative")
            webChromeClient = object : WebChromeClient() {
                override fun onGeolocationPermissionsShowPrompt(
                    origin: String,
                    callback: GeolocationPermissions.Callback,
                ) {
                    val trustedOrigin = origin.let {
                        val uri = Uri.parse(it)
                        uri.scheme == "https" && uri.host == Uri.parse(BuildConfig.ERP_ORIGIN).host
                    }
                    if (!trustedOrigin) {
                        callback.invoke(origin, false, false)
                        return
                    }
                    if (hasForegroundLocationPermission()) {
                        callback.invoke(origin, true, false)
                        return
                    }
                    pendingGeolocationOrigin = origin
                    pendingGeolocationCallback = callback
                    ActivityCompat.requestPermissions(
                        this@MainActivity,
                        arrayOf(
                            Manifest.permission.ACCESS_FINE_LOCATION,
                            Manifest.permission.ACCESS_COARSE_LOCATION,
                        ),
                        REQUEST_WEB_GEOLOCATION,
                    )
                }
            }
            webViewClient = object : WebViewClient() {
                override fun shouldOverrideUrlLoading(view: WebView, request: WebResourceRequest): Boolean {
                    val uri = request.url
                    val allowed = uri.scheme == "https" && uri.host == Uri.parse(BuildConfig.ERP_ORIGIN).host
                    if (allowed) return false
                    return true
                }
            }
        }
    }

    override fun onRequestPermissionsResult(requestCode: Int, permissions: Array<out String>, grantResults: IntArray) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults)
        if (requestCode == REQUEST_WEB_GEOLOCATION) {
            val granted = grantResults.any { it == PackageManager.PERMISSION_GRANTED }
            pendingGeolocationCallback?.invoke(pendingGeolocationOrigin, granted, false)
            pendingGeolocationOrigin = null
            pendingGeolocationCallback = null
            return
        }
        if (requestCode == NativeTrackingBridge.REQUEST_FOREGROUND_LOCATION
            && grantResults.any { it == PackageManager.PERMISSION_GRANTED }) {
            SecureSessionStore(this).activeRouteId?.let { routeId ->
                ContextCompat.startForegroundService(
                    this,
                    Intent(this, RouteTrackingService::class.java)
                        .setAction(RouteTrackingService.ACTION_START)
                        .putExtra(RouteTrackingService.EXTRA_ROUTE_ID, routeId),
                )
            }
            explainBackgroundLocation()
        }
    }

    private fun explainBackgroundLocation() {
        if (Build.VERSION.SDK_INT < 30 || ContextCompat.checkSelfPermission(
                this, Manifest.permission.ACCESS_BACKGROUND_LOCATION
            ) == PackageManager.PERMISSION_GRANTED) return
        AlertDialog.Builder(this)
            .setTitle("Ubicación durante la ruta")
            .setMessage("Para continuar con la pantalla bloqueada y después de reiniciar, selecciona Permitir todo el tiempo en Ubicación.")
            .setPositiveButton("Abrir permisos") { _, _ ->
                startActivity(Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.parse("package:$packageName")))
            }
            .setNegativeButton("Ahora no", null)
            .show()
    }

    private fun hasForegroundLocationPermission() = ContextCompat.checkSelfPermission(
        this,
        Manifest.permission.ACCESS_FINE_LOCATION,
    ) == PackageManager.PERMISSION_GRANTED || ContextCompat.checkSelfPermission(
        this,
        Manifest.permission.ACCESS_COARSE_LOCATION,
    ) == PackageManager.PERMISSION_GRANTED

    override fun onSaveInstanceState(outState: Bundle) {
        webView.saveState(outState)
        super.onSaveInstanceState(outState)
    }

    override fun onBackPressed() {
        if (webView.canGoBack()) webView.goBack() else super.onBackPressed()
    }

    companion object {
        private const val REQUEST_WEB_GEOLOCATION = 453
    }
}
