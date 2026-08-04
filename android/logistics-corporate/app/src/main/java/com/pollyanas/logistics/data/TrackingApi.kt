package com.pollyanas.logistics.data

import com.pollyanas.logistics.BuildConfig
import java.net.HttpURLConnection
import java.net.URL

data class ApiResult(val status: Int, val body: String) {
    val accepted: Boolean get() = status in 200..299
    val permanentRejection: Boolean get() = status in setOf(400, 403, 404, 409)
}

class TrackingApi(private val sessions: SecureSessionStore) {
    fun activeRouteMatches(routeId: Long): Boolean {
        var result = getActiveRoute(sessions.accessToken.orEmpty())
        if (result.status == 401 && refreshAccessToken()) {
            result = getActiveRoute(sessions.accessToken.orEmpty())
        }
        if (!result.accepted) return false
        val returnedId = Regex("\"ruta\"\\s*:\\s*\\{.*?\"id\"\\s*:\\s*(\\d+)", RegexOption.DOT_MATCHES_ALL)
            .find(result.body)?.groupValues?.get(1)?.toLongOrNull()
        val enRuta = Regex("\"estatus\"\\s*:\\s*\"EN_RUTA\"").containsMatchIn(result.body)
        return returnedId == routeId && enRuta
    }

    fun send(point: TrackingPoint): ApiResult {
        var result = request(point, sessions.accessToken.orEmpty())
        if (result.status == 401 && refreshAccessToken()) {
            result = request(point, sessions.accessToken.orEmpty())
        }
        return result
    }

    private fun request(point: TrackingPoint, token: String): ApiResult {
        if (token.isBlank()) return ApiResult(401, "missing_token")
        val connection = URL("${BuildConfig.ERP_ORIGIN}/api/logistica/rutas/${point.routeId}/tracking/")
            .openConnection() as HttpURLConnection
        return connection.useJson("POST", token, point.toJson())
    }

    private fun getActiveRoute(token: String): ApiResult {
        if (token.isBlank()) return ApiResult(401, "missing_token")
        val routeId = sessions.activeRouteId ?: return ApiResult(404, "missing_route")
        val connection = URL("${BuildConfig.ERP_ORIGIN}/api/logistica/rutas/$routeId/tracking/")
            .openConnection() as HttpURLConnection
        return connection.useRequest("GET", token, null)
    }

    private fun refreshAccessToken(): Boolean {
        val refresh = sessions.refreshToken ?: return false
        val connection = URL("${BuildConfig.ERP_ORIGIN}/api/logistica/auth/token/refresh/")
            .openConnection() as HttpURLConnection
        val result = connection.useRequest("POST", null, "{\"refresh\":${refresh.jsonQuote()}}")
        if (!result.accepted) return false
        val access = Regex("\"access\"\\s*:\\s*\"([^\"]+)\"").find(result.body)?.groupValues?.get(1) ?: return false
        sessions.accessToken = access
        return true
    }
}

private fun HttpURLConnection.useJson(methodName: String, token: String?, payload: String): ApiResult =
    useRequest(methodName, token, payload)

private fun HttpURLConnection.useRequest(methodName: String, token: String?, payload: String?): ApiResult = try {
    requestMethod = methodName
    connectTimeout = 15_000
    readTimeout = 15_000
    doOutput = payload != null
    setRequestProperty("Content-Type", "application/json")
    setRequestProperty("Accept", "application/json")
    if (!token.isNullOrBlank()) setRequestProperty("Authorization", "Bearer $token")
    if (payload != null) outputStream.bufferedWriter(Charsets.UTF_8).use { it.write(payload) }
    val status = responseCode
    val stream = if (status in 200..299) inputStream else errorStream
    ApiResult(status, stream?.bufferedReader()?.use { it.readText() }.orEmpty())
} finally {
    disconnect()
}

private fun TrackingPoint.toJson() = """{
  "latitud":$latitude,
  "longitud":$longitude,
  "precision_metros":${accuracyMeters ?: "null"},
  "velocidad_kmh":${speedKmh ?: "null"},
  "bateria_porcentaje":${batteryPercent ?: "null"},
  "timestamp_dispositivo":${capturedAt.jsonQuote()},
  "client_event_id":${eventId.jsonQuote()},
  "tracking_origen":"automatico_pwa",
  "fuera_de_ruta_confirmado":false,
  "desvio_motivo":""
}""".trimIndent()

private fun String.jsonQuote(): String = buildString {
    append('"')
    this@jsonQuote.forEach { c ->
        when (c) {
            '\\' -> append("\\\\")
            '"' -> append("\\\"")
            '\n' -> append("\\n")
            '\r' -> append("\\r")
            '\t' -> append("\\t")
            else -> append(c)
        }
    }
    append('"')
}
