package com.pollyanas.logistics.data

import android.content.Context
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKey
import android.util.Base64
import android.annotation.SuppressLint
import java.security.SecureRandom

class SecureSessionStore(context: Context) {
    private val masterKey = MasterKey.Builder(context)
        .setKeyScheme(MasterKey.KeyScheme.AES256_GCM)
        .build()
    private val preferences = EncryptedSharedPreferences.create(
        context,
        "corporate_session",
        masterKey,
        EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
        EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM,
    )

    var accessToken: String?
        get() = preferences.getString("access", null)
        set(value) = preferences.edit().putString("access", value).apply()
    var refreshToken: String?
        get() = preferences.getString("refresh", null)
        set(value) = preferences.edit().putString("refresh", value).apply()
    var activeRouteId: Long?
        get() = preferences.getLong("active_route", -1L).takeIf { it > 0 }
        set(value) = preferences.edit().putLong("active_route", value ?: -1L).apply()
    var sequenceNumber: Long
        get() = preferences.getLong("sequence", 0L)
        set(value) = preferences.edit().putLong("sequence", value).apply()

    fun storeTokens(access: String, refresh: String) {
        preferences.edit().putString("access", access).putString("refresh", refresh).apply()
    }

    fun clear() = preferences.edit()
        .remove("access")
        .remove("refresh")
        .remove("active_route")
        .remove("sequence")
        .apply()

    @SuppressLint("ApplySharedPref")
    fun databasePassphrase(): ByteArray {
        val stored = preferences.getString("database_key", null)
        if (stored != null) return Base64.decode(stored, Base64.NO_WRAP)
        val generated = ByteArray(32).also { SecureRandom().nextBytes(it) }
        preferences.edit().putString("database_key", Base64.encodeToString(generated, Base64.NO_WRAP)).commit()
        return generated
    }
}
