package com.pollyanas.logistics

import android.app.Application
import com.pollyanas.logistics.data.AppDatabase

class PollyanasApplication : Application() {
    val database by lazy { AppDatabase.create(this) }
}
