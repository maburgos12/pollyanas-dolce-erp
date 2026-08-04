# Pollyana's Operativa — Android corporativo

Contenedor Android privado para los teléfonos de Logística. Reutiliza la PWA del ERP y traslada el seguimiento de ruta a un `foreground service` nativo.

## Contrato operativo

- Solo inicia desde la PWA cuando la ruta vigente está `EN_RUTA`.
- Captura con Fused Location Provider cada 45 segundos.
- Persiste primero en Room y transmite después, una fila a la vez y en orden.
- Conserva rechazos permanentes en cuarentena local; no los presenta como enviados.
- Mantiene notificación permanente y se detiene al finalizar/cancelar la ruta o cerrar sesión.
- Después de reiniciar solo intenta recuperar una ruta previamente activa si existen token y permiso de ubicación en segundo plano.

## Compilación

Requisitos: JDK 17, Android SDK 35 y Gradle 8.9 o Android Studio reciente.

```bash
./gradlew --no-daemon :app:testDebugUnitTest :app:assembleDebug
```

La firma release debe vivir fuera del repositorio. No guardar keystore, contraseñas, APK/AAB ni identificadores de los dispositivos. Para generar un release firmado, proporcionar las credenciales únicamente mediante el entorno:

```bash
export POLLYANAS_KEYSTORE_PATH=/ruta/segura/logistics-release.p12
export POLLYANAS_KEYSTORE_PASSWORD='...'
export POLLYANAS_KEY_ALIAS=logistics-release
export POLLYANAS_KEY_PASSWORD='...'
./gradlew --no-daemon :app:bundleRelease :app:assembleRelease
```

En los equipos corporativos, recuperar las contraseñas desde el llavero o gestor de secretos en el momento de compilar; no escribirlas en `gradle.properties`, scripts ni historial de shell.

## Publicación privada

1. Generar AAB release firmado con la clave corporativa estable.
2. Publicarlo como app privada en Managed Google Play desde ManageEngine.
3. Asignarlo al grupo de Logística con instalación obligatoria.
4. Conceder ubicación precisa, ubicación en segundo plano y notificaciones.
5. Validar el estado aplicado y el comportamiento físico en Galaxy A26 / Android 15.
