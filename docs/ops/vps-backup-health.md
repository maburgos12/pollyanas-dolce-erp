# Revisión nativa de respaldos y disco

`erp-backup-health.timer` ejecuta diariamente a las 08:00 America/Mazatlan
`scripts/vps_backup_health.py` en el VPS. No depende del Mac ni de Codex.
El cron existente sigue creando respaldos a las 02:00 UTC; su rotación conserva
puntos sin recibo. El servicio comparte su bloqueo y genera los recibos faltantes.

La conexión guardada `qnap-nasdolce-archive` usa la llave restringida del VPS,
la cuenta no administrativa DolcePasteleria y WireGuard. No se cambian claves.
El NAS conserva un runtime PostgreSQL 16 ARM64 y el verificador en
`/share/homes/DolcePasteleria/.erp-backup-verifier`, privado, dueño DolcePasteleria.
Provisionar el runtime desde la imagen local comprobada postgres:16-alpine
`sha256:4327b9fd295502f326f44153a1045a7170ddbfffed1c3829798328556cfd09e2`:
exportar `/usr/local/bin`, `/usr/local/lib`, `/usr/local/share/postgresql`, `/lib`, `/usr/lib` y
`/usr/share/icu`; comparar SHA-256 del archivo recibido antes de extraerlo allí
en `runtime`. No descargar ejecutables de una fuente no verificada.

Después de merge y despliegue Git, ejecutar en VPS
`bash /opt/pastelerias-erp/scripts/ops/install_backup_health.sh`.
Validar una ejecución con `systemctl start erp-backup-health.service`, revisar
`/var/lib/erp-backup-restore/health-latest.json` y después activar
`systemctl enable --now erp-backup-health.timer`.
Consultar `systemctl list-timers erp-backup-health.timer` y
`journalctl -u erp-backup-health.service` para estado/errores. Los informes
privados incluyen disco, Docker, memoria, puntos, respaldo conservado y bytes
netos liberados; 75% es ALERTA y 85% URGENTE. Un fallo retorna código distinto
de cero y deja el servicio fallido. No se configura envío de mensajes externos.

Cada recibo nuevo exige restauración SQL íntegra con errores fatales, índices
válidos, migraciones, recuperación y hash de todos los medios y archivos de
evidencia, y referencias de documentos de logística. La prueba no ejecuta
transacciones de negocio ni certifica todas las referencias de cada módulo.
El runtime usa locale C, socket privado y ningún puerto TCP. Los recibos
anteriores solo se reutilizan para el mismo manifiesto y con comprobación
actual de todos los bytes del NAS por el rotador existente. No se altera este
rotador, que conserva el último punto local. También se bloquea la ejecución
ante puntos incompletos o si el último respaldo tiene más de 48 horas.

Cada restauración tiene bloqueo NAS, 80 GiB mínimos libres y límite de seis
horas. Se eliminan únicamente los temporales propios tras detener PostgreSQL;
se conservan logs y recibos en `evidence/`. Un scratch residual tras corte duro
bloquea la siguiente restauración: inspeccionar propietario y procesos antes
de retirarlo. No borrar por edad. El límite total del servicio es ocho horas.
Los respaldos originales se conservan ante cualquier verificación fallida.

Esta tarea no limpia volúmenes, contenedores, respaldos manuales ni imágenes
de rollback. Después de verificar los respaldos, si continúa en 75% o más,
ejecuta el mantenimiento Docker conservador existente (dangling/caché sin uso
de más de siete días), únicamente si conserva la huella de la versión revisada.
Para reversión: `systemctl disable --now erp-backup-health.timer`; dejar terminar
la ejecución activa o detener el servicio y revisar scratch NAS. El cron de
respaldo y todos los datos operativos permanecen intactos. Pausar la antigua
automatización Codex después de validar la sustitución para evitar duplicidad.
