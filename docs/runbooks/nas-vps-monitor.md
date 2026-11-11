# Vigilancia externa del VPS desde el NAS

**Significa:** el NAS no confirmó TCP público `68.183.165.47:443` o HTTPS del ERP
durante tres comprobaciones consecutivas. No prueba por sí solo que el servidor
esté apagado: revisar red, DNS, TLS y aplicación. Sondeo cada minuto; recuperación
tras dos éxitos; recordatorio cada seis horas. Telegram sale directamente por el
Internet del NAS, sin Mac, Codex ni túnel a través del VPS.

**Primero:** comprobar ERP desde otra conexión y el panel del proveedor del VPS.
En el NAS leer `/share/CACHEDEV1_DATA/.erp-vps-monitor/state/latest.json`:
`checked_at`, `entry_point=cron`, `checks`, `error` y `message_id`. No reiniciar
servicios ni borrar datos sin diagnosticar. Escalar a Mauricio si persiste.

**Instalación:** exportar estos scripts desde una revisión Git revisada al NAS,
añadir `scripts/telegram.json` privado0600 con el token y chat_id existentes,
y ejecutar con sudo `scripts/ops/install_nas_vps_monitor.sh`. El instalador crea
una carpeta nueva protegida por administrador y ejecuta el monitor como
DolcePasteleria(uid1000), sin concederle permisos nuevos. Guarda el cron previo,
conserva las entradas existentes y añade una sola entrada en `/etc/config/crontab`.
Recarga siguiendo el [procedimiento QNAP](https://www.qnap.com/en-us/how-to/faq/article/how-to-add-jobs-to-crontab-to-schedule-a-job).
Rechaza sobrescribir una instalación previa. Comprobar tanto cron persistente como
cron activo y observar un nuevo resultado con `entry_point=cron`.

Prueba explícita (dos mensajes simulados, sin alterar el estado de producción):
`/share/CACHEDEV1_DATA/.qpkg/HybridBackup/CloudConnector3/python/bin/python3 -I /share/CACHEDEV1_DATA/.erp-vps-monitor/nas_vps_monitor.py --entry-point test`.
La prueba recorre los umbrales de fallo y recuperación con sondas simuladas;
verificar además sondas reales y ejecución programada. Borrar la credencial de
staging únicamente después de verificar la instalación. No poner tokens en Git,
salida de comandos ni argumentos. El resultado de Telegram acredita aceptación
para el chat configurado, no lectura en el teléfono.

**Límites:** si el NAS, su Internet o Telegram fallan, no podrá entregar durante
esa falla. El estado de entrega solo avanza con confirmación y se reintenta en el
siguiente sondeo. No es vigilancia externa del propio NAS. No diagnostica todos
los módulos del ERP ni reemplaza pruebas de recuperación. Python depende de HBS;
tras actualizar HBS o QTS comprobar de nuevo el programador. No reiniciar el NAS
solo para validar esta instalación. No toca respaldos, retención ni datos del VPS.

**Retirada:** como administrador, retirar únicamente la línea marcada
`# erp-external-vps-monitor` del cron actual, recargar cron y verificar que no
quede un proceso del monitor. Conservar evidencia y retirar su carpeta exacta.
No restaurar ciegamente `crontab.before` si otros trabajos cambiaron después.
