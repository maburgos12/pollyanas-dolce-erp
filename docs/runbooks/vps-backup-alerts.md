# Alertas de respaldos y disco del VPS

El timer `erp-backup-alerts.timer` revisa cada cinco minutos, directamente en el
VPS, tres preguntas: ¿se puede acceder al NAS por SSH?, ¿la recuperación sigue
verificada?, ¿el disco raíz llegó al75%? No ejecuta limpieza ni restaura datos.
La limpieza conserva sus verificaciones y siempre el respaldo local más reciente.

Canal: `PollyanaERPAlertasBot`, chat privado de Mauricio ya configurado en el VPS.
Se reutiliza `/etc/erp-mac-ia-watchdog/telegram.json` con `LoadCredential`; no
copiar tokens al repositorio ni imprimir errores HTTP completos con la URL.
El transporte existente es `/usr/local/libexec/erp-mac-ia-watchdog/vigilar_revision.py`.
El nombre del helper no implica dependencia de Mac-IA: ambos corren en el VPS.

## Qué hacer con cada aviso

- **NAS:** verificar `ssh qnap-nasdolce-archive` desde el VPS, WireGuard y lectura
  de `/share/ERP_BACKUPS`. No desactivar verificación de identidad SSH. Conservar
  todos los originales hasta restablecer acceso y verificar recuperación.
- **Recuperación:** leer `/var/lib/erp-backup-restore/health-latest.json` y
  `journalctl -u erp-backup-health.service`. Incluye informes ausentes, errores,
  antigüedad mayor a36horas y fallos/timeouts de systemd aunque no se haya podido
  escribir un informe nuevo. No inventar recibos ni ejecutar borrados manuales.
- **Disco75%:** revisar `df -h /` y el informe de recuperación. Al85% se escala
  inmediatamente aURGENTE. Usar únicamente la rotación verificada existente;
  otros candidatos requieren revisar propiedad, uso y datos únicos.

Responsable: Mauricio/Codex. Si no se resuelve con estas comprobaciones, conservar
los datos y diagnosticar la causa antes de autorizar cambios adicionales.

### NAS bloqueado y respaldo local pendiente

Un proceso en estado `D` dentro de CIFS puede conservar `.backup.flock` incluso
después de que systemd marque el servicio como fallido. Comprobar PID, espera del
kernel y propietario de ambos locks antes de reintentar. No borrar los archivos
de lock: eso crea otro inode y permite dos operaciones concurrentes.

La verificación CIFS del recuperador y del rotador se ejecuta en un proceso lector
aislado, sin heredar el candado del respaldo. Al exceder el plazo se conserva el
conjunto y el padre libera su candado; el candado separado del lector evita
acumular verificadores si el kernel aún no puede terminarlo. Un timeout no prueba
recuperación ni repara la conexión: conservar el PID y revisar CIFS/WireGuard.
El lector admite hasta una hora por punto; la comprobación real de todos los
medios puede superar 20 minutos. La rotación aplica además su presupuesto menor.

El rotador tiene un presupuesto total de 34 minutos, anterior al límite de
35 minutos del cron existente. Cada lector recibe como máximo el tiempo restante;
si se agota antes del retiro, conserva el conjunto. Una señal de terminación al
padre también cancela el grupo lector, sin esperar indefinidamente al kernel.
Esto no garantiza terminar la rotación: una comprobación completa que no quepa
en esa ventana requiere diagnóstico, nunca omitir hashes para acelerar el retiro.

Después de restablecer la conexión, comprobar el último manifiesto completo y
si el respaldo diario omitió su ejecución por el candado. Un respaldo compensatorio
debe conservar los anteriores y usar el script oficial, sin invocar manualmente
la rotación. Validar recuperación por separado antes de cualquier retiro aprobado.
Registrar y restablecer cualquier pausa temporal del timer; no silenciar los
monitores de disco ni inventarios.

La unidad de recuperación usa `--verify-only`: verifica y registra recuperación,
sin iniciar rotación ni mantenimiento Docker. Los recibos nuevos se guardan en
`/var/lib/erp-backup-restore/verification-receipts`, un directorio privado separado
de `receipts`, que consume el rotador. Verificar no convierte un respaldo nuevo en
elegible para retiro. Puede leer recibos canónicos válidos, pero no los modifica;
siempre comprueba nuevamente el contenido del NAS antes de informar éxito.
El cron del rotador existente sigue
usando su entrada oficial, ahora conectada al verificador acotado. Esta reparación
no amplía su autorización ni autoriza otros lotes de limpieza.
El límite total de la unidad sigue siendo ocho horas: su ejecución programada
a las 08:00 de Mazatlán deja margen antes del respaldo de las 19:00 (02:00 UTC).
Antes de un arranque manual, comprobar ese margen; la restauración remota y los
puntos pendientes también consumen tiempo mientras se conserva el candado local.

Antes de sustituir la entrada externa del rotador, el instalador conserva su
original conocido con el SHA-256 en el nombre y verifica sus bytes. Una reversión
del checkout debe restaurar también esa entrada: el wrapper nuevo no es compatible
con una revisión que aún no contenga `scripts/rotate_verified_backups.py`.
Verificar el hash del original antes de restaurarlo y conservar la evidencia;
no reconstruirlo desde logs ni reemplazar una versión desconocida. La unidad
anterior tampoco acepta `--verify-only`: mantener su timer pausado, con responsable
y fecha de revisión, hasta validar una versión compatible. La reversión no autoriza
rotación, reinicios ajenos ni retirar respaldos.

El preparador de limpieza consulta las identidades de archivos en `/proc` sin
seguir enlaces a montajes externos. Si no puede determinar el dispositivo, reserva
el inode conservadoramente y excluye el candidato. Los errores del sistema guardan
`errno`, ruta y mensaje en su estado privado para distinguir acceso, montaje y
capacidad, sin publicar esos detalles en Telegram.

## Entrega y repetición

Se avisa al aparecer o cambiar una condición y cada seis horas si persiste.
Variaciones de75% a76% no generan mensajes adicionales; el salto a85% sí.
Al desaparecer las condiciones se envía una recuperación. Solo se registra un
envío aceptado por Telegram para el chat esperado; fallos de envío se reintentan
en el siguiente ciclo. Una respuesta perdida puede duplicar un aviso, pero no
silenciarlo. La API confirma aceptación, no lectura en el teléfono.

Estado privado: `/var/lib/erp-backup-alerts/delivery.json`; trazas JSON con run_id
en `journalctl -u erp-backup-alerts.service`. Ante una caída del VPS o de su salida
a Internet no puede entregar mensajes; se necesitaría un monitor externo para
cubrir esa situación. No depende del Mac ni de Codex abierto.

## Instalación y pruebas

Después del merge y despliegue Git, instalar las dos unidades de `scripts/ops/`
en `/etc/systemd/system/`, validar con `systemd-analyze verify`, ejecutar
`systemctl daemon-reload` y `systemctl start erp-backup-alerts.service`.
Después comprobar sus logs y activar `systemctl enable --now erp-backup-alerts.timer`.
Las pruebas `--test nas`, `--test verification` y `--test disk` envían avisos
marcados PRUEBA mediante las mismas credenciales de systemd; simulan condiciones,
no rompen NAS/SQL ni cambian umbrales o estado real de deduplicación. Probar cada
causa y conservar los message_id aceptados antes de cerrar la instalación.
Para revertir: desactivar solo este timer; no alterar el timer de recuperación.

## Solicitudes de limpieza con botones (preparador separado)

Autorizado por Mauricio el 16/11/2026: `erp-cleanup-prepare.timer` comprueba cada
cinco minutos si el porcentaje entero de `df` es **mayor que75**. El monitor
original conserva sus avisos a75/85%, sin cambios de horario. El preparador vive
en el VPS; no necesita una Mac encendida ni consume llamadas de IA.

El preparador acepta únicamente archivos de caché APT y registros rotados de la
lista cerrada de `erp_telegram_cleanup.FILE_PATH`. Excluye symlinks, enlaces duros,
archivos abiertos/mapeados y cambios durante la comprobación. Acota cada lote a
256 archivos y1GB; Docker, respaldos, bases, medios y registros activos quedan
fuera del alcance automático. No usa `prune` ni borra candidatos.

Antes de mostrar botones debe copiar al NAS, comprobar el hash allí, leer el
archivo de regreso y recuperar cada miembro en un directorio temporal aislado,
verificando tamaño y hash contra la fuente. Comprueba espacio disponible antes
de preparar y conserva una reserva de5GiB en el VPS. Solo se retira el temporal
creado por esa comprobación; el archivo NAS y los originales se conservan.
Si falla una condición, informa un bloqueo y no presenta una autorización falsa.

El manifiesto y la evidencia quedan en estado privado. La solicitud pasa por
`erp_telegram_cleanup.request`: comprueba destinatario, mensaje y ambos botones
**Aprobar limpieza / Rechazar / revocar**. La pulsación solo registra la decisión.
No hay ejecutor automático de limpieza ni despliegue. El operador debe verificar
la aprobación vigente y las condiciones exactas antes de un retiro autorizado.
Una solicitud vencida no se renueva a ciegas; el mismo alcance no se vuelve a
solicitar automáticamente. Una entrega ambigua requiere conciliación humana.

### Instalación del preparador

Solo después de aprobar el SHA exacto, integrar y desplegar por el flujo oficial.
Desde el checkout de producción limpio, verificar primero que el módulo de
limpieza instalado corresponde al contrato de manifiestos y que existen sus
carpetas privadas. No reinstalar el receptor Telegram ni el publicador para
habilitar este preparador.

```sh
cd /opt/pastelerias-erp
systemd-analyze verify scripts/ops/erp-cleanup-prepare.service scripts/ops/erp-cleanup-prepare.timer
install -m 0644 scripts/ops/erp-cleanup-prepare.service /etc/systemd/system/erp-cleanup-prepare.service
install -m 0644 scripts/ops/erp-cleanup-prepare.timer /etc/systemd/system/erp-cleanup-prepare.timer
systemctl daemon-reload
systemctl start erp-cleanup-prepare.service
journalctl -u erp-cleanup-prepare.service --no-pager -n 30
systemctl enable --now erp-cleanup-prepare.timer
```

La unidad hace el sistema de archivos de producción de solo lectura; solo permite
escribir su estado, manifiestos, ledger y temporales privados. No cambia timers
existentes. Verificar resultado real y message_id antes de declarar instalación
completa; los tests locales no demuestran entrega real. Para desactivar, parar y
deshabilitar exclusivamente `erp-cleanup-prepare.timer`; conservar evidencia y
las alertas originales. Nunca interpretar su aprobación como permiso genérico
para borrar otros archivos.

Pruebas: `python3 -m unittest discover -s scripts/tests -p test_cleanup_prepare.py`
y `python3 -m unittest discover -s scripts/tests -p 'test_telegram*.py'`.
