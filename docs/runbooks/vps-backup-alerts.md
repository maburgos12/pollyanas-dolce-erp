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
