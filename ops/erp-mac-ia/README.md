# Ejecutor Mac-IA-02

Código de producción: `/opt/pastelerias-erp/ops/erp-mac-ia/worker.py`, desplegado
únicamente mediante merge + `scripts/deploy_web_safe.sh` (no copiar código al VPS).
Lee/escribe la **cola técnica**, nunca los expedientes originales. El host ejecuta
comandos Django de nombre fijo por Docker, no instrucciones elegidas por el modelo.

Después del despliegue:

```sh
install -m 644 ops/erp-mac-ia/pastelerias-mac-ia.service /etc/systemd/system/
install -m 644 ops/erp-mac-ia/pastelerias-mac-ia.timer /etc/systemd/system/
systemd-analyze verify /etc/systemd/system/pastelerias-mac-ia.{service,timer}
systemctl daemon-reload
docker compose exec -T web python manage.py mac_ia_jobs enable
systemctl enable --now pastelerias-mac-ia.timer
```

La credencial del modelo va en `/etc/pastelerias-mac-ia/api.key`, root:root 0600,
directorio 0700, por canal SSH fijado. Nunca en argumentos, logs, Git ni `.env`.
El ejecutor usa exclusivamente HTTPConnection a 127.0.0.1:18125 (sin proxies),
llave Bearer y alias `pastelerias-qwen3-8b`. La cuenta SSH exclusiva aplica el
fragmento `81-pastelerias-mac-ia-02-llm.conf` y authorized_keys con
`restrict,port-forwarding,permitlisten="127.0.0.1:18125"`; sin sesiones.
La Mac mantiene el túnel saliente con su llave dedicada, distinta a gestión.

Horario Celery: una hora, alta idempotente por `mac_ia_jobs enable`; el setup global
de horarios no se modifica. Timer del VPS: un trabajo cada minuto después de acabar
el anterior. Flock + bloqueo PostgreSQL + lease de 12 minutos; HTTP 300 segundos,
salida 192 tokens, hasta 4 casos, máximo 8 trabajos pendientes/en ejecución.
Fallo de salud no consume turno ni inicia inferencia. Fallos de inferencia tienen
hasta 3 intentos con espera de 5/10 minutos; luego revisión técnica. No API pagada.
Un proceso interrumpido se recupera al vencer el lease; la respuesta vieja se rechaza.

Lectura: últimas 2,000 tareas, ventana móvil de 7 días, fuentes de inventario y roles
demanda/producción/compras/dirección del plan. Se conserva cada rol; duplicados del
mismo expediente se agrupan usando la evidencia más reciente, no IDs/horas nuevos.
Sólo estado/prioridad y conteos enteros de lista permitida; no notas, nombres,
montos ni documentos. Si hay más de 2,000 fuentes, no se crean resúmenes parciales.
Historial de firmas: últimos 2,000 trabajos; revisar retención antes de ampliar el
piloto. Reutilizar evidencia idéntica puede reutilizar un resumen anterior: la hora
de generación no prueba una verificación nueva del expediente operativo.

```sh
# Diagnóstico compacto / pausa / inspección de servicio
docker compose exec -T web python manage.py mac_ia_jobs status
docker compose exec -T web python manage.py mac_ia_jobs disable
systemctl stop pastelerias-mac-ia.timer
journalctl -u pastelerias-mac-ia.service -n 30 --no-pager
```

Desactivar impide nuevas tomas; una inferencia ya tomada puede terminar y guardar
sólo su resumen técnico. Para detenerla inmediatamente, parar también el servicio;
quedará su lease hasta caducar. No borrar historial ni cambiar tareas originales.
Ver el panel `/orquestacion/#mac-ia-02` con los permisos habituales. Contacto y
último éxito se muestran con hora; no interpretar un estado histórico como salud
actual. Tokens son locales, no dinero ahorrado. FileVault exige desbloqueo/inicio
de sesión después de reiniciar la Mac; mantener corriente, tapa abierta e Internet.

Pruebas: `python manage.py test orquestacion.tests_local_ai`; ejecutor:
`python -m unittest discover -s scripts/tests -p test_mac_ia_worker.py`.
