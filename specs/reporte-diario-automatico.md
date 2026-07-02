# Spec: Reporte diario automático (correo)

## Origen y relación con el resto del proyecto

Última pieza pendiente del proyecto "Consejo Estratégico de IA" (Caso 1 del
documento original). El 80% del trabajo ya existe: cada madrugada
(`PeriodicTask` "reportes: snapshot operacion dg", ~4:45am hora Mazatlán por
default, sembrada por `pos_bridge/management/commands/setup_celery_schedules.py`)
corre `task_refresh_dg_operacion_snapshot` (`reportes/tasks.py`) que llama a
`reportes/services_dg_operacion_snapshot.py::refresh_dg_operacion_snapshot()`
y guarda el resultado en `reportes.models.DgOperacionSnapshot` (campos
`fecha_operacion`, `payload` JSONField, `status` READY/ERROR/STALE,
`generated_at`). Hoy nadie recibe nada — Dirección tendría que entrar al
dashboard ejecutivo a verlo. Esta rebanada solo agrega el **envío**, no toca
el cálculo del snapshot (ya funciona).

## Decisiones ya tomadas (no reabrir)

- **MVP sin texto de IA.** El correo usa solo cifras reales del payload ya
  calculado — nada generado por OpenAI en esta rebanada. Una futura
  iteración podría agregar 1-2 líneas de "acción recomendada" reutilizando
  el patrón de `agente_rentabilidad.py`/`consejo_ia`, pero queda fuera de
  alcance aquí.
- **Destinatario: solo Dirección.** Mismo patrón que el correo de cierre
  mensual existente (`core/tasks.py` líneas 70-109): `_director_email()`
  resuelve `settings.DIRECTOR_EMAIL` → `DEFAULT_FROM_EMAIL` →
  `EMAIL_HOST_USER`; `_from_email()` = `DEFAULT_FROM_EMAIL` →
  `EMAIL_HOST_USER`. Sin CC a otros roles por ahora.
- **Correo de texto plano, sin adjuntos.** El patrón con adjuntos Excel/PDF
  (`recetas/tasks/daily_inventory_close.py`) es para otro destinatario
  (Carolina/producción) y otro caso de uso — no aplica aquí.
- **Programación vía `PeriodicTask`/`CrontabSchedule`** (Celery Beat usa
  `DatabaseScheduler`, el schedule real vive en la base de datos, no en el
  dict `CELERY_BEAT_SCHEDULE` de `config/settings.py`, que solo tiene
  entradas legacy). La tarea nueva se agrega a
  `pos_bridge/management/commands/setup_celery_schedules.py`, anclada unos
  minutos después de `dg_operacion_snapshot_cron` para asegurar que el
  snapshot del día ya esté listo antes de mandar el correo.

## Contenido real del payload (verificado en vivo)

`DgOperacionSnapshot.payload` (construido por
`recetas/views/plan.py::_build_dg_operacion_dashboard_payload`) trae, entre
otras, estas secciones:
- `point_exec_summary`: ventas/tickets/ticket promedio del día y del mes,
  `top_branches`, `active_branch_count`.
- `point_closure_summary`: `rows` por sucursal — `status` de cierre,
  `avg_ticket`, `sold_units`, `waste_units`, `sold_tickets`, `variance`.
- `point_waste_summary`: `total_qty`, `total_cost`, `top_branches`,
  `top_responsibles` de merma del día.
- `resumen_cierre`: semáforo (rojo/verde) por sucursal, `estado`
  (PENDIENTE/etc.).

El correo arma su cuerpo leyendo directamente estas secciones — no se
recalcula nada, no se inventa ningún número. Si una sección no está en el
payload (`.get(key)` devuelve `None`), esa parte del correo se omite con una
nota, no se rellena con ceros que parezcan datos reales.

## Requisitos exactos

### Nueva función/tarea (`reportes/tasks.py` o archivo nuevo
`reportes/tasks_reporte_diario.py`, decidir al implementar cuál ensucia
menos el archivo existente)
- `@shared_task(name="reportes.enviar_reporte_diario")`
- Lee el `DgOperacionSnapshot` más reciente (o el de `fecha_operacion` dado
  como parámetro opcional, mismo patrón que `inventario_final_cierre_email`
  con `fecha_operacion: str | None`).
- **Si `status != READY`:** no envía el correo normal — registra
  `logger.warning` y retorna `{"status": "omitido", "reason": "snapshot_no_listo"}`.
  No hay que alertar por correo un dato que la propia app ya marcó como
  incompleto/con error; el pipeline de monitoreo interno es responsabilidad
  de otra pieza, no de este correo.
- Si `status == READY`: arma el asunto y cuerpo (ver abajo) y llama a
  `send_mail` (mismo patrón que `_send_email` en `core/tasks.py`):
  destinatario = `_director_email()`; si no hay destinatario configurado,
  `logger.warning` + retorna `{"status": "omitido", "reason": "sin_destinatario"}`,
  igual que el patrón existente (nunca truena silenciosamente).

### Asunto y cuerpo del correo
- Asunto: `"Reporte diario Pollyana's Dolce - {fecha_operacion:%d/%m/%Y}"`.
- Cuerpo (texto plano, sin HTML, mismo estilo que el correo de cierre
  mensual):
  - Ventas del día (total, tickets, ticket promedio) desde
    `point_exec_summary`.
  - Sucursales con cierre pendiente o en alerta (semáforo rojo) desde
    `resumen_cierre` — lista corta con nombre de sucursal y estado.
  - Top sucursales por venta y por merma desde `point_exec_summary`/
    `point_waste_summary.top_branches`.
  - Merma total del día (cantidad y costo) desde `point_waste_summary`.
  - Nota de cierre: firma simple, sin sección de "acción recomendada"
    (fuera de alcance de este MVP).

### Programación (`pos_bridge/management/commands/setup_celery_schedules.py`)
- Nuevo bloque, mismo patrón que `dg_operacion_snapshot_cron` (líneas
  ~424-439): `CrontabSchedule.objects.get_or_create(...)` anclado a
  `dg_operacion_snapshot_cron` + 15-20 minutos (dar margen a que el
  snapshot termine de generarse), `PeriodicTask.objects.update_or_create(
  name="reportes: enviar reporte diario", defaults={"task":
  "reportes.enviar_reporte_diario", "crontab": ..., "enabled": True})`.

## Edge cases

- Snapshot con `status == ERROR` o `STALE`: no se envía correo normal (ver
  arriba), se registra en logs para diagnóstico interno.
- No hay ningún `DgOperacionSnapshot` para el día (aún no corrió la tarea de
  refresh, o falló completamente): la tarea de envío no encuentra registro
  → mismo tratamiento que status no READY, se omite con log.
- `DIRECTOR_EMAIL`/`DEFAULT_FROM_EMAIL` vacíos: se omite el envío con
  warning, igual que el patrón ya existente — nunca lanza excepción que
  tumbe el worker de Celery.
- Sección del payload ausente (`resumen_cierre` o `point_waste_summary` no
  presentes): esa parte del cuerpo se omite con una nota corta, no se
  inventa "0 mermas" si el dato realmente no está.
- Reenvío manual: la tarea debe poder llamarse con `fecha_operacion`
  explícito (para reenviar un día pasado si hace falta), igual que
  `inventario_final_cierre_email`.

## Fuera de alcance (explícito)

- Texto de "acción recomendada hoy" generado por IA — spec/PR futuro si se
  decide agregarlo.
- Destinatarios adicionales (jefas de sucursal/producción) — solo
  Dirección por ahora.
- Adjuntos (Excel/PDF) — correo de texto plano únicamente.
- Cambiar la hora/lógica de generación del snapshot mismo — ya funciona,
  no se toca.

## Definición de "hecho"

- [ ] Tarea `reportes.enviar_reporte_diario` implementada, con el mismo
      patrón de resolución de destinatario/remitente que `core/tasks.py`.
- [ ] Correo se arma solo con datos reales del payload, secciones ausentes
      se omiten con nota, nunca se inventa un número.
- [ ] `status != READY` o snapshot inexistente → se omite el envío con log,
      no lanza excepción.
- [ ] Sin destinatario configurado → se omite con log, no lanza excepción.
- [ ] Comando `setup_celery_schedules.py` actualizado con el nuevo
      `PeriodicTask`, anclado después del snapshot.
- [ ] Tests: envío exitoso con payload completo, snapshot con status ERROR
      (no envía), snapshot inexistente (no envía), sin `DIRECTOR_EMAIL`
      configurado (no envía), sección de payload ausente (correo se arma
      igual, sin esa sección).
- [ ] `python manage.py check` y `migrate --check` en 0 (no debería requerir
      migración — reutiliza `DgOperacionSnapshot` existente).
- [ ] Verificar en VPS tras deploy: correr la tarea manualmente una vez
      (`enviar_reporte_diario.delay()` o vía management command) y confirmar
      que el correo llega a `DIRECTOR_EMAIL` real antes de confiar en el
      cron.
