# RRHH Cumpleaños Implementation Plan

> **For agentic workers:** Execute with subagent-driven-development for the isolated UI task and inline work for the data/dispatch task. Review spec compliance and code quality before delivery.

**Goal:** Calendario y avisos ERP/correo de cumpleaños para empleados activos y responsables autorizados.

**Architecture:** RRHH mantiene una fecha nullable y una bitácora de despacho con unicidad por destinatario/tipo/referencia. Servicio compartido calcula alcance y ocurrencias anuales; vista y worker consumen el mismo servicio. Worker de notificaciones despacha independientemente los dos canales.

**Tech Stack:** Django 5, PostgreSQL 16, Celery/django-celery-beat, templates Django y CSS del ERP.

## 1. Fuente y despacho

Files: rrhh/models.py, rrhh/migrations/0046_*.py, rrhh/services_cumpleanos.py, rrhh/tasks.py, config/settings.py, rrhh/tests_cumpleanos.py.

- [x] Pruebas de ocurrencia: `fecha_cumpleanos(date(2000,2,29),2027) == date(2027,2,28)`; cambio de año y próximos siete días.
- [x] Pruebas de alcance: CH/DG global, JEFATURA por departamento, inactivos/no autorizados sin acceso y empleados sin cuenta incluidos.
- [x] Implementar fecha_nacimiento nullable y AvisoCumpleanos único (usuario,tipo,fecha_referencia), FK a notificación, estado correo, destino, referencia proveedor, error e intentos.
- [x] Servicio público: `puede_ver_cumpleanos(user)`, `vista_global_cumpleanos(user)`, `puede_gestionar_cumpleanos(user)`, `empleados_visibles(user)`, `fecha_cumpleanos(nacimiento,anio)`, `eventos_entre(queryset,desde,hasta)` retorna lista `{empleado,fecha}`.
- [x] Pruebas de despacho con locmem: repetir genera una sola notificación/correo; correo rechazado recuperable no duplica ERP; error de red no reenvía; baja entre creación y envío no avisa; sin correo no usa expediente.
- [x] Implementar `generar_avisos_cumpleanos()` por fecha local, sin histórico. Atomic/row-lock para ERP y reserva de correo antes del proveedor.
- [x] Job rrhh.tasks.avisar_cumpleanos en cola notificaciones; schedule diario 8:00 en CELERY_BEAT_SCHEDULE. Reintentos de fallo confirmado cada 5 minutos, máximo tres y solo mismo día.

## 2. Calendario y captura

Files: rrhh/views_cumpleanos.py, rrhh/templates/rrhh/cumpleanos.html, static/css/rrhh_cumpleanos.css, rrhh/tests_cumpleanos_views.py.

- [x] GET autorizado usa empleados_visibles; filtros mes/departamento/sucursal, calendario mensual y lista responsive. Hoy/siete días/mes, fechas pendientes. Historial de bitácora visible solo global.
- [x] POST autorizado solo gestión CH/superuser; fecha válida no futura, motivo obligatorio, lock empleado activo, update fecha/updated_at y AuditLog en misma transacción. JSON y HTML comparten acción; errores preservan inputs.
- [x] Formularios data-async-action; éxito toast con redirect a #empleado-id, errores JSON400; fallback HTML conserva datos de captura. No mostrar año/edad en vista de jefaturas.
- [x] Tests POST validación y 403 de jefatura, privacidad, filtros y retorno estable. Navegador local escritorio/móvil con console y XHR.

## 3. Integración y entrega

Files: rrhh/urls.py, core/navigation.py, core/access.py, rrhh/views.py, static/erp-sw.js, docs/ux/action-context-coverage.md.

- [x] Agregar ruta y NAV_GROUPS Cumpleaños con permiso específico, sin conceder RRHH completo. Mantener pestaña heredada coherente.
- [x] Bump CACHE_NAME en mismo commit del template/estáticos. Registrar cobertura de captura progresiva.
- [x] `manage.py check`, `migrate --check`, `makemigrations --check --dry-run` y tests cumpleaños/RRHH/navegación con DATABASE_URL PostgreSQL aislado.
- [ ] Revisar spec y diff completo, commit solo alcance, PR borrador con resumen funcional y pruebas, esperar CI; publicar/merge autorizado.
- [ ] Deploy `bash scripts/deploy_web_safe.sh` sin pull previo; verificar migración, worker/beat/cola, página y nueva lectura de activos/fechas en producción. No enviar pruebas a personas con datos ficticios.
- [ ] Cerrar con task_workspace_close --state merged solo tras evidencia de producción; conservar recursos ajenos. Si datos siguen pendientes, documentar cantidad y limitación al usuario.
