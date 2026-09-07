# Conteos físicos de sucursales Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox syntax for tracking.

**Goal:** Capturar, revisar y conservar conteos físicos de sucursales desde App Operativa y ERP sin alterar saldos.

**Architecture:** Dominio nuevo en inventario, con modelos propios importados por models.py. Un servicio transaccional compartido para app y ERP, referencias Point pasivas y vistas separadas por finalidad. Reutilizar acceso granular y acciones progresivas; no invocar el cierre histórico.

**Tech Stack:** Django 5.0.1, PostgreSQL 16, templates Django, JavaScript/CSS locales, openpyxl.

## Entorno

- [x] Worktree registrado conteos-sucursales desde 87b5fa43, preflight limpio.
- [x] PostgreSQL aislado erp_conteos_sucursales_db, puerto 55519, volumen erp_conteos_sucursales_pg.
- [x] Migraciones completas de main, migrate --check y check sin errores.

Comandos desde el worktree, siempre después de pg_isready:
```bash
export APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55519/pastelerias_erp
PY=/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/.venv/bin/python
$PY manage.py check
$PY manage.py migrate --check
```

## 1. Dominio transaccional y permisos

Archivos: inventario/models_conteos.py, inventario/conteos_access.py, inventario/services_conteos.py, inventario/models.py (import), inventario/migrations/0015_conteos_sucursales.py, inventario/tests_conteos_domain.py.

- [x] Pruebas primero: preparar como coordinador; asignado solo sucursal vigente; rechazo de usuario ajeno y revocado; vacío vs cero; Decimal finito no negativo; versión desactualizada; misma clave retorna recibo, distinto cuerpo rechaza; recontar preserva rondas; aceptar no cambia tablas ajenas; fallo de auditoría revierte escritura.
- [x] API de servicio: preparar_conteo(actor, sucursal, responsable, fecha, titulo, items, request_id); ejecutar_accion(conteo_id, actor, action, version, request_id, payload). El payload de guardar/enviar contiene lecturas {linea_id: {cantidad, incidencia}} y observaciones. reconteo contiene linea_ids y motivo; aceptar/cancelar contiene motivo. Retorno dict id/version/ronda/estado.
- [x] Modelo/constraints: conteo, linea de alcance con exactamente uno producto/insumo, lectura única por linea/ronda, evento y recibo único por conteo/request_id. Cantidades >=0 o NULL; no redondeo silencioso. Acceso explícito usuario/sucursal, flags capturar/revisar/activo.
- [x] Revisar cumplimiento y calidad antes de integrar vistas. Generar e inspeccionar migración aditiva; correr pruebas.

## 2. Referencia pasiva

Archivos: inventario/conteos_point.py, inventario/tests_conteos_point.py.

- [x] Tests con modelos reales de Point: ausencia, sucursal ambigua, ciclo parcial/fallido, un solo ciclo exitoso, stock crudo vacío, unidad incompatible, congelación de referencia histórica.
- [x] Construir referencia_conteo(conteo) sin red ni escrituras fuera del conteo. Devuelve estado, fuente, timestamps, lineas con cantidad nullable, unidad, advertencias y corte_verificado=False. No fallback de saldo ERP.
- [x] Persistir referencias por acción explícita y versionada, con auditoría. Confirmación temporal solo por revisor con motivo y datos completos; nunca derivada de captured_at.

## 3. Vistas y formularios compartidos

Archivos: inventario/views_conteos.py, inventario/urls_conteos.py, inventario/forms_conteos.py, inventario/urls.py, operacion/urls.py, inventario/tests_conteos_views.py.

- [x] Tests primero: login, CSRF, permisos de objeto, página ciega sin datos Point, GET sin mutaciones, POST JSON/HTML único servicio, 400 validación/403 permisos/409 conflicto, Excel compatible y seguro ante fórmulas.
- [x] App /app/conteos/ y ERP /inventario/conteos-sucursales/ comparten dominio. Exportación filtrada, identidad/sucursal/ronda/unidad/cantidad/fecha y estado explícitos.
- [x] Preparar artículos desde catálogo mediante selección explícita; unidad y evidencia obligatorias por artículo, sin conversiones. Filtros de búsqueda/categoría. Lista de responsables con perfil sucursal o acceso explícito.
- [x] Evidencias con validación de tamaño/tipo, descarga autenticada con mismo permiso del conteo; sin URL pública en respuestas.

## 4. UI y navegación

Archivos: templates/inventario/conteos/*.html, static/inventario/conteos.css, static/inventario/conteos.js, operacion/services.py, core/navigation.py, core/access.py, core/middleware.py, static/operacion/sw.js, templates/operacion/app_home.html, docs/ux/action-context-coverage.md.

- [x] Impeccable/emil: marca existente, controles de 44px, labels reales, accesibilidad, cero/vacío, estados de envío y referencia distinguibles.
- [x] Guardado usa contrato data-async-action y toasts. Borrador keyed por identidad/ronda/versión; mantener UUID si se pierde respuesta; conflicto no auto-rebase. Evitar doble envío y conservar posición/búsqueda.
- [x] Agregar tile solo a quien puede ver conteos; permitir ruta estrecha en MermasOnlyMiddleware, sin ampliar sus demás permisos. Añadir navegación en fuente NAV_GROUPS y submódulo.
- [x] Bump SW y registro; excluir rutas de datos/evidencias de cache; regresión app existente.

## 5. Verificación y entrega

- [x] Pruebas domain/Point/views y concurrencia PostgreSQL. Probar exportación y aislamiento de movimientos/saldos.
- [x] Browser real local 390px y escritorio; envío/reconteo/revisión, pérdida de red, recuperación, consola y XHR.
- [x] Check, migrate --check, makemigrations --check, regresión de core/operacion/mermas; revisión del diff y esquema.
- [ ] Commits solo relacionados; PR borrador con pruebas y límites; CI y revisión; merge + scripts/deploy_web_safe.sh sin pull manual.
- [ ] Producción: migración registrada, ruta y assets correctos, acceso autenticado y ausencia de escrituras ajenas. No inventar conteo real de sucursal.
- [ ] Cierre/handoff documentado según evidencia. Piloto físico pendiente hasta conteo real por personal; no declarar validación física con fixtures.

## Evidencia técnica local

- 505 pruebas de regresión ejecutadas; tres fallos correspondían a versiones anteriores de SW, corregidos. Posteriormente 115 pruebas focalizadas pasaron.
- 54 pruebas del módulo (dominio/Point/vistas), incluyendo concurrencia real, CSRF, acceso restringido y payload compacto de 510 artículos.
- Navegador Chromium: 390 px y 1440 px, preparar entre búsquedas, inicio, cero, guardar, adjuntar, offline, conflicto entre pestañas, ventana duplicada con opener, almacenamiento bloqueado, reconteo, aceptación, Excel y cancelar/Escape.
- Respaldo: 10 tests standalone; dump real de PostgreSQL local y TAR restaurados en base aislada erp_conteos_restore. Archivo restaurado coincide en SHA-256 y tamaño con el evento.
- Alcance adicional necesario: respaldo conjunto en scripts/backup_db.sh y prueba en CI; no cambia cron/env.
- Piloto real de sucursal todavía pendiente; fixtures no acreditan observación física.
