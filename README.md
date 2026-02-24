# Mini-ERP Pollyana's Dolce (Sprint 1 ejecutable)

Sprint 1 entrega:
- Importación desde Excel: catálogo de costos (Costo Materia Prima) + recetas (hojas con “Ingredientes”)
- Matching de insumos (EXACT / CONTAINS / FUZZY) con cola “Needs review”
- UI web (Django) para ver recetas, detalle, pendientes y MRP básico
- Historial de ventas (CSV/XLSX) por sucursal + pronóstico estadístico (mes / semana / fin de semana)
- Recomendación automática subir/bajar solicitud de Ventas según histórico
- Backtest estadístico visible en Plan Producción (MAPE/Bias por ventana)
- Ajustes de inventario con flujo de aprobación/rechazo (ADMIN) y trazabilidad
- Recepciones de compra cerradas impactan inventario automáticamente (entrada + bitácora)
- API: POST /api/mrp/explode/
- API: GET/POST /api/mrp/planes/ (listado y creación manual de planes de producción)
- API: GET/PATCH/DELETE /api/mrp/planes/{id}/ (detalle y edición/eliminación de plan)
- API: POST /api/mrp/planes/{id}/items/ (agrega renglón al plan)
- API: PATCH/DELETE /api/mrp/planes/items/{id}/ (edita/elimina renglón del plan)
- API: POST /api/mrp/calcular-requerimientos/ (por plan, por periodo o por lista manual)
- API: POST /api/mrp/generar-plan-pronostico/ (crea plan de producción desde pronóstico)
- API: POST /api/ventas/pronostico-backtest/ (mide precisión histórica: MAPE/Bias por ventana, con escenario `base/bajo/alto` y `min_confianza_pct`; soporta `?export=csv|xlsx`)
- API: GET /api/ventas/pronostico-insights/ (perfil estacional por mes/día, top recetas y participación; soporta `top`/`offset_top` y `?export=csv|xlsx`)
- API: GET /api/ventas/historial/ (histórico de ventas filtrable por sucursal/receta/fecha; soporta paginación `limit`/`offset` y `?export=csv|xlsx`)
- API: GET /api/ventas/pronostico/ (pronósticos filtrables por periodo/rango/receta; soporta paginación `limit`/`offset` y `?export=csv|xlsx`)
- API: GET /api/ventas/pipeline/resumen/ (resumen ejecutivo + detalle por receta y por sucursal: historial vs pronóstico vs solicitud; filtros `q`/`status`/`delta_min`, paginación `top`/`offset` y `top_sucursales`/`offset_sucursales`, ordenamiento `sort_by|sort_dir|sort_sucursales_by|sort_sucursales_dir` y soporte `?export=csv|xlsx`)
- API: GET /api/ventas/solicitud/list/ (solicitudes de ventas filtrables por alcance/sucursal/periodo; paginación `limit`/`offset`, opcional `include_forecast_ref=1` para comparar contra pronóstico, filtros `forecast_status`/`forecast_delta_min`, ordenamiento `sort_by`/`sort_dir` y soporte `?export=csv|xlsx`)
- API: POST /api/ventas/pronostico/bulk/ (carga masiva de pronóstico con dry_run)
- API: POST /api/ventas/pronostico/import-preview/ (previsualiza carga de pronóstico sin aplicar cambios)
- API: POST /api/ventas/pronostico/import-confirm/ (confirma y aplica carga de pronóstico)
- API: POST /api/ventas/pronostico-estadistico/ (forecast por sucursal con banda inferior/superior + comparativo contra solicitud en escenario `base/bajo/alto` y filtro opcional `min_confianza_pct`; soporta `?export=csv|xlsx`)
- API: POST /api/ventas/pronostico-estadistico/guardar/ (persiste forecast estadístico en Pronóstico de venta por escenario base/bajo/alto, con filtro opcional `min_confianza_pct` y soporte `?export=csv|xlsx`)
- API: POST /api/ventas/historial/bulk/ (carga masiva de historial de ventas con dry_run)
- API: POST /api/ventas/historial/import-preview/ (previsualiza carga de historial de ventas sin aplicar cambios)
- API: POST /api/ventas/historial/import-confirm/ (confirma y aplica carga de historial de ventas)
- API: POST /api/ventas/solicitud/ (alta/actualización de solicitud de ventas; incluye `forecast_ref` opcional y permite desactivar validación con `?validate_forecast=0`)
- API: POST /api/ventas/solicitud/bulk/ (carga masiva de solicitudes de ventas con dry_run)
- API: POST /api/ventas/solicitud/import-preview/ (previsualiza carga de solicitudes de venta sin aplicar cambios)
- API: POST /api/ventas/solicitud/import-confirm/ (confirma y aplica carga de solicitudes de venta)
- API: POST /api/ventas/solicitud/aplicar-forecast/ (ajuste automático desde escenario `base/bajo/alto`, con `dry_run`, tope opcional `max_variacion_pct`, filtro opcional `min_confianza_pct` y soporte `?export=csv|xlsx`)
- API: GET/POST /api/inventario/ajustes/ (consulta y alta de ajustes en pendiente / aplicado)
- API: POST /api/inventario/ajustes/{id}/decision/ (aprobar/aplicar/rechazar ajuste)
- API: GET/POST /api/inventario/aliases/ (catálogo de alias y alta/actualización)
- API: POST /api/inventario/aliases/reasignar/ (reasignación masiva de alias a insumo oficial)
- API: GET /api/inventario/aliases/pendientes/ (pendientes de homologación: almacén, Point y recetas, con export `csv|xlsx`)
- API: GET /api/inventario/aliases/pendientes-unificados/ (tablero unificado de pendientes cross-fuente con filtros, `limit`/`offset`, `sort_by`/`sort_dir` y export `csv|xlsx`)
- API: POST /api/inventario/aliases/pendientes-unificados/resolver/ (auto-resuelve alias cross-fuente con dry_run/commit)
- API: GET /api/integraciones/point/resumen/ (KPI de homologación Point: insumos, recetas, proveedores y pendientes)
- API: POST /api/integraciones/point/clientes/desactivar-inactivos/ (operación de clientes API inactivos; soporta `dry_run`)
- API: POST /api/integraciones/point/logs/purgar/ (limpieza de logs API por retención; soporta `dry_run`)
- API: POST /api/integraciones/point/mantenimiento/ejecutar/ (operación combinada: desactivación + purga; soporta `dry_run`)
- API: GET /api/integraciones/point/operaciones/historial/ (bitácora operativa de integraciones con filtros `action`, `user`, `model`, `q`, `date_from`, `date_to`, paginación `limit`/`offset`, ordenamiento `sort_by`/`sort_dir` y `export=csv|xlsx`)
- API: POST /api/inventario/point-pendientes/resolver/ (resolver/descartar pendientes Point por insumo, producto o proveedor)
- API: GET /api/compras/solicitudes/ (listado de solicitudes con filtros, estatus y presupuesto estimado; soporta `limit`/`offset`, `sort_by`/`sort_dir` y totales filtrados)
- API: POST /api/compras/solicitudes/import-preview/ (vista previa de carga masiva de solicitudes en JSON)
- API: POST /api/compras/solicitudes/import-confirm/ (confirmación de carga masiva usando filas de preview)
- API: GET /api/compras/ordenes/ (listado de órdenes con filtros y monto total estimado; soporta `limit`/`offset`, `sort_by`/`sort_dir` y totales filtrados)
- API: GET /api/compras/recepciones/ (listado de recepciones con filtros y totales por estatus; soporta `limit`/`offset` y `sort_by`/`sort_dir`)
- API: GET /api/presupuestos/consolidado/{YYYY-MM}/ (consolidado de presupuesto/ejecución con tablero `consumo_vs_plan`; soporta `periodo_tipo=mes|q1|q2`, `source`, `plan_id`, `categoria`, `reabasto`, `consumo_ref`, y paginación/ordenamiento en consumo con `limit`/`offset` + `sort_by`/`sort_dir`)
- API: POST /api/compras/solicitud/{id}/estatus/ (cambio de estado con reglas de transición)
- API: POST /api/compras/solicitud/{id}/crear-orden/ (genera OC desde solicitud aprobada, idempotente)
- API: POST /api/compras/orden/{id}/estatus/ (cambio de estado de OC con validaciones)
- API: POST /api/compras/orden/{id}/recepciones/ (registra recepción y puede cerrar OC)
- API: POST /api/compras/recepcion/{id}/estatus/ (actualiza recepción y cierra OC automáticamente si aplica)
- API: POST /api/auth/token/ (obtiene token para integraciones API)
- API: POST /api/auth/token/rotate/ (rota token del usuario autenticado)
- API: POST /api/auth/token/revoke/ (revoca el token del usuario autenticado)
- API: GET /api/auth/me/ (perfil + roles/capacidades del usuario autenticado)
- API: GET /api/audit/logs/ (bitácora operativa filtrable para ADMIN/DG; soporta `limit`/`offset` y `sort_by`/`sort_dir`)

## Requisitos
- Docker Desktop (Mac/Windows) **o** Python 3.12 + Postgres 16

## Opción A (recomendada): correr con Docker
1) Copia `.env.example` a `.env`
2) En la carpeta del proyecto:
   ```bash
   docker compose up --build
   ```
3) En otra terminal:
   ```bash
   docker compose exec web python manage.py migrate
   docker compose exec web python manage.py createsuperuser
   docker compose exec web python manage.py bootstrap_roles
   docker compose exec web python manage.py import_costeo test_data/COSTEO_Prueba.xlsx
   ```
4) Abre:
   - UI: http://localhost:8000/
   - Admin: http://localhost:8000/admin/
5) (Opcional) generar token API para integraciones:
   ```bash
   docker compose exec web python manage.py generar_token_api --username admin
   ```
6) Smoke operativo de integraciones API:
   ```bash
   BASE_URL=https://pollyanas-dolce-erp-production.up.railway.app \
   TOKEN=<TOKEN_DRF> \
   ./scripts/smoke_integraciones_api.sh
   ```
   Si tu Python local falla por certificados TLS en macOS, agrega `--insecure`.
   Opcional live (con efectos reales, requiere confirmación explícita):
   ```bash
   BASE_URL=https://pollyanas-dolce-erp-production.up.railway.app \
   TOKEN=<TOKEN_DRF> \
   ./scripts/smoke_integraciones_api.sh --live --confirm-live YES
   ```
7) Mantenimiento operativo por comando (sin API, apto para cron):
   ```bash
   # Preview (sin cambios)
   .venv/bin/python manage.py run_integraciones_maintenance --dry-run

   # Live (con cambios)
   .venv/bin/python manage.py run_integraciones_maintenance --confirm-live YES
   ```

## Opción B: correr sin Docker (dev)
- Configura un Postgres local y variables de entorno similares a `.env.example`
- Instala dependencias:
  ```bash
  pip install -r requirements.txt
  python manage.py migrate
  python manage.py createsuperuser
  python manage.py bootstrap_roles
  python manage.py generar_token_api --username admin
  python manage.py runserver
  ```

## Archivos importantes
- `recetas/management/commands/import_costeo.py` (comando de importación)
- `recetas/utils/importador.py` (parser de Excel)
- `recetas/utils/matching.py` (matching)
- `logs/` (reportes CSV del import)
- `scripts/smoke_integraciones_api.sh` (smoke operacional de endpoints de integraciones)
- `integraciones/management/commands/smoke_integraciones_api.py` (comando smoke con salida JSON)
- `integraciones/management/commands/run_integraciones_maintenance.py` (mantenimiento operativo CLI con bitácora)
- `scripts/auto_maintenance_integraciones.sh` (scheduler opcional para mantenimiento periódico)

## Notas
- El matching y captura operativa en UI ya incluyen búsqueda/autocomplete para insumos.
- El importador es idempotente por `source_hash` en costos y por `hash_contenido` en recetas.
