# SOP · Loop comercial y aprobación de forecast

## Objetivo
Dejar una rutina operativa única para que el ERP mantenga completo el loop comercial: ventas canónicas, forecast por `SKU x sucursal x día`, snapshots, artifacts y estado de aprobación sin inconsistencias.

## Política permanente
- La verdad del forecast es `SKU x sucursal x día`.
- El ingreso final siempre es `precio real vigente por SKU y sucursal x piezas forecast`.
- `MATRIZDBG` nunca cuenta como sucursal operativa.
- `GUAMUCHIL` se trata como expansión reciente, no como same-store maduro.
- Un `0` diario no es hueco automático si hay indicador diario, backfill oficial exitoso con `0` filas o cierre/incidente operativo validado.
- Nada se declara corregido o aprobado si no quedó recalculado, persistido, snapshot-verificado, auditado y consistente en ERP.

## Tabla de control para Notion / operación

| Control | Qué valida | Responsable | Evidencia mínima | Estatus |
|---|---|---|---|---|
| Fuente canónica diaria | Ventas históricas en PostgreSQL canónico | BI / Integraciones | `reportes_factventadiaria` y/o `pos_bridge_daily_sales` visibles | Pendiente / OK |
| Gobernanza de sucursales | Solo 9 sucursales reales, sin `MATRIZDBG` | BI / ERP | Conteo y códigos operativos del día | Pendiente / OK |
| Ceros válidos | `0` justificados por indicador, backfill `0` o excepción validada | Integraciones / Operación | Indicador Point, log de backfill o nota operativa | Pendiente / OK |
| Forecast por piezas | Proyección a nivel `SKU x sucursal x día` | Comercial / ERP | Forecast persistido por evento | Pendiente / OK |
| Curva diaria | Día principal y pre-pico sin residual | Comercial / ERP | Auditoría diaria 2026 vs 2025 | Pendiente / OK |
| Ingreso real | Precio vigente por SKU x piezas forecast | Finanzas / ERP | Financial base persistido | Pendiente / OK |
| Snapshot alineado | `snapshot_version == version` | ERP | Detail snapshot visible | Pendiente / OK |
| Artifacts alineados | Dashboard/ZIP/exportes en la versión vigente | ERP | 5 artifacts por forecast vigente | Pendiente / OK |
| Guard estacional | Auditoría 10/10 sin findings bloqueantes | Dirección / ERP | `audit_seasonal_event_forecasts` | Pendiente / OK |
| Estado ERP | Solo subir a `LISTO_PARA_REVISION` si no hay findings | Dirección / ERP | Estado final coherente | Pendiente / OK |

## Rutina diaria recomendada
1. Ejecutar `./.venv/bin/python manage.py audit_commercial_forecast_loop --days-back 30 --write-report`
2. Revisar huecos diarios válidos pendientes.
3. Revisar eventos en `EN_MODELADO`, `LISTO_PARA_REVISION` y `PENDIENTE_DIRECCION`.
4. Si la auditoría estacional regresa findings, regresar a modelado y recalcular.
5. Si no hay findings, dejar `LISTO_PARA_REVISION`.

## Criterio de cierre real
Un evento o forecast comercial solo queda cerrado si:
- recalculó
- persistió
- snapshot quedó alineado
- artifacts quedaron vigentes
- auditoría quedó en verde
- estado ERP coincide con la auditoría
