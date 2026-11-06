# Ficha de fuentes — lectores de ventas históricas y ERP Doctor

Fecha y ambiente consultado: 2026-11-06; PostgreSQL local aislado de la tarea
`doctor-nas-sales-20261106` (puerto 55562) y producción en lectura acotada.

## Necesidad y unidad de análisis

Eliminar lecturas directas de evidencia Point desde reportes y rentabilidad que
ERP Doctor detecta como violaciones arquitectónicas. Conservar la consulta
histórica existente: una venta por fecha, sucursal Point y producto. El segundo
consumidor necesita únicamente los meses donde existe alguna fila Point, no una
nueva selección de fuente ni una reconstrucción de importes.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Evidencia de venta diaria | `pos_bridge.PointDailySale` / `pos_bridge_daily_sales` | Importación/sincronización Point existente | PK; unicidad fecha + branch + product; fuente en `source_endpoint` | Inventario confirma tabla y campos; baseline ene–sep 2026: 2312 grupos fecha/sucursal histórica, bruto 31544517.95, descuento 262116.95 | Reparto histórico; meses disponibles de reporte mensual |
| Identidad Point actual | `pos_bridge.PointBranch` | Sincronización Point y vínculo ERP existente | `external_id`; FK `erp_branch` | Regla preexistente de Point `2` y cobertura comprobada por baseline y fixtures | Lector histórico |
| Identidad ERP histórica | `core.Sucursal` | Catálogo operativo existente | PK y código `CRUCERO` | La consulta existente resuelve por código; prueba explícita del caso ausente | Anotación `sucursal_historica_id` |
| Fuentes analíticas de venta | `reportes.FactVentaDiaria`, `ventas.VentaAutoritativaPoint` | Publicación analítica/importación existentes | Fuentes y períodos propios | Candidatos léxicos en inventario; no equivalencia nueva inferida | Otros lectores y selección de ventas existentes |

## Alias y equivalencias

| Términos o identificadores | Estado: confirmada / candidata / distinta / no resuelta | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Point `external_id='2'`, fechas 2026-01-01 a 2026-06-30 → `CRUCERO` | Confirmada como regla implementada previamente; no se introduce equivalencia | Se traslada literalmente el `Case/When` del servicio de reparto histórico. Desde julio, antes de enero y otros IDs se conserva `branch.erp_branch_id` | Ninguna equivalencia nueva; cambios futuros requieren revisión |
| `CRUCERO` ausente | No resuelta como identidad operativa | La anotación devuelve NULL; no se sustituye por Bamoa ni otra sucursal | No adivinar ni modificar catálogo |
| Meses con evidencia Point vs meses de fuentes canónicas combinadas | Distinta | La primera consulta solo consulta filas `PointDailySale`; mezclar fuentes puede agregar períodos y cambiar consumidores | Se preserva consulta raw existente |

## Decisión de diseño

Reutilizar `ventas/services/sales_read_service.py`, lector autorizado existente,
con dos funciones específicas. `point_sales_with_historical_branch_identity`
recibe fechas inclusivas y devuelve la misma consulta anotada, sin filtrar por
fuente, signo, importe, cantidad o sucursal vinculada. La función pública
`reportes.services_reparto_historico.ventas_con_identidad_historica` conserva su
firma y delega. `point_sales_evidence_months` conserva `dates('sale_date', 'month')`
sobre el mismo rango; el consumidor mensual sigue calculando el fin de mes.

No se crean tablas, capturas, importaciones, cachés ni equivalencias; no se
modifican registros operativos. No se usa un lector que combine evidencia de
fuentes diferentes. Los cálculos posteriores y las demás fuentes del reporte
mensual permanecen idénticos.

Consultas o procedimiento reproducible:

- Inventario `manage.py inventario_fuentes_datos --term ventas --term pointdailysale`
  con PostgreSQL local. Artefacto de tarea: `fuentes.txt` en
  `~/.codex/automations/vps-erp-disk-health/doctor-nas-20261106/`.
  Es descubrimiento léxico; por sí solo no confirma equivalencias.
- Baseline productivo tomado por el coordinador en transacción
  `REPEATABLE READ, READ ONLY`, rango 2026-01-01 a 2026-09-30, agrupación
  fecha/sucursal con la anotación existente: 2312 grupos y meses enero–septiembre.
  SHA-256 del resultado normalizado:
  `3fb79b865114792b0577c35414c43905e851226faab62c640ebe2295e9df5e39`.
  Comando y resultado conservados en `baseline.py` y `baseline.json` del mismo
  directorio de artefactos. Los 2312 son grupos, no el conteo de ventas crudas.
- Regresiones `ventas.tests_sales_historical_readers`: bordes inclusivos de fechas,
  rango histórico, identidad actual fuera del rango, ausencia de Crucero,
  sucursal sin vínculo, fuentes distintas y cantidades/importes cero o negativos.
  Se comparan las filas y vínculos antes/después y se comprueba lectura SELECT.
- Consumidores: suites de reparto histórico de reportes/rentabilidad y reporte
  mensual, resultado integral y personal histórico.

Riesgos y pendientes: la ficha documenta una preservación de contrato, no una
reconciliación nueva de las ventas. La comparación del baseline después del
despliegue y la aceptación en producción pertenecen al cierre obligatorio de
esta tarea; las pruebas locales no las sustituyen.
