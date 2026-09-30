# Ficha de fuentes — identidad de sucursal y precio Point para Maya

Fecha local: 29 de septiembre de 2026, America/Mazatlan. Ambiente consultado: PostgreSQL y Point de producción, solo lectura. Implementación: worktree local aislado `codex/maya-point-branch-source`.

## Necesidad y unidad de análisis

Maya necesita comprobar qué producto y sucursal Point originaron una respuesta de stock. El identificador ERP y un nombre visible no prueban por sí solos esa identidad. Para precio necesita una lectura vigente del precio de venta, distinta del costo y de una réplica semanal.

## Fuentes candidatas

| Concepto | Modelo / fuente | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- |
| Sucursal ERP | `core.Sucursal` | ID 4, código `CRUCERO`, nombre `Sucursal Bamoa` | Consulta acotada en transacción READ ONLY | CRM, reservas y demás módulos ERP |
| Sucursal Point operativa | `pos_bridge.PointBranch` | ID interno 5, external_id `2`, nombre Bamoa, ERP 4 | Registro actual; respuesta directa Point stock con `PK_Sucursal=2`, `Sucursal=Bamoa` | Lector vivo y snapshots |
| Sucursal Point histórica | `pos_bridge.PointBranch` | ID 14, external_id `Crucero`, nombre Crucero, ERP 4 | Registro distinto, última observación de junio | Históricos; no equivale a Bamoa por compartir ERP 4 |
| Registro Point por nombre | `pos_bridge.PointBranch` | ID 27, external_id `Bamoa`, nombre Bamoa, ERP 4 | Resolver `BAMOA` selecciona este registro | Resolución CRM |
| Stock vivo | `/Stock/get_productos_existencia` | Producto PK 101, sucursal PK 2 | Bamoa: cantidad 3.0 en lectura directa; `Produccion Crucero` es otra fila, PK 10, cantidad 0.0 | `PointLiveInventoryLookupService` |
| Precio de catálogo vivo | `/Catalogos/get_producto_byID` | PK 101/código 0101 y PK 145/código 0145 | 2026-09-30 01:34:23 UTC: `Precio_default` 340.0 y 90.0, activos | Catálogo Point; alcance por sucursal no probado |
| Precio replicado | `pos_bridge.PointProduct.precio` | Producto y última captura | Códigos 0101/0145: captura 2026-09-28 09:00:37 UTC | Catálogo ERP; no prueba de precio vigente |
| Catálogo Maya | PostgreSQL `branches` | UUID y nombre | Ocho filas activas, ninguna Bamoa; conserva Crucero | Maya selección de sucursal |

## Alias y equivalencias

| Identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Point PK 2 y nombre Bamoa | Confirmada en respuesta directa | Misma fila de stock devuelta por Point | Preservar ambos campos de origen |
| ERP 4/CRUCERO y Bamoa | Compartición técnica existente, no fusión histórica | Tres PointBranch distintos enlazados a ERP 4 | No reasignar histórico ni renombrar datos operativos en esta corrección |
| Point external_id `Bamoa` y PK `2` | Correspondencia candidata para resolver entrada | Resolver elige alias 27; stock vivo identifica PK 2 | Conservar identificador real de la respuesta; no convertir alias en PK por suposición |
| `Precio_default` y precio de venta por sucursal | No resuelta | Detalle vivo no trae moneda, impuestos, lista ni precio específico por sucursal | Confirmación de la regla comercial solicitada a Mauricio |

## Decisión de diseño

Reutilizar el lector vivo, `PointBranch` y el contrato de disponibilidad. Corregir el selector compartido para priorizar globalmente el ID exacto y rechazar identidad ambigua o contradictoria. Con un PointBranch identificado, el respaldo por nombre usa esa identidad, evitando que el código ERP histórico elija otra sucursal. Añadir evidencia de origen real al contrato sin cambiar `branch_code` ERP ni crear tablas o equivalencias.

No usar `Costo_U`/`Costo_T` del stock como precio de venta. No activar precio en Maya hasta resolver el alcance de `Precio_default`. La configuración del proyecto es MXN, pero Point no devolvió moneda en las lecturas observadas.

## Procedimiento reproducible y límites

- `inventario_fuentes_datos --term sucursal --term point --term alias --presence --limit 8`: 123 candidatos. `horarios_especiales.SucursalAlias` no contiene registros y no participa en este resolver.
- Inventario de precio/producto: 33 candidatos, primeros 12 mostrados.
- Consultas SELECT acotadas y resolución en transacción PostgreSQL READ ONLY. Maya: `SELECT id,name FROM branches WHERE is_active=true ... LIMIT 30`.
- Lecturas Point autenticadas mediante cliente existente y bloqueo de sesión: catálogo/detalle por PK y stock por producto. Sin sincronización, reservas, pagos ni mensajes.
- No se repitió el GET público de disponibilidad: su código actual puede barrer reservas vencidas. Su respuesta previa no revela la sucursal Point real y no se toma como prueba de stock de Bamoa.

Los valores observados son evidencia fechada, no disponibilidad futura. Sigue pendiente configurar la sucursal Bamoa en el catálogo Maya mediante una acción revisada y validar el consumidor después del despliegue.
