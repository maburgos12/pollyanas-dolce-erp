# Ficha de fuentes — activación Point de Sinaloa de Leyva

Fecha y ambiente consultado: 2026-10-03; PostgreSQL de producción.

## Necesidad y unidad de análisis

Activar la sucursal existente para sincronizar los movimientos de Point y atender
las transferencias de apertura, conservando la identidad de cada sucursal y folio.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Sucursal ERP | core.Sucursal / core_sucursal | ERP | id 18, SINALOA_LEYVA | Inactiva; apertura 2026-10-03; activada con AuditLog por solicitud DG | Catálogos, reportes, logística |
| Sucursal Point | pos_bridge.PointBranch / pos_bridge_branches | Sincronización Point | id 28, external_id 14; erp_branch_id 18 | Workspace Point consultado en vivo: id_suc 14, wsName Sinaloa de Leyva | Inventario, ventas, movimientos |
| Transferencia | pos_bridge.PointTransferLine / pos_bridge_transfer_lines | Point; extractores y persistencia existentes | transfer_external_id + detail_external_id; source_hash | 100 líneas vigentes en folios 39691, 39693, 39694; enviado y recibido en cero al consultar | Logística, evidencia de transferencias |
| Inventario | pos_bridge.PointInventorySnapshot | PointInventoryExtractor / PointSyncService | Sucursal, producto, captura | Cero snapshots para Point 14 antes de la carga inicial | Cierre diario, conteos |
| Venta | pos_bridge.PointDailySale | Reportes oficiales Point | Sucursal, producto, fecha | Cero registros antes de la carga inicial | Ventas y analítica |
| Punto de reparto | logistica.PuntoLogistico | ERP | sucursal_id, coordenadas | Sin punto para ERP 18; ubicación solicitada a DG | ParadaRuta y geocerca |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Point 14 / Sinaloa de Leyva / Sin. Leyva / ERP 18 SINALOA_LEYVA | Confirmada, enlace existente | Workspace vivo y FK existente apuntan al mismo registro | Activación solicitada por DG |
| Leyva / Point 6 / ERP 8 LEYVA | Distinta | Identificadores y sucursales separados | No fusionar |

## Decisión de diseño

Reutilizar todos los registros y servicios existentes. Activar únicamente ERP 18,
sin crear otra sucursal ni cambiar fecha de apertura. Añadir SINALOA_LEYVA a la red
del cierre diario; conservar la exclusión de sucursales inactivas o futuras y no
añadirla a la cohorte histórica de sucursales maduras. No modificar API, modelos,
migraciones, permisos, variables de entorno ni contratos de sincronización.

Los jobs periódicos de ventas, inventario, mermas y transferencias consultados en
producción están habilitados y no tienen filtro de sucursal. La activación utiliza
esos mismos procesos. Sincronizar mediante los servicios existentes y conservar
solicitado/enviado/recibido según Point, sin confirmar despachos físicos por inferencia.

Consultas reproducibles: inventario_fuentes_datos --term sucursal --term transferencia
--term inventario; filtros Sucursal(id=18), PointBranch(external_id=14), snapshots y
ventas por branch_id=28, transferencias destino 28 con is_current_snapshot=True;
PeriodicTask habilitados task__startswith=pos_bridge. Consulta autenticada de
workspaces Point mediante PointHttpSessionClient, sin guardar credenciales.

Riesgos y pendientes: no confundir inventario ausente con cero; la parada de reparto
requiere coordenadas confirmadas. Los folios iniciales estaban solicitados pero no
enviados ni recibidos. La carga inicial y la validación visible se reportan con sus
resultados de ejecución, sin convertir esta ficha previa en prueba de entrega física.
