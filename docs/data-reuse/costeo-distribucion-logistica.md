# Ficha de fuentes — distribución gerencial de logística

Fecha y ambiente consultado: 2026-10-17; PostgreSQL16 local y VPS de producción en transacción READ ONLY. Periodo septiembre2026.

## Necesidad y unidad de análisis

Distribuir una sola vez el fondo logístico mensual existente a ruta y luego a destino/producto cargado. Cada fila final es sucursal + identidad explícita de artículo/receta + unidad física. La logística recibida no se considera automáticamente costo de lo vendido.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Fondo mensual | leer_logistica + leer_cargas_pagadas | Nómina, gastos, expedientes ERP | Periodo | $137822.44 septiembre | Reporte tres etapas |
| Ruta/distancia | RutaEntrega | Logística operativa | PK ruta + mes |49rutas con carga;48distancias programadas | App logística |
| Distancia alternativa | BitacoraSalidaLlegada | Operador de unidad | FK ruta + unidad + bitácora única cerrada | Ruta166: bitácora465,46km | Combustible y logística |
| Carga | RutaCargaChecklistLinea | Checklist operativo | PK / source_hash; sólo filas vigentes positivas |4378líneas positivas con destino y transferencia | Checklist y ERP |
| Valor transferido | PointTransferLine | Sincronización Point | FK exacta desde carga; unidad compatible |487líneas con costo cero, mayormente rebanadas | Inventario y logística |
| Receta mensual | RecetaCostoHistoricoMensual | Motor de costeo ERP | RecetaFK + periodo; cobertura100 | Rebanadas con histórico de septiembre | Costeo producción |
| Reventa mensual | ProductoReventaCostoHistoricoMensual | Costeo ERP/Point | FK_articulo del payload = PointProduct.external_id | SKU duplicados en variantes de toppers; no equivalentes | Costeo reventa |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Carga-transferencia | Confirmada | FK existente; no matching por nombre | No se modifica |
| FK_articulo / external_id | Confirmada | Identificador Point explícito; SKU no único | No se crea equivalencia |
| Unidad carga / transferencia | Confirmada cuando misma_unidad | Reutiliza helper inventario; nunca sumar kg con piezas | Ninguna conversión nueva |
| Distancia faltante programada / odómetro | Confirmada si bitácora cerrada de misma unidad y usada por una sola ruta | No duplicar kilómetros entre rutas | Regla de lectura documentada |

## Decisión de diseño

Reutilizar fuentes y servicio de etapas, añadir cálculo puro de reparto por mayor residuo. Fondo mensual completo a rutas por kilómetros; a carga por valor económico conocido. No sumar asignaciones previas por tiempo/combustible. No nuevas tablas, capturas, migraciones ni modificaciones de nómina o registros operativos. Consumidores: reporte HTML y Excel, con pestañas de rutas, sucursales, productos e incidencias.

Inventario: manage.py inventario_fuentes_datos --term ruta --term transferencia --term costo, con PostgreSQL16 verificado. Consultas acotadas por fecha septiembre y rutas incluidas, vía Django transaction.atomic + SET TRANSACTION READ ONLY.

Riesgos y pendientes: accesorios con costo cero y sin histórico exacto no equivalen a cero económico. Sin autorización para excluirlos de la base, preservar la asignación de su ruta pendiente a productos; si el destino de toda la ruta es único, sí se puede identificar la sucursal. Una ruta sin distancia no permite asignar todo el fondo a las demás. No se infiere consumo físico, tiempo trabajado ni entrega por una carga. Este reparto gerencial requiere conciliar carga contra venta antes de deducirlo del margen vendido.
