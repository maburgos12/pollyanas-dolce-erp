# Ficha de fuentes — Costeo mensual en tres etapas

Fecha: 16/10/2026. PostgreSQL de producción, corte septiembre; implementación local aislada.

## Necesidad y unidad de análisis
Producto/mes en fabricación; persona, unidad y ruta/mes en distribución; producto/sucursal/mes en venta. El reporte es provisional y no altera cierres aprobados, nómina ni inventario.

## Fuentes candidatas y decisión
| Concepto | Modelo / servicio | Identificador y creador | Evidencia septiembre | Consumidores / decisión |
|---|---|---|---|---|
| Fabricación | services_mano_obra_mensual.calcular | RRHH + Point, periodo/receta | 50 productos, 11778 piezas | Reutilizar sin despacho para fabricación vendida |
| Nómina | NominaLinea, Empleado | RRHH, línea/empleado/usuario ERP | 5 repartidores, 44184.38; despacho 17776.14 | Un solo fondo, retiro analítico de sucursal; no escritura |
| Operación | Repartidor, RutaEntrega, BitacoraSalidaLlegada | Logística, FK usuario/ruta/bitácora | 49 rutas, 107 bitácoras | Referencia de horas de ruta y km de bitácora; sin atribuir domicilios por descarte |
| Cargas | CargaCombustibleUnidad | Logística, unidad/bitácora | 30 cargas, 32000 | Desglosar real consolidado, nunca sumarlo de nuevo |
| Gastos logística | LineaPresupuestoMensual ORIGINAL | Consolidación ERP, línea/mes | Combustible 32000, mantenimiento 14930, GPS 887 | Leer importes respaldados por fuente automática; nulos pendientes |
| Carga transportada | RutaCargaChecklistLinea | Operación / PointTransferLine | 5048 vínculos únicos, 178 pendientes/faltantes | Mostrar por unidad física y estado, no afirmar entregas |
| Venta | sales_read_service.get_sales_range_grouped | Point, FK sucursal/receta | 2664013.99, 9 sucursales | Reutilizar fuente canónica; conciliar detalle antes de costear |
| Gasto sucursal | leer_costos_mensuales / costos_de_sucursal | Gastos, obligaciones, RRHH y reglas | Parcial, electricidad/soportes/cargas patronales pendientes | Reutilizar sin modificar otros consumidores |
| Reparto de venta | ReglaAsignacionGasto, OPEX_TOTAL_SUC | Configuración ERP, regla única activa | VENTAS | Sólo gasto de venta dentro de sucursal; no extender a logística |
| Domicilio comercial | PointSalesDailyProductFact | Point, SKU 0303/0304, fecha | 67 servicios / 1035; notas 59 / 915 | Universos distintos, no afirmar viajes ni pagos |
| Domicilio operativo | SolicitudDomicilio → PedidoCliente | CRM / logística, point_note_id, point_note_sold_at | Vínculos de ejecución incompletos | Sin margen completo ni creación de solicitudes ficticias |

## Alias y equivalencias
Se usan FK de usuario, receta, sucursal, unidad, ruta y bitácora; no nombres. Los IDs Point y ERP son distintos. Puesto REPARTIDOR confirmado para cinco personas; sexto conductor con puesto vacío permanece en origen. Dirección confirmó operación mixta (abasto y domicilio) y pidió conservar pendientes odómetros GS-DC1-0161/0149 y duraciones de RUT-202609-0053/0048. No se aplican correcciones operativas.

## Reglas y límites
- Fabricación vendida: costo mensual por receta × venta de esa receta. No FIFO; productos sin receta/costo quedan pendientes.
- Referencia de personal por ruta: percepciones / horas de nómina × intervalo operativo. No mide jornada. Duraciones no positivas, ausentes, mayores a 24 h, rutas no completadas y solapamientos quedan sin asignar. Si las bases exceden horas de nómina, no se reparte ese fondo.
- Combustible: conciliar total de cargas contra real. Una unidad con cualquier odómetro incompleto no reparte su fondo; no redistribuye la cuota faltante. Una bitácora con más de una ruta queda pendiente. Cargas no equivalen a consumo exacto.
- Coordinación, mantenimiento y GPS sin conductor validado se conservan en fondo sin reparto; el subtotal por ruta es parcial. Tramos/destinos y producto permanecen pendientes, no usar porcentaje global de ventas.
- Gasto de venta por producto sólo si regla única OPEX_TOTAL_SUC=VENTAS y detalle de ventas conciliado; redondeo hacia abajo mantiene centavos residuales visibles.
- No sumar gastos, facturas, pagos y snapshots como nuevos costos. No mostrar utilidad neta con distribución y gastos faltantes.
- Nómina de sucursal usa expediente actual según lector existente; mantener advertencias históricas. Personas de producción/distribución se excluyen analíticamente una sola vez cuando también aparecen en ese lector.

## Reproducción y validación
Inventario de fuentes ejecutado con PostgreSQL y términos logistica/domicilio/rentabilidad; sólo candidatos léxicos. Consultas de producción acotadas a septiembre con transacción READ ONLY; corte preservado en runtime de tarea. Pruebas verifican conservación, remanentes, exclusiones, odómetros y duraciones pendientes, solapamientos y controles de acceso existentes. Sin nuevas tablas, capturas, migraciones ni cambios de reglas. Panel y Excel usan las mismas lecturas; cierre aprobado continúa con su contrato anterior.
