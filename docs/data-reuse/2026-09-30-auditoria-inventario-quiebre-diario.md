# Ficha de fuentes — auditoría diaria de inventario

Fecha y ambiente consultado: 2026-09-30; código en `main` y PostgreSQL de producción en modo de solo lectura.

## Necesidad y unidad de análisis

La auditoría mensual debe señalar el primer corte comprobado en el que el saldo de un producto deja de coincidir con Point y concentrar los movimientos ocurridos desde el último corte correcto. La unidad de análisis sigue siendo el expediente existente `mes + sucursal Point canónica + producto Point`. Un punto de control representa una observación persistida de existencia, no una nueva transacción ni un cierre diario oficial.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Expediente mensual | `reportes.ProductInventoryAuditCase` | Materializador mensual | Único por mes, sucursal Point y producto Point | El caso 79 conserva apertura, movimientos, cierre, diferencia y `source_trace` | Auditoría mensual y agente auditor |
| Existencia observada | `pos_bridge.PointInventorySnapshot` | Sincronización de inventario Point ya operativa | sucursal, producto, `captured_at`, job | Agosto: 182,308 filas y 29 fechas UTC; 3 Pecados Chico/CEDIS: 30 observaciones en 28 fechas | Inventario, cierres y reportes |
| Apertura y cierre protegidos | `PointHistoricalInventoryClosing` y líneas | Captura histórica verificada | fecha operativa, sucursal y producto | Cierres 31/07/2026 y 31/08/2026 usados por la auditoría mensual | Balance mensual |
| Ventas diarias | `PointDailySale` | Reporte oficial diario Point persistido | fecha, sucursal y producto | Agosto: 10,476 filas, del 01 al 31 | Balance y reportes de ventas |
| Producción | `PointProductionLine` | Reporte Point persistido | fecha, sucursal, detalle y producto | Agosto: 1,538 filas, del 01 al 31 | Producido vs Vendido y auditoría |
| Mermas | `PointWasteLine` | Reporte Point persistido | fecha-hora, sucursal y movimiento | Agosto operativo: 227 filas | Mermas y auditoría |
| Transferencias y retornos | `PointTransferLine` | Sincronización Point/Logística | transferencia, detalle, origen, destino y fechas | Agosto: 10,336 filas; conserva enviado, recibido y estado | Logística y auditoría |
| Conversiones | `PointConversionLine` | Reporte Point persistido | movimiento, sucursal y producto destino | Agosto: 16 filas; el origen puede estar vacío | Producido vs Vendido y auditoría |
| Ajustes de producto terminado | No existe una fuente Point persistida separada y completa para agosto | — | — | `ProductInventoryAuditCase.identified_adjustment` permanece en cero | Auditoría |
| Historial exacto por movimiento | `PointProductHistoryRow` | Importación manual histórica | importación + renglón | Producción: 0 filas | No disponible para esta etapa |

## Alias y equivalencias

| Términos o identificadores | Estado: confirmada / candidata / distinta / no resuelta | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Sucursales Point con igual `erp_branch_id` | confirmada | La FK explícita conserva la misma ubicación ERP | Ya aprobada |
| Snapshot Point ↔ cierre diario oficial | distinta | Es una observación a una hora concreta; no prueba por sí sola el cierre completo del día | Mostrar como “corte observado” |
| Primera observación diferente ↔ movimiento culpable | no resuelta | Las ventas están agregadas por día y no todos los movimientos tienen precisión intradía comparable | No atribuir culpabilidad automática |
| Movimiento con fecha operativa ↔ efecto acumulado al corte | confirmada cuando ocurre antes del corte y tiene identidad inequívoca | Reutiliza la misma regla mensual, zona `America/Mazatlan` y alias canónicos | Pruebas por tipo de movimiento |
| Diferencia diaria ↔ merma | distinta | Solo una fila de merma confirma merma | Mantener unidad no localizada si falta evidencia |

## Decisión de diseño

Extender la investigación regenerable del expediente existente. Un servicio mensual reutilizará las fuentes persistidas, construirá puntos de control por producto y ubicación y comparará el saldo acumulado con el último snapshot disponible de cada fecha local. Guardará solamente el resumen del primer quiebre y su ventana de movimientos dentro de `investigation_summary`; no creará tabla, importador, descarga Point ni captura paralela.

El cálculo se ejecutará en lote para el mes y cargará cada fuente una sola vez. El detalle del caso leerá la proyección almacenada. Para movimientos que solo informan fecha, un snapshot intradía se compara contra el rango mínimo/máximo posible y no contra un orden inventado. Cuando no exista snapshot suficiente, el expediente dirá “sin corte intermedio comprobable” y conservará la diferencia mensual.

Consultas o procedimiento reproducible:

```bash
python manage.py inventario_fuentes_datos \
  --term inventario --term transferencia --term conversion --term merma \
  --term produccion --term venta --term ajuste --term devolucion
python manage.py shell -c "# consultas ORM acotadas a agosto de 2026"
```

Riesgos y pendientes: los snapshots no cubren cada transacción y no son cierres diarios oficiales. La auditoría localizará el primer intervalo comprobado, no inventará el movimiento causante. Una precisión transaccional mayor requeriría una fuente histórica persistida que hoy está vacía; queda fuera de esta etapa y no autoriza una nueva descarga.
