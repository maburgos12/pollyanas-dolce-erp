# Ficha de fuentes — fusión de insumos y consumo BOM

Fecha y ambiente: 2026-11-05; diagnóstico en producción, pruebas en PostgreSQL16
aislado de la tarea `inventario-fusion-consumos2026`.

## Necesidad y unidad de análisis

Conservar un consumo calculado por producción Point e insumo al fusionar dos
artículos. Una fusión cambia su FK; la clave idempotente incluye ese identificador.
El reproceso debe reconocer el evento existente. No representa un conteo físico.

## Fuentes candidatas

| Concepto | Modelo / tabla | Creador y actualizador | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Artículo | `maestros.Insumo` | Maestro y fusión autorizada | PK canónica | 556 inactivo y493 actual, ambos Pan de Lotus Bollo | Recetas, inventario |
| Producción | `pos_bridge.PointProductionLine` | Importación nativa Point | Producción/detalle/sucursal | Sin claves de producción repetidas en el alcance consultado | Generador BOM |
| Consumo calculado | `inventario.MovimientoInventario` | `ConsumoInsumoAutoService` | SHA256 de `BOM\|producción\|insumo`; referencia PROD | 19pares con misma fecha, cantidad y origen,473piezas | Auditoría y costeo |
| Costo mensual | `inventario.ConsumoInsumoMensual` | Auditoría mensual | Periodo/insumo | Abril408real/367teórico; mayo824/412; junio314/294. Costo vigente cero | Presupuesto y rentabilidad |
| Corrección documentada | `MovimientoInventario.trazabilidad.exclusion_costeo` | `corregir_costeo_documentado` | Movimiento/hash/cantidad y autorización | Mecanismo existente, con simulación, huellas y auditoría | Auditoría mensual |

## Alias y equivalencias

| Identidad | Estado | Evidencia y límite | Revisión |
| --- | --- | --- | --- |
| Claves BOM556 trasladadas a493 | Confirmada para19pares | Hash exacto, maestro anterior inactivo, mismo origen/fecha/cantidad | Mauricio autorizó corregir causa y conciliar19pares; sin tocar existencias |
| Cuatro grupos con hash anterior292 | No resuelta | Maestro292 ausente; dos cantidades diferentes | Excluidos de la aplicación |
| Consumos por venta sin receta identificable en referencia | No reconstruible automáticamente | El hash incluye receta, la referencia no | Bloquear fusión; no inventar equivalencia |

## Decisión de diseño

Reutilizar maestro, movimientos, clave BOM y exclusión de costeo existentes.
Normalizar la clave durante la fusión conservando la anterior en trazabilidad.
Rechazar antes de modificar si hay colisión, origen no verificable o exclusión
auditada. Serializar construcción y escritura BOM, fusión y corrección mediante
un bloqueo transaccional PostgreSQL compartido.

Una exclusión por duplicidad identifica el movimiento canónico, su hash, fecha,
referencia, artículo y cantidad íntegra. Restar únicamente el duplicado real;
conservar el consumo teórico ya único. Las correcciones anteriores de captura
siguen corrigiendo ambas magnitudes. No crear tabla, dependencia o migración.

Consumidores: las acciones individuales y grupales de Inventario y Maestros
conservan la transacción y muestran el motivo del bloqueo. El generador BOM,
la auditoría mensual y el comando existente comparten las protecciones.

Procedimiento: `inventario_fuentes_datos --term insumo --term consumo --term fusion`
sobre PostgreSQL16; descubrimiento por grafo y consultas acotadas. La salida
léxica y los originales se conservan en el expediente privado de la tarea.

Límites: no reconstruir recetas históricas con el BOM actual, no cambiar existencias,
no aplicar los cuatro casos292 y no declarar por esto certificado todo el cierre.
