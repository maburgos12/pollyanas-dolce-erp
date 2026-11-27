# Ficha de fuentes — cargas de combustible sin turno

Fecha: 27/11/2026. Producción consultada en PostgreSQL, sólo lectura, 27/11.

## Necesidad y unidad de análisis
Una carga física por unidad y responsable, con ticket, fecha de consumo y autor de captura. La petición de DG autoriza registrar la nota de Cheyenne y habilitar cargas sin turno. No se crea un recorrido ni se infiere kilometraje.

## Fuentes y evidencia
| Concepto | Fuente oficial | Identificador | Evidencia | Consumidores |
| --- | --- | --- | --- | --- |
| Unidad | logistica.Unidad | PK y código | id 5, GS-CH1, Chevrolet Cheyenne, UL-71-533 | bitácoras, combustible, presupuesto |
| Responsable | logistica.Repartidor → auth.User | PK | repartidor 13 → usuario 3 carolina.cayetano | captura y auditoría |
| Carga | logistica.CargaCombustibleUnidad | PK | ninguna carga $500 desde 25/11; última Cheyenne #182 del 02/11 | presupuesto, conciliación, costeo, API |
| Turno | logistica.BitacoraSalidaLlegada | PK | ninguna Cheyenne desde 24/11, ninguna de Carolina | recorridos |
| Soporte | ticket original suministrado por DG | SHA256, estación + folio | folio 3003609, Servicio Carranza, 26/11/2026 13:11, 21.277 L, $500 | comprobación documental |

Alias: Cheyenne → GS-CH1 confirmada por descripción única. Carolina → repartidor 13 confirmado por usuario operativo exacto; la otra cuenta de producción no se fusiona. Usuario ratificó responsable en este chat.

Procedimiento: consultas acotadas por descripción de unidad, nombre/cuenta de responsable, fecha y monto. Inventario de fuentes ejecutado en PostgreSQL aislado antes de implementar; evidencia externa de tarea.

## Decisión
Extender la tabla existente: bitácora opcional, fecha_carga nullable (legacy usa fecha_registro), autor nullable para históricos, estación/folio/notas. Litros conserva tres decimales; no redondear el original. Permisos de DG/Administración con gestión de Bitácoras, sin ampliar roles. La PWA sigue exigiendo turno. Captura administrativa protege duplicados por hash y estación/folio; conserva audit log y foto. Reportes usan fecha de consumo para estas nuevas cargas y conservan fallback histórico. Costeo deja cargas sin turno pendientes, sin inventar reparto a rutas.

Límites: ticket CONTADO no demuestra pago bancario; captura no equivale a conciliación fiscal. La auditoría automática permanece separada de la lectura manual y de las notas.
