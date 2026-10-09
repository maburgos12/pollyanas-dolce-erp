# Ficha de fuentes — cálculo comercial visible

Fecha y ambiente: 8oct2026; lecturas acotadas producción y PostgreSQL16 aislado55604.

## Necesidad y unidad de análisis

Mostrar la ecuación conocida por producto/sucursal/mes aunque exista contradicción
entre venta comercial y salida de stock. No aprobar el caso ni modificar cantidades.
Separar expediente histórico retirado de la proyección actual plenamente reconstruida.

## Fuentes candidatas

| Concepto | Modelo | Creador | Identidad/ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Ecuación | ProductInventoryAuditCase | InventoryAuditMaterializer | mes/branch/product | Dot2832: inicial0/entrada10/salida2/venta5/merma2/final2 | reporte, JSON, CSV/XLSX/PDF |
| Efecto inventario | source_trace.point_history | AuditStockHistoryService | canónica mismo par/mes | Dot2832: venta4, ambos cortes, cadena sin residuo | lector compartido |
| Publicación | ProductInventoryAuditRun | materializador existente | mes | última reconstrucción exitosa/partial_published | reporte y caché |
| Expediente retirado | issue_codes CASE_MISSING_FROM_REBUILD | materializador existente | par ausente del rebuild | Oreo2067/2422/3402; raw49178 y193 son VELA INDIVIDUAL | conservar caso; no sumar a reporte vigente tras rebuild completo |

## Alias y equivalencias

Vela875 y Galleta Oreo Point875 son distintas: Código875 de la transacción
no es FK del producto875. Raw transferencia FK_articulo1001, nombre VELA;
conversión193 original Alegría/VELA. No nueva equivalencia ni edición de maestros.
Decoración Bollo Patrio sin receta no se excluye por nombre: continúa pendiente.

## Decisión

Reutilizar read_audit_report y campos existentes. Cálculo sólo ante razón exacta
SALES_STOCK_EFFECT_UNVERIFIED, ambos cortes, COMPLETE, unknown[], residuo0,
única comparación sales coherente, siete otros rubros idénticos y ecuación Stock
igual al final Point. Mostrar aritmética comercial, no estado canónico autorizado.
case_balance_status compartido y guard documental/mensual permanecen intactos.
Ausencia de cualquier prueba o contradicción adicional conserva dato pendiente.
Excluir casos retirados únicamente si existe rebuild exitoso y no publicación
parcial; no borrar esos casos ni ocultar faltantes actuales o meses sin rebuild.
En publicación parcial, retirar únicamente expediente sin actividad/cortes propios
cuyas referencias originales prueban accesorio: transferencia con FK producto,
dominio false, nombre/categoría exactos y mismo mes/sucursal; conversión bajo
_documentary_commercial_exclusion ya publicada. Referencia inexistente, otro mes,
nombre contradictorio, Dot fabricado o actividad propia conservan el pendiente.

Fuentes inventariadas con inventario_fuentes_datos: saldo calculado/auditoria
inventario. Evidencia íntegra: pendientes-fabricados-finales-20261008.json y
clasificacion-pendientes-fabricados-20261008.json. GET del reporte nunca Point.
Pruebas de HTML/exportes, lector, agentes y cierre; aceptación visible posterior
obligatoria. Ningún cambio operativo, conteo físico, offset, dependencia o migración.
