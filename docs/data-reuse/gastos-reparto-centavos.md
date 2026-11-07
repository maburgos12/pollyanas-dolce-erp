# Ficha de fuentes — centavos del gasto distribuido

6 noviembre 2026; PostgreSQL16 aislado, contraste de solo lectura en producción.

## Necesidad y unidad
Un gasto documentado por centro, categoría y mes. Su reparto completo debe sumar el importe original sin crear otro gasto ni cambiar porcentajes.

| Concepto | Fuente existente | Identidad y evidencia | Consumidores |
| --- | --- | --- | --- |
| Gasto real | `GastoOperativoMensual` | PK del comprobante, categoría y centro. Octubre Telmex2392/2393, más cargo115 acreditado en aviso | Presupuesto real, planeación, rentabilidad, exportaciones |
| Destinos | `ReglaFuenteRubro` | Fuente GASTO_OPERATIVO, DISTRIBUCION, categoría/centro y vigencia; nueve reglas de Telmex | Mismos consumidores |
| Espejo | `LineaPresupuestoMensual` | Rubro, mes, versión; real_breakdown conserva regla e importe | Presupuesto y P&L |
| Redondeo vigente | `services_rentabilidad_gastos._repartir` | Orden determinista por PK; residuo al último destino sólo si porcentajes suman100; rechaza solapamientos | Lector de gastos; reutilizado por consolidación |

## Equivalencias y decisión
No se crea tabla, identidad, captura ni alias. `DISTRIBUCION` mantiene su significado y vigencia actuales. El presupuesto redondeaba el agregado de gastos por regla; tres documentos4679.06+549.01+115 distribuidos entre nueve destinos podían diferir del lector por centavos. Reutilizar el mismo reparto por documento elimina esa diferencia. Las reglas canónicas, de control y manuales siguen su ruta; una distribución parcial no se completa artificialmente.

## Alcance y verificación
Cambio exclusivo del evaluador GASTO_OPERATIVO/DISTRIBUCION. La selección conserva la exclusión existente de gastos con obligación exacta; no escribe fuentes, porcentajes, nómina, bancos ni pagos. Prueba de nueve destinos compara cada LP con el lector, el total5343.07 y reejecución; pruebas de reparto parcial y vigencia/otro centro. Contrastar enero–octubre antes/después por SHA y aplicar sólo espejos autorizados, sin consolidación masiva.

Inventario de metadatos y registros reales: expediente local cfe-abigail-telecom-cierre/fuentes-actuales.json y destinos-actuales.json. Los candidatos léxicos no prueban identidad. La ficha de la integración documental permanece en ese expediente; no forma parte del cambio de código.
