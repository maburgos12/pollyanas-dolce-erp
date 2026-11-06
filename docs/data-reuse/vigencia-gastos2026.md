# Ficha de fuentes — vigencia de gastos operativos

Fecha y ambiente: 2026-11-05; consultas acotadas de solo lectura en producción,
pruebas en PostgreSQL 16 exclusivo de `reportes-vigencia-gastos2026`.

## Necesidad y unidad de análisis

Una regla de fuente por rubro y mes debe reconocer únicamente el gasto del periodo
para el que está configurada. El consolidador y el desglose deben interpretar
`desde` y `hasta` de la misma manera.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Gasto real | `GastoOperativoMensual` | Captura/importación ERP | PK, categoría, centro y periodo | Point octubre: gasto2382, categoría31 y centro12 | Presupuesto y desglose de rentabilidad |
| Regla vigente | `ReglaFuenteRubro` | Administración | PK y filtros desde/hasta | Reglas16463–16472: exclusivamente octubre2026, reparto total100% | Consolidador y lector de gastos |
| Resultado mensual | `LineaPresupuestoMensual` | Consolidador o captura manual protegida | Rubro, periodo y versión | Rubro769 inactivo: agregado histórico; no reactivarlo | Presupuesto maestro y fondos asignados |
| Evidencia fiscal pagada | `CfdiDescargado` | Importación SAT existente | UUID de ingreso y `IdDocumento` del complemento | Ocho ingresos enero–agosto sin `GastoOperativoMensual.cfdi_soporte` en ninguna categoría; importe89088 | Conciliación documental, sin generar gasto desde el complemento |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| `desde`/`hasta` del consolidador y del lector | Confirmada | Mismos campos del mismo registro; el lector ya usa `_vigente` | Reutilizar el helper existente; no introducir otra interpretación |
| Licencias contratadas y sucursal usuaria histórica | No resuelta | La factura global no identifica el padrón por sucursal | Este cambio no asigna identidades ni distribuye importes nuevos |

## Decisión de diseño

Reutilizar `_vigente` de `services_rentabilidad_gastos` antes de construir el índice
de gastos en `_monto_regla`. Fuera de fechas o con filtro inválido, devolver ausencia
de fuente; conservar el tratamiento existente de capturas manuales y reales previos.
Los límites son inclusivos y se admiten las fechas que ya interpreta el lector.
No crear modelos, tablas, migraciones, categorías ni equivalencias nuevas.

Consumidores afectados: consolidación mensual, comando y tarea programada,
presupuesto maestro y lectores de sus fondos. No cambia la interpretación de las
otras fuentes ni las existencias, nómina, ventas o presupuestos capturados.

Procedimiento: `inventario_fuentes_datos --term gasto --term rubro` sobre PostgreSQL;
grafo de llamadas; consulta de reglas, centros y gastos por las ocho FK fiscales
exactas. Los resultados y originales se conservan en el expediente privado.

Riesgos y pendientes: un real consolidado anteriormente se conserva con advertencia
de fuente sin datos; su conciliación requiere revisar el origen, no borrarlo por
esta corrección. Esta entrega de código no contabiliza las facturas pendientes ni
certifica el cierre anual.
