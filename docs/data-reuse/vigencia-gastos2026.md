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


## Lectura del cierre de septiembre de 2026

La revisión posterior de las capturas históricas de renta identificó que el
lector mensual habilitaba el reparto consolidado solamente hasta agosto, aunque
el cierre conciliado y el Estado de resultados cubren enero–septiembre. La renta
compartida ya existe como gasto2121, obligación704 y contrato112; las partidas12857
y19905 contienen sus dos destinos guardados. La pantalla de gastos debe reutilizar
ese mismo desglose del mes, conservando el origen y el total.

Se extiende exclusivamente hasta septiembre el uso existente de
`usar_reparto_consolidado`. El lector conserva sus validaciones de sumas, tipos,
reglas y bandera `sin_datos_fuente`; octubre y otros años siguen con el criterio
anterior. No se crean gastos, repartos, equivalencias, contratos ni pagos.

Consumidores: `leer_costos_mensuales`, panel y detalle de rentabilidad, recálculo
mensual y lectores de sus resultados. La regresión usa un recibo compartido con
centavos, comprueba la participación de venta y producción, el total conservado,
la ausencia de escrituras y que octubre no adopte el reparto histórico.

Verificación de fuentes: `inventario_fuentes_datos --term renta` sobre PostgreSQL16
aislado de `reportes-etapa-sinaloa2026`, grafo de llamadas y lecturas acotadas de
producción. Los originales y las exclusiones de capturas históricas se conservan
en el expediente privado; no se usa una constante manual como prueba de importe.
