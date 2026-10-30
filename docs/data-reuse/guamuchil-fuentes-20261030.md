# Ficha de fuentes — conciliación Guamúchil

Fecha y ambiente: 30 octubre 2026; PostgreSQL local aislado y consultas acotadas de producción.

## Necesidad y unidad de análisis

Costo por sucursal, concepto y mes. Una factura no es un pago ni una medición mensual de consumo. Una obligación y su gasto enlazado representan el mismo costo.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Electricidad | sat_client.CfdiDescargado; reportes.GastoOperativoMensual | SAT y importar_recibos_cfe | UUID; external_key CFE:UUID:mes; centro | CFDI 9927, UUID 21FB90AD-A533-4DA9-B490-FF660B7CB180, 19428.09, recibo servicio 545230101121 | Reportes y Rentabilidad |
| Renta | reportes.GastoRecurrente, ObligacionGasto, GastoOperativoMensual | Maestro y capturas existentes | Contrato 31, versión 129; obligación.gasto_operativo | Obligaciones 802–810 enlazadas a originales; renta 9367.50 | Maestro, generación masiva, Rentabilidad |
| Nómina | rrhh.NominaLinea, Empleado | Nómina cerrada/pagada | Empleado y periodo; sucursal_snapshot, departamento_snapshot | Personal Guamúchil identificado previamente; no modificar RRHH | Rentabilidad |
| Cargas | reportes.LineaPresupuestoMensual, ExpedienteCedulaIMSS | SIPARE o captura documentada existente | Rubro, periodo, versión ORIGINAL; expediente y patronal | Septiembre líneas 9845/9857 sin importe; último expediente aplicado agosto. Archivo administrativo de septiembre VENTAS AB213=5568.12, AB214=3567.96 | Reportes y Rentabilidad |

## Alias y equivalencias

| Términos | Estado | Evidencia y caso contrario | Revisión |
| --- | --- | --- | --- |
| Servicio 545230101121 y CFDI septiembre | Confirmada documentalmente | UUID completo en recibo y SAT, importe coincide; no usar saldo previo como nuevo gasto | No hay nueva equivalencia de maestros |
| IMSS del Excel y cuota patronal certificada | No equivalente demostrado | Excel administrativo no sustituye cédula SUA; conservar origen manual y estado parcial | No etiquetar AUTO:SIPARE |
| Misma categoría/centro/mes | Candidata, nunca identidad automática | Puede haber gastos distintos; bloquear generación recurrente ante captura real sin vínculo, sin fusionar ni borrar | Conciliación documental |

## Decisión

Reutilizar tablas y consumidores actuales. No crear maestro ni captura paralela. Proteger generación recurrente en el servicio compartido. Usar snapshot de nómina existente para no trasladar historia cuando cambia RRHH. Importación CFE exige UUID ante importes ambiguos y guarda soporte; una porción mensual no conserva cobertura del bimestre completo. Los prorrateos estimados siguen siendo estimados y no se convierten en consumo real.

SIPARE ya existe: la ausencia de cargas debe reducir cobertura. Conservar un total histórico cuando falte un componente no debe ocultar un total conocido mayor; usar el mayor sin sumarlos y mantener señal de incompleto.

Consultas reproducibles: inventario_fuentes_datos --term electricidad; --term gasto --term nomina --term cedula --term cfdi, PostgreSQL configurado. El inventario es léxico; identificación confirmada por modelos y consultas de registros limitadas a Guamúchil.

Riesgos: facturación eléctrica no equivale a consumo mensual; Excel administrativo no acredita exclusivamente cuota patronal. No modificar nómina, identidad, sueldos ni cédulas para aparentar cobertura.

## Comprobación adicional de venta neta

PointDailySale septiembre sucursal 6: 778 registros; gross_amount 142030.00, descuento 295.98, impuestos 4554.72, net_amount 137179.30. El snapshot omitía restar impuestos. El cálculo compartido usa gross_amount menos tax_amount antes de descontar; no altera las ventas originales. Recalcular solo el registro derivado 94 al aplicar esta conciliación.
