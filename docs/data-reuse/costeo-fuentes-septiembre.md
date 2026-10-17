# Ficha de fuentes — corrección de lectura de septiembre

Fecha y ambiente consultado: consulta actual en VPS 68.183.165.47, PostgreSQL de producción. Sólo lectura con SET TRANSACTION READ ONLY.

## Necesidad y unidad de análisis
Costeo gerencial por producto/sucursal y desembolsos de septiembre. Mauricio confirma incluir pagos realizados en septiembre aunque correspondan a agosto. Mantener periodo de cobertura distinto del periodo de pago. Luis Daniel pertenece a supervisión de ventas, no a logística por conducir ocasionalmente. Dos domicilios excluidos del alcance.

## Fuentes candidatas
| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Carga por destino | RutaCargaChecklistLinea | Logística y transferencias Point | PK, point_transfer_line, erp_destination_branch, checklist/ruta | Septiembre: 49 rutas, 243 paradas, 5048 líneas vigentes; todas con destino y transferencia | Costeo etapas; logística |
| Patronales | ExpedienteCedulaIMSS / DetalleCedulaIMSS / DocumentoCedulaIMSS | Importación SUA y aplicación | expedientes1/2 agosto; detalle empleado/área/sucursal | Mensual79931.51 (72/72 cruzados); bimestral185026.19 (76/76). Documentos XLS y EMA/EBA guardados | Reportes, rentabilidad |
| ISN | ExpedienteISN | CFDI y aplicación ISN | expediente1 agosto, CFDI FK | APLICADO, importe_pagado16168.00,71empleados | Reportes fiscales |
| Servicios | GastoOperativoMensual / LineaPresupuestoMensual | Captura/importación y consolidación de reglas | periodo, categoría, centro, rubro, versión ORIGINAL | Agua Matriz1075.05; luz Matriz13721.75; luz Bamoa10816.00; teléfono Bamoa549.00; todos existentes en septiembre | Presupuesto real, rentabilidad |
| Rentas y fumigación | GastoOperativoMensual / ObligacionGasto | Captura/importación y recurrentes | obligación/gasto FK y centro | Septiembre contiene importes reales; fumigación de sucursales3969.00. No sumar rentas en bruto: hay representación por centros y reglas | Presupuesto real, rentabilidad |

## Alias y equivalencias
- Receta existente y costo vendido reconocido son conceptos distintos: el lector actual sólo enlaza recetas presentes en producción del mes.
- Periodo agosto de cédulas/ISN y pago septiembre: Dirección confirma el criterio; preservar esta procedencia, no reescribir periodo fiscal.
- Conductor ocasional y puesto repartidor: distintos. Luis Daniel continúa en ventas por confirmación de Dirección.
- Las FK carga-transferencia-sucursal están confirmadas; no requieren emparejamiento por nombre.

## Decisión de diseño
Reutilizar fuentes actuales. Corregir lector de costeo en lugar de crear capturas. Separar desembolso y cobertura; usar detalle patronal sin volver a sumar componentes incluidos en cuota_patronal. No tratar IVA/retenciones automáticamente como gasto adicional a nómina. Completar reglas de asignación logística con destinos y carga, no porcentaje global de ventas. No aplicar cambios de clasificación a RRHH.

Consultas reproducibles: inventario_fuentes_datos --term pago --term patronal --term ruta; consultas ORM acotadas septiembre de2026 y expedientes agosto. leer_gastos_mensuales filtra cobertura y clasificación; leer_personal_mensual exige cédula del periodo solicitado. La integración de etapas no consume detalle de carga por destino.

Riesgos y pendientes: PagoObligacionGasto y MovimientoBancario no contienen filas septiembre en las consultas realizadas; esto no prueba ausencia de pagos en otras fuentes. Confirmación de Dirección del mes pagado se distingue de comprobación bancaria. No se verificaron aún todos los impuestos ni recibos de cada sucursal. No hay cambios de código o datos en esta revisión; no se crearon recursos Docker.

## Implementación de la lectura

- El periodo de pago de agosto a septiembre se limita al corte confirmado por Dirección: septiembre2026. No infiere pago ni desplaza expedientes en otros meses. No modifica el periodo fiscal ni registros operativos.
- Las cédulas aplicadas deben pertenecer al registro patronal autorizado, contener un SUA único y conciliar su detalle con el total. El ISN aplicado debe conservar CFDI vigente y detalle que concilie con el importe pagado. Se sustituye, no se suma, la carga por cobertura del lector previo.
- La distribución fiscal se toma del empleado y centro del detalle. Personal de elaboración y despacho se separa con las personas ya identificadas en el reporte; los repartidores conservan su fondo logístico. Administración se muestra separada, sin inventar una regla para repartirla.
- El lector económico puede aceptar un reparto mensual documentado en el consolidado ORIGINAL si su desglose concilia exactamente, identifica la regla vigente y no está marcado sin_datos_fuente. La opción se usa sólo en costeo; los consumidores existentes conservan su política de vigencia. El consolidado es evidencia del reparto, no un gasto adicional.
- Fumigación (INDIRECTO_SUC) y seguros de establecimiento reutilizan la familia otros. Se conserva el vínculo obligación/gasto para evitar doble suma.
- Las recetas mensuales completas que no tienen producción final registrada en el mes usan la misma tasa de elaboración, servicios y adicionales por MP. No se declara FIFO ni equivalencia de cantidades producidas y vendidas.
- Las ventas conservan código Point o FK inequívoca cuando existe. Dos códigos agrupados bajo el mismo nombre no producen una identidad inventada. La reventa usa su costo histórico mensual, sin elaboración.
- Los lectores, plantilla y Excel consumen el mismo resultado. El cierre aprobado de mano de obra original no se modifica: el costeo ampliado sigue siendo análisis gerencial, no contabilización.

Validación de fuentes mediante cálculo temporal en memoria dentro de transacción READ ONLY del VPS, sin sustituir archivos servidos: cargas281125.70; producción adicional139848.61 (renta43036.58 y cargas96812.03); cargas de logística28044.92; gasto de venta de nueve sucursales583094.61. La suma ISN distribuido es16168.00, incluido su ajuste ya realizado en el ERP. No volver a redondear ni redistribuir ese importe.

Límites explícitos: la atribución de logística por destino/producto aún necesita una base homogénea, pues la carga contiene piezas, kilos, litros y cajas. Se conserva la atribución a rutas existente y el remanente identificado; no se convierte un faltante de asignación en ausencia de registros. Los servicios comerciales y adicionales sin receta/costo mensual no reciben costo inventado.

ISN almacena departamento de nómina (VENTAS, PRODUCCION, ADMINISTRACION, RRHH), no el código de área presupuestal de las cédulas. Se reutiliza DEPARTAMENTO_A_AREA y AREA_RESPALDO del importador fiscal existente; no se crea una nueva equivalencia. Totales de cargas por área: producción96812.03, logística28044.92, gastos de venta109283.27, administración46985.48; remanente sin clasificar0.
