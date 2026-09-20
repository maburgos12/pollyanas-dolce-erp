# Expediente documental IMSS por empleado

## Objetivo

Convertir la carga de cédulas SUA en una fuente auditable para costos de personal, presupuesto y rentabilidad. El sistema conservará el archivo original, el total patronal y el detalle útil por trabajador sin duplicar el gasto ni convertir la carga en un repositorio indiscriminado de celdas.

El primer cierre cubre las cédulas de Grupo Empresarial FONSMA, registro patronal `E52-40157-10-0`, de enero a agosto de 2026. La información histórica ya aplicada no se volverá a contabilizar: se enlazará con el expediente y se reconciliará contra sus totales existentes.

## Alcance

Incluye:

- Cédulas SUA mensuales y bimestrales en formato `.xls`.
- PDF EMA y EBA como evidencia asociada, no como una segunda fuente de importes.
- Archivo original, huella SHA-256, registro patronal, periodo, totales y usuario de carga.
- Detalle patronal por trabajador y sus componentes relevantes.
- Cruce por NSS con RRHH y conservación explícita de trabajadores sin cruce.
- Materialización idempotente de los importes por área y sucursal en Presupuesto vs Real.
- Lectura del expediente por rentabilidad y planeación de personal.
- Regularización documentada de enero a julio y aplicación de agosto de 2026.

No incluye todavía:

- Conciliación automática contra movimientos bancarios.
- Cálculo de cuotas a partir de salarios; el SUA sigue siendo la fuente de la obligación.
- La parte obrera o amortizaciones de crédito como costo patronal.
- OCR general para PDF ni almacenamiento de todas las celdas del archivo.

## Alternativas consideradas

1. Mantener únicamente totales mensuales. Es liviano, pero no permite explicar el costo por empleado ni auditar una distribución.
2. Guardar documento y detalle patronal normalizado. Es la opción elegida: agrega pocas decenas de filas por periodo y conserva trazabilidad suficiente.
3. Replicar cada concepto, movimiento y celda del SUA. Aumenta complejidad, superficie de datos sensibles y mantenimiento sin un consumidor actual.

## Modelo de datos

### Expediente de seguridad social

Representa una obligación mensual o bimestral para un registro patronal. Sus campos esenciales son:

- tipo: mensual o bimestral;
- periodo de proceso y meses contables cubiertos;
- registro patronal normalizado y razón social;
- estado: previsualizado, válido, aplicado o con discrepancias;
- total patronal calculable;
- conteo de trabajadores, cruzados y sin cruce;
- usuario y fechas de creación/aplicación.

Una restricción impide dos expedientes aplicados del mismo tipo, registro patronal y periodo. Una corrección crea una nueva revisión; no borra evidencia histórica.

### Documento del expediente

Cada archivo se conserva una sola vez con:

- clase `SUA_XLS`, `EMA_PDF` o `EBA_PDF`;
- nombre original, tamaño, tipo MIME y archivo privado;
- SHA-256 único;
- total visible, cuando aplique;
- metadatos mínimos de validación.

Los PDF se asocian como soporte. Nunca generan importes adicionales ni sustituyen el cálculo del Excel.

### Detalle patronal por trabajador

Cada fila conserva:

- expediente y documento fuente;
- NSS normalizado, empleado enlazado opcionalmente y nombre de origen;
- días y SDI;
- mensual: cuota patronal total;
- bimestral: retiro, cesantía patronal y aportación patronal de vivienda;
- costo patronal total y resultado del cruce;
- área y sucursal usadas al aplicar, como fotografía auditable.

La parte obrera, las amortizaciones de vivienda y los totales de pago permanecen disponibles en el documento, pero no forman parte del costo patronal.

Los modelos quedarán en `reportes`, porque su primer consumidor y la materialización presupuestal viven ahí. Se usarán índices por registro/periodo, expediente/empleado y empleado/periodo. El acceso seguirá los permisos actuales de cédulas y reportes; no se expondrá el NSS en listados generales.

## Flujo de importación

1. El usuario selecciona uno o varios archivos y solicita previsualización.
2. El servidor calcula la huella, identifica el tipo y extrae registro, periodo, filas y totales.
3. El registro patronal se obtiene tanto de una celda adyacente como del valor embebido en la misma celda. Se normaliza para comparar sin guiones.
4. El importador suma todos los movimientos de cada trabajador y no presupone una sola fila de importes.
5. Se valida que la suma del detalle cuadre al centavo con el total patronal del SUA.
6. Se cruzan NSS con empleados. Un NSS duplicado en RRHH o una diferencia de totales bloquea la aplicación. Los NSS inexistentes quedan visibles y no desaparecen del total de control.
7. Al aplicar, una transacción crea o revisa el expediente, guarda documentos y detalle, y materializa presupuesto desde ese detalle.
8. Una huella ya aplicada se reconoce como duplicado idempotente. Una corrección con otra huella exige una revisión nueva y conserva la anterior.

## Materialización y consumidores

La cédula mensual materializa únicamente IMSS patronal en su mes. La cédula bimestral materializa retiro, cesantía patronal e Infonavit patronal 50/50 entre los dos meses, conservando el total al centavo.

La distribución usa el departamento y la sucursal del empleado al momento de aplicar. El área Nómina conserva el total corporativo íntegro como control; las áreas no se vuelven a sumar al control corporativo. Los renglones mantienen `AUTO:SIPARE`, pero su metadata apunta al expediente y documento, no funciona como único registro documental.

Rentabilidad por sucursal consume la materialización por área/sucursal. Planeación de personal consume el total corporativo del expediente y deja de depender de un registro patronal vacío en metadata. El costo individual se consulta desde el detalle del expediente, separado de la nómina y sin modificar `NominaLinea`.

## Históricos y agosto de 2026

La migración de datos no recalcula ni sobreescribe importes históricos. Un comando explícito e idempotente:

1. lee los archivos de enero a julio ya conservados;
2. verifica su SHA-256 y los totales contra `LineaPresupuestoMensual`;
3. crea los expedientes y detalles;
4. enlaza las líneas existentes al expediente;
5. reporta cualquier diferencia sin aplicar cambios parciales.

Para agosto se cargan el SUA mensual, el SUA bimestral, EMA y EBA. La mensual corresponde a agosto; la bimestral corresponde a julio-agosto. La aplicación presupuestal ocurre solo después de que ambos Excel concilien con su detalle. Los PDF quedan asociados como evidencia de la propuesta oficial, aun si su importe propuesto difiere del SUA pagado.

## Errores y seguridad

- Extensión o firma de archivo inválida: rechazo antes de guardar.
- Documento duplicado: respuesta idempotente con enlace al expediente existente.
- Registro o periodo ambiguo: bloqueo con mensaje específico.
- Totales sin cuadrar: expediente en discrepancia, sin materialización.
- NSS duplicado en RRHH: bloqueo; NSS inexistente: aviso y total corporativo conservado.
- Conflicto con una fuente manual u otra fuente automática: no se pisa y se reporta.
- Fallo de almacenamiento o base: rollback de la operación lógica; no se deja un expediente aplicado a medias.

Los archivos se almacenan en el medio privado ya configurado para documentos operativos. Las vistas de expediente exigen los mismos permisos de carga de cédulas; las descargas requieren autorización y quedan sujetas al registro de auditoría existente.

## Interfaz

La pantalla actual conserva previsualización como modo predeterminado. El resumen agrega:

- documentos reconocidos y duplicados;
- registro patronal y meses cubiertos;
- total patronal y conciliación contra el detalle;
- trabajadores cruzados y sin cruce;
- efecto estimado en líneas presupuestales.

Después de aplicar muestra el identificador del expediente y permite consultar el resumen. No presenta NSS completos en la tabla general.

## Pruebas y aceptación

Las pruebas unitarias deben demostrar antes de implementar:

- extracción del registro patronal en ambos diseños observados;
- suma de varios movimientos por trabajador;
- separación de componentes patronales y obreros;
- reparto bimestral exacto al centavo;
- idempotencia por SHA-256 y revisiones corregidas;
- bloqueo por totales discordantes y NSS duplicados;
- conservación de NSS sin cruce en el control corporativo;
- asociación de EMA/EBA sin doble conteo;
- backfill sin alterar montos históricos;
- consumo correcto desde rentabilidad y planeación.

La validación final exige `migrate --check`, `check`, pruebas de `reportes`, previsualización con los cuatro archivos de agosto, comparación de totales y una verificación visible en producción después del despliegue. Antes y después del backfill se registran conteos e importes por periodo. No se modifica información de nómina ni se declara completado hasta verificar el consumidor real.
