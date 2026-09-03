# Rentabilidad: gastos recurrentes y personal por sucursal

Fecha: 2026-09-02. Base: `f025358742adb6eca707c544bc193a0c61f70f92`.
Estado: especificación aprobada en conversación, incluida la corrección de renta, y ejecución autorizada con «procede». Implementación local revisada y verificada históricamente sobre la base `2199ba2f690f303c26e920f8394163d736a98485`. Trasladada para revalidación a `/Users/mauricioburgos/Downloads/codex_worktrees/rentabilidad_gastos_publicacion`, rama `codex/rentabilidad-gastos-publicacion`, base `1bb263b579566ce228cdca79e6a467df61b569b4`. Codex asume revisión y eventual publicación según PR #1247, sin dependencia de Claude; aún sin commit, PR de Rentabilidad ni cambios en producción. Véase la entrega de verificación del mismo día.

## 1. Resultado solicitado y decisiones aprobadas

Rentabilidad debe reunir los gastos operativos recurrentes de cada sucursal: renta, teléfono/internet, sistemas, electricidad, alarmas y costo del personal. Debe mantener los repartos por sucursal y área ya definidos en el ERP.

- Matriz–Ventas conserva su proporcional de renta; no se le carga la renta completa del complejo.
- La nómina procede del ERP, no de los libros de OneDrive.
- Los cargos bimestrales se distribuyen entre los meses cubiertos. El pago mantiene su fecha e importe originales.
- No se modifican nóminas, facturas, pagos, ventas ni porcentajes de reparto originales.
- Una fuente ausente o incompleta no equivale a costo cero ni permite presentar un punto de equilibrio definitivo.

## 2. Arquitectura comprobada

- `rentabilidad/tasks_rentabilidad.py` alimenta `SucursalRentabilidad` con `GastoOperativoMensual` REAL. Busca nómina únicamente en la categoría `NOMINA`.
- `reportes/services_presupuesto_real.py` ya resuelve fuentes por reglas, sucursal y departamento; consulta nóminas cerradas/pagadas según el mes de `fecha_fin`.
- `ReglaFuenteRubro` distingue fuentes canónicas, distribuciones y espejos de control; tiene una huella de origen que evita duplicados. La sucursal efectiva puede heredarse del rubro.
- `LineaPresupuestoMensual` conserva importe real, fuente, versión y metadatos de trazabilidad. Las capturas manuales están protegidas.
- `reportes/services_cedula_imss.py` ya distribuye cargas patronales según RRHH; los conceptos bimestrales se dividen entre sus dos meses. No deben mensualizarse por segunda vez.
- `GastoRecurrente`, sus versiones y `ObligacionGasto` existen; en la consulta de producción de este hilo sus catálogos tenían cero registros. No pueden considerarse una fuente completa por el mero hecho de existir.
- Hay reglas de electricidad compartida de 35%/65%. Son reglas de esa fuente, no porcentajes generales aplicables a rentas u otros servicios.
- El total correcto de la factura de renta del complejo es **$66,210.12 MXN**, coincidente con el pago a la arrendadora. Este es el importe que se usará en Rentabilidad, sin presentar otra cantidad como referencia. Matriz–Ventas mantiene su proporcional existente; no se le asigna el importe completo del complejo ni se modifica la factura original.

## 3. Enfoques evaluados

1. **Reutilizar las fuentes y reglas del ERP con una lectura mensual trazable (elegido).** Mantiene una sola captura y respeta distribución, vigencia y fuente original.
2. Importar nuevamente todo desde Excel. Se descarta como fuente regular: duplicaría nómina y dejaría fuera los meses actualizados únicamente en el ERP.
3. Sumar exclusivamente salidas bancarias en el mes de pago. Se descarta para Rentabilidad: confunde transferencias/apartados con gastos y concentra cargos bimestrales en un solo mes. Los movimientos bancarios se conservan como evidencia del desembolso.

## 4. Contrato del cálculo mensual

Un servicio de lectura en `reportes` devolverá filas normalizadas y totales por sucursal, área, concepto y mes. Cada fila debe identificar:

- origen económico y registro ERP de origen;
- sucursal/área efectiva y regla de distribución aplicada;
- concepto y familia del gasto;
- monto original, base usada por el ERP y monto atribuible al mes;
- meses cubiertos, porcentaje y versión/vigencia de la regla;
- estado de cobertura: completo, parcial o pendiente;
- vínculo al soporte y, cuando exista, al pago sin volver a contabilizarlo.

El servicio será de lectura: no ejecutará consolidaciones, importaciones ni escrituras durante una consulta GET. La tarea de recálculo consumirá el mismo resultado que el detalle visual. No se construirá un segundo catálogo de personas o sucursales.

### Fuentes y precedencia

- Gastos no laborales: registros reales y obligaciones no canceladas con sucursal/centro asignados. Si una obligación enlaza un gasto operativo, ambos representan un solo gasto.
- Repartos: usar `ReglaFuenteRubro` y la sucursal efectiva. Los espejos de control no se suman. Un total ya repartido no recibe nuevamente el porcentaje.
- Reales consolidados/manuales: se admiten únicamente con fuente, periodo y rubro inequívocos; no se suman además del registro que los originó. Una versión original y una revisada no son gastos distintos; se reutiliza la selección de versión del consumidor existente.
- Nómina: las fuentes RRHH y sus conceptos oficiales; no las categorías de nómina importadas de Excel. Sueldo, bonos y prestaciones se cuentan una vez, evitando sumar un total y sus componentes.
- Cargas patronales: usar los datos patronales consolidados por el flujo SIPARE. No sumar descuentos al trabajador como gasto adicional. Si falta la cédula, declarar incompleto el costo del personal, sin inventar un porcentaje.
- Facturas/bancos: evidencia del importe y pago. Una coincidencia solo por monto, texto de proveedor o fecha no basta para asignar sucursal ni declarar conciliación.
- Fuentes sin asignación, discrepancias o reglas incompletas: lista de pendientes con el registro original; nunca distribución automática entre todas las sucursales.

No se extrapolarán recibos históricos ni importes presupuestados como reales. CFE se mostrará dentro de gastos recurrentes, aunque su monto sea variable; no se etiquetará como una cuota contractual constante.

## 5. Mensualización y compatibilidad

Se propone ampliar de forma aditiva la cobertura del gasto operativo y la periodicidad de la versión recurrente, sin cambiar sus importes originales. La implementación necesitará una migración de Reportes revisada antes de aplicarse:

- `GastoOperativoMensual.cobertura_mes_inicio` y `cobertura_mes_fin`: fechas opcionales, ambas nulas o ambas informadas, primer día del mes e inicio no posterior al fin. La migración no rellenará automáticamente cobertura histórica.
- `GastoRecurrenteVersion.periodicidad_meses`: entero con valores admitidos 1 (mensual) y 2 (bimestral), predeterminado 1 para conservar compatibilidad. La vigencia inicial ancla el ciclo. Una obligación bimestral se genera solo en el mes de inicio del ciclo y su gasto enlazado cubre ese mes y el siguiente.
- La validación de cobertura se aplicará en el servicio y formulario existentes. El cálculo no inferirá estos campos a partir del mes de pago.

- Cobertura explícita: primer y último mes de servicio, ambos representados por el primer día del mes. La fecha de pago no establece por sí sola los meses cubiertos.
- Un gasto mensual se asigna una sola vez a su mes. Un cargo que cubre dos meses se distribuye en partes iguales entre esos meses.
- Se usará `Decimal` y se conservará exactamente el total; cualquier centavo residual se asigna al último mes cubierto.
- Primero se identifica el monto atribuible a la sucursal según su regla vigente; después se distribuye entre meses, conservando ambos niveles de conciliación.
- Una regla que cambia dentro del periodo requiere dividir la cobertura por vigencia; no se aplicará retroactivamente el porcentaje actual a todo el periodo.
- Los registros históricos sin cobertura mantienen su periodo registrado. Si son bimestrales conocidos y no se sabe qué meses cubren, se muestran pendientes de asignación temporal, no como mensuales completos.
- No se altera el saldo de la obligación, el calendario de vencimientos, las parcialidades ni la fecha del movimiento bancario.
- No se vuelve a dividir una línea SIPARE u otra fuente que ya represente el costo de un mes.
- Ausencia de cuota nueva no significa que el servicio dejó de existir. La integridad se evalúa contra los conceptos aplicables y su vigencia, no contra la existencia de una sola fila de gasto.

No se agregará un modelo paralelo de pagos ni se modificarán modelos de RRHH. Los consumidores legados mantendrán la lectura de sus importes originales; la nueva lectura mensualizada será explícita para Rentabilidad, sin reescribir retroactivamente líneas presupuestales ajenas a la tarea.

## 6. Consumidores y visualización

- `rentabilidad/tasks_rentabilidad.py`: consumir el resultado mensual en vez de clasificar por fragmentos del nombre de categoría o usar una única categoría de nómina.
- `rentabilidad/views_rentabilidad.py` y sus plantillas: mostrar total mensual recurrente, personal, desglose por concepto y estado de cobertura usando el mismo servicio.
- Mantener separados costo mensual y pago. El detalle debe explicar, por ejemplo, que un recibo cubre dos meses y mostrar el cargo original.
- Conservar el estado «No calculable» cuando falte cobertura relevante; no bastará encontrar nómina para considerar completos todos los gastos fijos.
- El listado por sucursal no mostrará detalles salariales individuales ni ampliará los permisos actuales.
- No introducir gráficas o rediseños ajenos al objetivo. Mantener el diseño y navegación del ERP.
- No recontar renta, nómina o cargas de Producción si el costo de fabricación consumido ya las incorpora. Un cambio al costeo de recetas queda fuera de esta tarea y requiere propuesta separada.

## 7. Alcance de archivos y límites

Permitidos: servicio nuevo y pruebas en `reportes/`; campos aditivos y migración de cobertura/periodicidad en Reportes; tareas, vistas, pruebas y plantillas de `rentabilidad/`; controles mínimos de cobertura en el flujo de gasto existente; esta especificación y el plan correspondiente.

Lectura de dependencias: RRHH, SIPARE, fuentes presupuestales, SAT, movimientos bancarios, costo de fabricación y reglas de asignación. No se escribirán nóminas, ajustes, expedientes, CFDI, pagos, inventario, ventas ni archivos OneDrive. No se modificarán `.env`, puertos del compose del proyecto, autenticación ni permisos.

La carga histórica requiere una previsualización que contraste registro de origen, mes, sucursal, importe y posibles duplicados. No habrá una carga general basada solo en nombres de proveedor. Ninguna fuente con asignación dudosa se corregirá silenciosamente.

## 8. Aceptación y pruebas obligatorias

1. La renta del complejo usa el total correcto de la factura: $66,210.12 MXN. Matriz–Ventas conserva su proporcional existente; el total del complejo no se añade de nuevo a Ventas ni se muestra otra cantidad como referencia.
2. Nómina ERP por sucursal reemplaza la fuente Excel sin sumar ambos importes.
3. Prestaciones y bonos no se duplican entre totales y conceptos; cargas patronales no incluyen retenciones al empleado.
4. Un bimestre con importe impar en centavos conserva el total después del reparto.
5. Pago en un mes distinto no mueve el costo fuera de los meses cubiertos.
6. Obligación, gasto enlazado, factura y pago producen un solo costo económico.
7. Repartos compartidos conservan exactamente el total; los espejos de control no se agregan.
8. Falta de sucursal, cobertura, recibo o cédula se refleja como pendiente y no como cero.
9. Recalcular dos veces produce el mismo resultado; una fuente temporalmente ausente no borra importes previamente validados ni los presenta como actuales sin advertencia.
10. El recálculo y la vista coinciden en todos los totales; consultar la vista no escribe datos.
11. Se respeta la vigencia histórica de asignaciones. Si RRHH no permite reconstruir una sucursal histórica de forma inequívoca, se declara pendiente en vez de usar silenciosamente la ubicación actual.
12. Pruebas de regresión de Rentabilidad, consolidación presupuestal, SIPARE y gastos/compromisos; `check`, migraciones y revisión del diff.
13. Validación posterior en navegador autenticado, detalle por sucursal, permisos, consola y solicitudes pertinentes. Una prueba local no equivale a producción.

## 9. Entrega y siguiente paso

La implementación seguirá el ciclo aislado del repositorio, revisión, PR, despliegue oficial y validación de los consumidores reales. Según el protocolo local, la revisión/decisión de commit, merge y deploy corresponde al responsable final indicado en AGENTS.md; no se hará push directo a main.

Esta especificación no es una migración, carga de datos ni cambio de cálculo. La ejecución autorizada sigue el plan de implementación. No se declara resuelta Rentabilidad mientras la integración no esté desplegada y sus datos completos hayan sido comprobados.
