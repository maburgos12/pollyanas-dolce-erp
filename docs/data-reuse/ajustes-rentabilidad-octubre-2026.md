# Ficha de fuentes — ajustes verificados de octubre 2026

Consulta: PostgreSQL16 de producción, 19/11/2026, snapshot `69772567:69772567:`,
revisión `2d4cb1c63ae9b542e3a53deb756a8414e70f2e80`. Aplicación autorizada por
Mauricio en el chat financiero; esta ficha no declara aplicado el ajuste.

## Necesidad y unidad de análisis

Reconocer cinco destinos de presupuesto ORIGINAL de octubre y actualizar el
resultado empresarial del mismo mes. No capturar de nuevo compras, pagos o nóminas.

## Fuentes candidatas

| Concepto | Modelo / lector | Identificadores y evidencia | Productor y consumidores |
| --- | --- | --- | --- |
| COEPRIS | GastoOperativoMensual / leer_gastos_mensuales | Gastos2419–2422; CFDI1993/2002/2047/2207; regla16542; línea20404; cobertura anual existente; octubre586.64 | Gastos y regla existentes → PresupuestoReal y Rentabilidad |
| Licencias | ObligacionGasto y GastoOperativoMensual | Gastos2130/2131; CFDI794/8884; reglas16350/16351; líneas20532/20522; octubre1224.76/250.36 | Obligaciones existentes → mismo lector mensual |
| RRHH | NominaLinea y NominaConceptoLinea | Líneas1409/1476, periodos22/24, empleado33; destino_personal=administracion; sueldo11039.76, premios2200, extras363.15; percepciones13602.91; despensa32 adicional1420 | Nómina cerrada/pagada → rubros738/742, líneas8826/8874 |
| Resultado y BI | leer_costos_mensuales, registros_del_mes, EmpresaResultadoMensual | EmpresaResultadoMensual ID10, periodo2026-10-01; ventas netas3648207.58, material1566669.92, reventa50191.39, merma11656.98 | Lector mensual → actualización acotada del snapshot, BI/API/export existentes |

## Alias y equivalencias

Las identidades de gastos, reglas y documentos son existentes. No se introducen
equivalencias Point ni cambios de empleado. Dirección autorizó reconocer las
percepciones RRHH en administración y la despensa separada en su rubro existente.
Los premios y horas extra ya forman parte de percepciones: no se vuelven a sumar.
Telcel4113.95 y GPS290 ya están en centrales; no se incluyen en el delta.

## Decisión de diseño

Reutilizar el lector mensual con un filtro optativo de cobertura documentada
exclusivo `2026-10` en las tres reglas verificadas. El lector normal identifica
también esas fuentes para retirar avisos falsos; administración/producción siguen
fuera de los gastos directos de sucursal, sin volver a sumar centrales. Añadir
dos reglas nominales disjuntas, vigentes solo
octubre2026. Extender la consolidación con filtro opcional de rubros: una lista
vacía procesa cero rubros. Se preservan capturas manuales y reglas restantes.

El comando `aplicar_ajustes_rentabilidad_octubre_2026` comprueba las fuentes y
precondiciones; sin `--apply` no escribe. Al aplicar exige `--backup-file` externo
o en el almacén privado ya ignorado `storage/private/conciliaciones`. Escribe con
permisos0600, sincroniza a disco y verifica por round-trip el contenido completo de
cinco líneas, tres reglas, `EmpresaResultadoMensual:10` y su consumidor existente
`PresupuestoResumenMensual:10`. Son modelos distintos, identificados por periodo y
fuente. El consumidor conserva todos sus presupuestos y conceptos; actualiza
exclusivamente reales/variancias. Antes del primer lector bloquea también los
seis gastos/CFDI, obligaciones/pagos y nómina/conceptos con sus padres pertinentes:
no permite intercalar modificaciones ni altas hijas. Conserva la huella completa
de fuentes y destinos y la exige también cuando informa YA_APLICADO. Pruebas con
dos sesiones acreditan bloqueo de UPDATE e INSERT de conceptos (SQLSTATE55P03).
Comprueba hashes de las demás líneas, conserva auditoría e impide una segunda
aplicación. Usa READ COMMITTED y bloquea periodos, filas de nómina y empleados antes de
derivar identidades y leer conceptos; vuelve a comprobar los IDs derivados.
Una inserción confirmada antes del bloqueo se ve y obliga a abortar por composición
distinta; tras el bloqueo, UPDATE e INSERT no pueden intercalarse. Una discrepancia
revierte todo.
No refresca costos por producto,
inventarios, tareas, pricing o contribuciones históricas.

La aplicación produce además un recibo `.after.json` con el hash del respaldo,
estado posterior completo e IDs de las dos reglas creadas. El modo `--restore-file`
comprueba ambos archivos y las fuentes; rechaza cambios posteriores incluso de
igual importe. Sin `--apply` solo verifica la reversión. Con `--apply` restaura
exactamente los registros anteriores, retira únicamente las dos reglas creadas y
conserva una nueva auditoría. La restitución desde archivo y el rollback ante
fallo después de consolidar tienen pruebas independientes. El respaldo nunca se
elimina automáticamente y no se sirve desde media pública.

El snapshot mantiene merma separada en metadata y la misma composición del lector
actual; la fabricación conserva su definición material+mano de obra+indirectos+
empaque, sin volver a restarla del resultado ya calculado. Conserva metadata
previa de cobertura/cálculos como antecedente, fuera de las claves que acreditan
el corte actual, e informa parcialidad, fuente, fecha y cantidades pendientes.
BI muestra1628518.29 de costo producto/merma con porcentaje de ese mismo total,
detalle1566669.92+50191.39+11656.98 y margen2019689.29. API de gráficas usa la
merma financiera del snapshot exclusivamente para el corte autorizado, manteniendo
las capturas operativas originales y su procedencia por periodo. API/export de
Presupuesto vs Real comparten el margen registrado. Los demás meses conservan su
contrato anterior; ningún GET ejecuta el costeo. La proyección de octubre conserva
la venta neta del snapshot frente a ventas comerciales distintas. Merma requiere
importe finito no negativo y conciliación exacta con el margen; una marca con
componentes inválidos no acredita importes. Presupuesto vs Real rechaza ese
snapshot antes de calcular o persistir; API y pantalla informan conflicto409,
y el comando informa CommandError sin sobrescribir el consumidor. La cobertura de venta costeada/no receta
no se infiere del antecedente: octubre y su acumulado muestran N/D. La nota YTD
identifica que solo la merma de octubre fue acreditada por este ajuste.

El commit invalida únicamente el scope de caché `dashboard` mediante el helper
existente; preview, rollback y reintento idempotente no invalidan. Restaurar una
aplicación sí invalida tras su commit. Las tres referencias del SW usanv13 sobre
la base8114673e, evitando dejar HTML financiero viejo en PWA. Los avisos
RRHH se retiran solo si la nómina coincide con el desglose central reconocido.

## Verificación y límites

Inventario de fuentes local con PostgreSQL: `inventario_fuentes_datos --term nomina
--term cobertura --term cedula`; es descubrimiento léxico, no equivalencia.
Evidencia monetaria reproducible: preview financiero único en el expediente externo
`rentabilidad-74-costos-20261119/preview-unico-rrhh-puentes-readonly.json`.

Delta aprobado17084.67; centrales600079.66→617164.33; resultado empresarial
parcial864624.87→847540.20. El snapshot anterior1285478.39 además arrastraba
fuentes anteriores: la diferencia total reconciliada es437938.19.

CEDIS10 conserva centro25 tipoSUCURSAL_VENTA en ERP: no se excluye por nombre ni
se inventa nómina de ventas. El comprobante bancario de SIPARE883001 acredita
pago301320.90 del17/11 para202610; no separa parte obrera/patronal ni distribución.
No incorporar el total recaudatorio como gasto ni duplicar obligaciones de nómina.
El cierre patronal permanece sujeto a sus cédulas y conciliación por periodo.
