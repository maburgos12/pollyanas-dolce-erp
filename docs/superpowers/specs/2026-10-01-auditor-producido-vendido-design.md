# Conciliación compartida: auditor y Producido vs Vendido

Fecha: 2026-10-01. Diseño conversacional aprobado por Mauricio; documento pendiente de revisión antes del plan de implementación.

## Resultado esperado

El auditor investiga cada mes por producto y sucursal con las fuentes existentes. Producido vs Vendido muestra la misma conciliación, sus pendientes y su fecha de actualización. Abrir el reporte no descarga Point ni reconstruye meses en la petición web.

Se conserva el inventario inicial del cierre del último día del mes anterior y el final del último día del mes auditado, en America/Mazatlan. El envío de madrugada pertenece al día operativo que terminó.

## Evidencia y problema actual

La revisión de producción de septiembre encontró 1,958 expedientes: 1,259 conciliados, 220 pendientes de explicación y 479 con fuente incompleta. Entre los pendientes hay 127 diferencias numéricas: 76 sin historial guardado suficiente y 51 cuyo historial no explica el saldo. La revisión de esos 127 no encontró casos elegibles para conciliación automática. Estos conteos son una fotografía, no constantes del sistema.

InventoryAuditMaterializer ya ejecuta InventoryAuditAgent después del commit. Producido vs Vendido usa MonthlyPointProductBalanceService por otra vía; su encabezado puede decir «Conciliación Point completa» mientras las filas dicen «Revisar fuente». No hay tarea periódica propia del auditor. El cierre mensual de Point está desactivado y su implementación refresca fuentes: no se activará como atajo para este trabajo.

## Fuente compartida y trazabilidad

Reutilizar ProductInventoryAuditRun, ProductInventoryAuditCase y ProductInventoryAuditEvent, los servicios actuales, los movimientos Point y las relaciones existentes de Logística. No crear tablas maestras, otra captura ni un segundo cálculo de conciliación en la vista.

La unidad de auditoría es mes, identidad Point del producto y sucursal ERP canónica. Reutilizar únicamente equivalencias vigentes y comprobadas; conservar como pendientes las identidades ambiguas. No asignar rebanadas al Chico o Mediano si Point no identifica el origen.

Saldo esperado = inicial + producción real + transferencias de entrada + conversiones de entrada + ajustes identificados − ventas − merma − transferencias de salida − conversiones de salida. Incorporar retornos según la evidencia y reglas existentes sin duplicarlos contra transferencias o ajustes. Nunca suponer que una entrega no recibida regresó al origen sin un movimiento acreditado.

El historial es evidencia de reconciliación, no cantidades adicionales para sumar sobre los movimientos ya contabilizados. Cada componente conserva identificadores y cobertura. Una fuente incompleta no equivale a cero; una diferencia cero no resuelve una discrepancia operativa abierta. Conteos físicos y futuros cierres manuales permanecen como evidencia separada del cierre Point.

## Actualización automática

Extender los disparadores de los flujos existentes que guardan fuentes relevantes: ventas, producción, merma, conversiones, transferencias/carga/recepción/retorno, ajustes, inventarios de cierre e historial. Encolar después del commit y procesar sólo los meses afectados, en segundo plano. Reutilizar exclusión mutua y fingerprints existentes para evitar carreras, notificaciones repetidas y trabajo duplicado.

Agregar una revisión diaria de respaldo a las 04:15 de America/Mazatlan: mes actual, anterior y meses ya auditados con pendientes. Comprobar vigencia antes de reconstruir; no descargar toda la historia ni recorrer meses sin cambios. Un mes histórico que recibe nueva evidencia entra por su disparador. Los meses formalmente bloqueados no se reabren ni se modifican automáticamente.

Si una sincronización sigue activa o falla, conservar el último resultado válido y mostrar que está pendiente de actualizar. El respaldo diario recupera disparadores omitidos. La ejecución sólo reutiliza datos guardados; las fuentes faltantes siguen pendientes y no generan una descarga nueva de Point desde este proceso.

Registrar todas las diferencias. Mantener las reglas existentes de prioridad y notificación: asignar/notificar relevantes, recurrentes o de alto riesgo; agrupar menores. Si falta responsable válido, conservar el motivo, sin asignación arbitraria. Nunca autorizar pérdidas, ajustar inventario, inventar merma ni bloquear el cierre mensual sin intervención humana.

## Producido vs Vendido

Conservar la ruta, los filtros actuales, costos y exportaciones. Añadir sucursal y última actualización; consumir los expedientes guardados para saldos y estados. Mantener los enlaces producto/receta comprobados. No excluir silenciosamente productos sin equivalencia; mostrarlos como pendientes de identificación en el detalle.

Estados visibles: «Conciliado», «Pendiente de conciliar» y «Falta información», derivados del mismo expediente que el auditor. En la vista global, un producto con sucursales pendientes no puede quedar conciliado porque sus diferencias se compensen. Los componentes incompletos no generan un total presentado como comprobado.

Sustituir los múltiples badges técnicos y explicaciones extensas por un resumen breve. Concentrar fuentes, movimientos y próximos pasos en el detalle por sucursal. El encabezado no afirma conciliación completa mientras existan pendientes en el alcance seleccionado. Mantener encabezados fijos, alineación y navegación por teclado.

HTML, JSON, CSV, XLSX y PDF deben usar los mismos filtros, cantidades y estados. Mostrar resultados persistidos en la carga inicial, sin trabajo pesado en GET. Al completarse una actualización, la pantalla abierta podrá comprobar la fecha y renovar su información conservando filtros y posición, sin descargas Point ni sondeo agresivo. Si no existe una auditoría del mes seleccionado, mostrar «Aún no auditado», no valores cero ni conciliación completa.

## Validación y entrega

Pruebas PostgreSQL de conciliación compartida, sucursales que se compensan, conversión sin origen, fuente faltante, evento tras commit/rollback, duplicación de eventos, sincronización activa, reejecución sin cambios y conservación de resoluciones humanas/meses bloqueados. Verificar también cambio de mes, sucursal, exportaciones y actualización de una pantalla abierta.

Comparar septiembre antes/después sin modificar movimientos operativos. Validar check y migraciones; revisar diff, PR, merge y despliegue oficial. Confirmar en producción el job periódico, una ejecución idempotente y el reporte autenticado con consola y peticiones de red. No declarar septiembre cerrado mientras falte evidencia.

## Alcance excluido

No agregar un agente o plataforma nuevos, aprendizaje estadístico, equivalencias nuevas, captura manual duplicada ni descarga masiva. No modificar configuración de Point, inventarios, ventas, nómina ni responsables maestros. Resolver las fuentes faltantes requerirá evidencia adicional; publicar este cambio no demuestra por sí mismo dónde quedó el producto.
