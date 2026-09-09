# Proveedor nuevo y compras en línea en cotizaciones

Diseño aprobado por Mauricio con «Procede», después de la propuesta de alta dentro del formulario y plataforma, vendedor y enlace.

El proveedor sigue siendo maestros.Proveedor: se selecciona uno activo o se abre un alta explícita junto al selector. Guardar el alta selecciona el nuevo proveedor y conserva cantidades, importes y adjuntos de la cotización. La respuesta usa data-async-action y reemplaza solamente el bloque del proveedor. Sin JavaScript vuelve al mismo artículo mediante fragmento. Duplicados normalizados e inactivos se rechazan con explicación; cada alta se audita.

En cada cotización se añade plataforma (compra directa, Amazon, Mercado Libre u otra tienda en línea) y enlace HTTP(S) al producto. El vendedor es el proveedor elegido, no un segundo catálogo. Una compra en línea requiere enlace. El detalle muestra plataforma y enlace junto al proveedor. No se consulta automáticamente el sitio ni se inventan datos fiscales.

Dos campos opcionales nuevos preservan cotizaciones históricas; no cambian presupuesto, autorización, generación de órdenes ni permisos existentes. El backend valida cantidades e importes, conserva errores y guarda la cotización/selección en una transacción. El formulario ofrece guardar para comparar sin seleccionar, además de la selección existente.

Aceptación: alta auditable sin perder captura; rechazo de duplicados e inactivos; cotización directa y online; URL insegura rechazada; permisos conservados; detalle y totales coherentes; navegación local escritorio/móvil, consola/red, CI y despliegue oficial con verificación de producción. No crear proveedores ni cotizaciones ficticios en producción.
