# Cotizaciones editables y compra realizada

Aprobación: Mauricio autorizó el 9 de septiembre editar cotizaciones con historial y nueva validación de importe, registrar compra realizada con fecha, importe final, pedido y comprobante, y mantener la entrega separada.

Compras edita desde el detalle mediante formulario con datos precargados, motivo obligatorio y control de versión. Cada modificación guarda actor, fecha y valores anteriores/nuevos. No se permite editar después de registrar compra o recepción. Las órdenes existentes permiten corregir importes antes de comprar; proveedor y cantidad se conservan por estar vinculados a la orden. Incrementos de una propuesta seleccionada requieren revisión de DG. Las modificaciones de texto preservan autorizaciones.

Un registro único por artículo documenta la compra con cotización, fecha, importe final positivo, pedido opcional y comprobante. Debe existir cotización autorizada; si no hay orden se genera en la misma transacción. Un importe superior requiere corregir y autorizar previamente la cotización. El estado comprado queda pendiente de entrega. El registro no crea pagos ni altera monto_gastado. Las entregas históricas conservan su flujo.

Se reutilizan permisos y respuestas async del módulo. Cada formulario muestra errores, preserva inputs y vuelve al artículo. Archivos de compra se descargan mediante endpoint autorizado. No se cambian datos reales durante pruebas.

La investigación del presupuesto es de solo lectura: carga PAQUETE_2026_REAL de abril sin actor auditable, y agregado inactivo incluido en total. Corregir el presupuesto y definir asignaciones es un alcance separado.

Validación: PostgreSQL aislado, checks/migraciones, pruebas de permisos, historial, conflictos, reautorización, registro único, entrega separada; navegador local y producción tras CI y despliegue oficial.
