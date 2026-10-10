# Seguimiento confirmado de reportes existentes

## Contrato

El mismo runtime ofrece búsquedas y fichas de reportes de Fallas (también instalaciones), y prepara incident.followup. El modelo usa tool calling; no se introducen clasificadores por palabras clave. El servidor vuelve a consultar usuario activo, participante, flags existentes, permisos de gestión, visibilidad de costos y sucursal. No se incorporan usuarios ni se incrementan los límites de gasto.

La herramienta admite report_id, comentario, estatus en_proceso/resuelto, fecha_trabajo_finalizado, evidence_count y referencias de continuación. Rechaza proveedor, importes, pagos, usuario y confirmación. Resolver requiere fecha real entre la creación del reporte y hoy. Reportes finalizados, principales con duplicados o duplicados requieren revisión fuera de este corte.

La propuesta vive en ChatToolCall, vinculada a conversación y propietario; conserva contexto y versión. No modifica el reporte. Su vigencia es 24 horas; cambios de estado, presupuesto, proveedor o bitácora invalidan su ejecución hasta una nueva revisión.

## Fotografías y confirmación

La interfaz admite hasta cinco JPEG/PNG/WebP: 10 MiB por archivo, 20 MiB por lote, 25 millones de píxeles por imagen. MIME, firma, extensión, decodificación y SHA256 se validan en servidor. Los archivos seleccionados inicialmente permanecen en el navegador hasta identificar una sola propuesta lista para asociarlos. La cantidad seleccionada es un dato del servidor que el modelo no puede reducir para omitir fotos.

POST /api/ai-gateway/incidents/<draft_id>/evidence/ exige sesión, CSRF, versión y hash; crea archivos privados de propuesta sin filas de evidencia operativa. GET bajo esa ruta verifica de nuevo propietario y acceso; la ruta media directa de un archivo pendiente no sirve como acceso público. Los previews usan private/no-store y nosniff. Las imágenes no se envían al proveedor LLM en este corte; no se atribuye al modelo inspección visual.

Las cargas por propietario se serializan. El total retenido en propuestas no ejecutadas tiene tope 100 MiB; una propuesta no permite sustituir un lote ya asociado. Un reintento idéntico devuelve el mismo resultado. El contenido se conserva durante la revisión; la expiración no autoriza borrar archivos. Una política posterior de retención/archivo deberá contemplar su recuperación antes de liberar espacio.

La confirmación usa el endpoint existente incidents/<draft_id>/confirm/. El botón humano envía true, versión y hash que incluye campos, snapshot del reporte, cantidad requerida y SHA de archivos. El modelo carece de herramienta para confirmar. El servidor bloquea/consulta el reporte, revalida los archivos y escribe atómicamente estado, fecha real, registro temporal, bitácora, referencias de evidencia y auditoría. Conserva proveedor, cotización y costo real. Repetir confirmación no duplica nada. Fallos de DB o auditoría revierten las filas; fallos de upload eliminan únicamente sus propios blobs nuevos.

Los recibos conservan los importes y proveedor de la intervención auditada, aunque luego cambie el reporte; exigen bitácora, auditoría y referencias de evidencias vigentes. Los permisos de lectura de respuestas se revalidan con report_ids, además de las referencias existentes de activos y recibos. Historial y recibos se reconstruyen desde registros actuales, nunca desde una afirmación del modelo. El canal no envía avisos adicionales: este corte no autoriza comunicaciones a terceros.

## Fecha y consumidores

Migración fallas.0009 añade fecha_trabajo_finalizado nullable sin reescribir datos. fecha_resolucion registra cuándo se incorporó el seguimiento; no se inventa una hora real. tiempo_resolucion_horas devuelve desconocido cuando solo hay precisión de día. Fallas muestra fecha real; Mantenimiento la devuelve en fechas.trabajo_finalizado. Los filtros de reportes/conciliación existentes siguen usando sus fechas históricas de registro. El estado resuelto es operativo, no pago ni cierre financiero.

## Validación y publicación

Pruebas PostgreSQL: permisos y revocación, CSRF, esquema estricto, datos faltantes y continuación, fechas, cambios posteriores, duplicados, expiración, imágenes inválidas, modificación SHA, cuota, reintentos y rollback. Pruebas de runtime: recibos actualizados/historial y límites existentes. Pruebas JS: escape, confirmación, carga de fotos, estados y controles existentes. Validar interfaz autenticada, consola/XHR y vista móvil antes de cierre.

Publicación: inspeccionar estado de migraciones/columna y respaldo recuperable; PR y CI completos, revisión del diff, merge y deploy_web_safe.sh. Bump Fallas SW v12-fecha-trabajo y versiones IA privada.

Rollback de aplicación mediante revert revisado del commit y deploy oficial: conservar la columna nullable y evidencias para no perder datos. Los flags existentes permiten detener las herramientas; cualquier cambio de configuración se tramita según las aprobaciones del proyecto. No hacer un reverse migrate que borre la fecha real.

## Caso de aceptación real

Reporte 114 de El Tunel, Pedro Navarez, terminado el 2026-10-09 según Mauricio; tres fotos; base 2500 MXN más IVA, sin pago ni costo real inventado. Preparar propuesta y comprobar previews/versión/hash en IA privada. La modificación operativa requiere confirmación explícita de esa propuesta. No crear actividad ficticia en producción para probar el flujo.
