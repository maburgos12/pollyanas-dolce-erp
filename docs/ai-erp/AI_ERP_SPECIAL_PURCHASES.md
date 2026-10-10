# Compras especiales mediante el Agent Core

## Alcance del corte

Mismo runtime Responses, catálogo de tools y controles acumulativos de participantes y gasto. El modelo interpreta el lenguaje natural; el servidor valida identidad, cantidades, precios, ámbito y confirmación. No se incorporan keywords ni un segundo agente.

Tools: `purchase.requirements`, `purchase.search`, `purchase.get`, `purchase.prepare`.
El runtime ofrece estas capacidades exclusivamente al participante activo del piloto que ya tenga permisos de compras o un área capturable. Reutiliza las puertas READ y confirmación existentes. No habilita participantes ni aumenta límites.

## Captura y confirmación

1. Requisitos resuelve áreas autorizadas y personas activas; no expone correo, teléfono ni datos de RH.
2. Buscar solicitudes existentes por fragmentos útiles de artículos/folio antes de preparar.
3. Preparar/resumir conserva JSON estructurado en ChatToolCall con UUID, versión, hash y caducidad de 24 horas. La continuación usa el mismo draft_id.
4. Imágenes opcionales: hasta cinco JPG/PNG/WebP, 10 MB cada una, 20 MB total, validación de contenido y dimensiones. Subida idempotente, cuota de pendientes, acceso privado y hash de bytes. La evidencia queda ligada a persona y área y no se sirve por URL media.
5. Confirmar mediante POST de sesión con CSRF, `confirm: true`, versión y hash. El modelo no recibe esta herramienta. Se valida de nuevo identidad, permisos, duplicados y archivos, y se crea y envía una solicitud EXTRAORDINARIA mediante el mismo servicio del formulario.
6. El folio y recibo se revalidan al recargar historial. El solicitante autorizado puede consultar la evidencia desde la ficha de Compras después de confirmar.

Precios son cadenas decimales. `items.precio_total` corresponde a toda la cantidad; el backend calcula el unitario exacto a centavos. Si no es exacto, pide revisión. Sin precio se conserva NULL. Productos más envío se comparan contra el total informado cuando todos están disponibles.

## Compra reportada y registro financiero

`compra_reportada` conserva lo que informó una persona; NO representa CompraRealizadaDepartamental, autorización anterior, pago, entrega ni aviso de compra enviado. El envío global sin asignación permanece informado una sola vez. No se crean proveedores, cotizaciones o registros financieros con datos incompletos. La ficha muestra este pendiente de regularización.

La compra financiera existente conserva cotización seleccionada, límite autorizado, intento vigente y avisos outbox al solicitante. Su evidencia ahora es opcional; ausencia de comprobante da 404 al descargar y no bloquea registro ni avisos. La captura no es CFDI. Se puede agregar evidencia posteriormente con la corrección auditable existente; la recepción mantiene su proceso separado.

## Pruebas y reversión

Pruebas PostgreSQL: permisos y representación, otro propietario/participante, flags, precios/totales, NULL, multi-turn, repetición, duplicados, CSRF, archivo privado/integridad, historial y herramientas del mismo runtime. Pruebas de Compras conservan autorizaciones y avisos sin evidencia obligatoria.

Rollback de código mediante revert del commit y deploy oficial. La migración 0021 sólo modifica estado de Django (sqlmigrate no-op). Si ya se registraron compras sin evidencia, NO revertir el requisito de archivo sin revisar dichos registros: conservar `blank=True` evita bloquear su corrección. No borrar solicitudes, eventos, archivos ni recibos creados.

## Pendientes explícitos

Este corte no añade visión al proveedor ni transcripción de voz. Las imágenes son evidencia privada; el agente no afirma haberlas leído. Los adaptadores Telegram/WhatsApp tampoco se habilitan aquí. Antes de conectar estas modalidades al mismo core, deben conservarse los límites de gasto, acceso y trazabilidad.

No ejecutar la compra financiera del ejemplo hasta identificar vendedores, cotizaciones y reparto de envío reales. No enviar un aviso de compra realizada por confirmar únicamente la solicitud.
