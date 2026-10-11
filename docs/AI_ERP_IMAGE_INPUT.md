# Lectura de imágenes y capturas — corte 2026-10-11

## Resultado y alcance

Web → sesión y CSRF → validar/guardar original privado → mensaje existente → Agent Core Responses → herramientas autorizadas → respuesta e historial → vincular original a propuesta con versión/hash → confirmación humana existente.

No cambia modelo, participantes, presupuesto, servicios financieros ni permisos. El piloto conserva sus 20 turnos y USD 5 acumulados. Sin migraciones, nuevas dependencias, voz, PDFs ni canales nuevos.

## Contratos y límites

- El stream existente sigue aceptando JSON. Multipart admite `content` y hasta cinco `files` PNG/JPEG/WebP; 10 MiB por archivo, 20 MiB por turno y 25 millones de píxeles por imagen. Formato real, nombre, integridad y duplicados se validan en servidor.
- El historial añade `images`: UUID, nombre, bytes y URL privada. Nunca expone path ni base64. GET de imagen exige propietario, sesión, conversación activa y acceso fresco; `private, no-store` y `nosniff`.
- La imagen no se publica en una URL accesible a OpenAI. Se envía como `input_image` inline con `store=False`, sólo al proveedor ya autorizado. El modelo ve las imágenes como datos; los permisos y confirmaciones nunca dependen del prompt.
- El contexto reutiliza la última entrada visual accesible dentro de los 20 mensajes anteriores. Cada llamada vuelve a comprobar permisos e integridad. No recupera fotos de otros chats ni deja que el modelo elija rutas de almacenamiento.
- La evidencia de compras/seguimiento acepta `source_message_id` alternativo a archivos nuevos. Sólo dentro de la misma conversación autorizada; conserva original y evita duplicar almacenamiento. Versionado, hash, idempotencia y confirmación no cambian.
- Staging de propietario acotado a 100 mensajes con imágenes, 100 propuestas pendientes y 100 MiB compartidos, deduplicado por referencia física. No se eliminan originales ni datos operativos automáticamente.
- Se conservan límites del texto, seis llamadas y 60 segundos. Las imágenes tienen un límite de transporte separado de 32 MiB; no elevan el presupuesto de texto. El contador de tokens de Responses recibe el mismo input/tools/text que la generación. Conteo inválido, timeout o exceso bloquean generación. Reserva acumulada confirmada en PostgreSQL antes de generar; se revalida dentro del bloqueo existente. No hay reembolso automático por timeout ni reinicio de ledger.

## Pruebas y aceptación

PostgreSQL 16 aislado, proveedor doble: originales/SHA, formatos dañados, duplicados, cuotas, CSRF, propietario y revocación, historial y continuación, conteo visual, gasto, idempotencia y reutilización sin copia. Regresión del runtime y propuestas existentes, incluida confirmación sintética de seguimiento y lectura del original por mantenimiento. Las rutas UUID tienen máximo 100 caracteres, compatible con los campos de evidencia existentes. Navegador real con captura autorizada, recarga, respuesta sin inventar importes ausentes, consola/Network, imagen privada y móvil. La validación local no sustituye producción.

## Reversión y operación

Revertir el commit por Git y despliegue oficial; conservar originales y ledger existentes. El metadata adicional es compatible con el lector previo. No borrar archivos al revertir. El servidor antiguo no interpretará entrada visual, por lo que se debe revertir UI y backend juntos. El despliegue no habilita usuarios ni escritura adicional.

Una respuesta puede equivocarse al leer una imagen: mostrar incertidumbre y pedir sólo el dato faltante. No calificar una captura como comprobante fiscal ni una foto como verificación funcional. Voz y Telegram requieren cortes posteriores del mismo core.

Fuentes de proveedor: [entrada visual](https://developers.openai.com/api/docs/guides/images-vision), [conteo de tokens](https://developers.openai.com/api/docs/guides/token-counting) y [modelo vigente](https://developers.openai.com/api/docs/models/gpt-6.1-sol). No se aplica una fórmula de imágenes de otro modelo a Sol.
