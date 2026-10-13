# Entrada privada PDF del ERP AI Agent

El compositor acepta JPEG/PNG/WEBP y PDF dentro del mismo turno, con texto y/o WAV. Se conserva el original privado; no hay tablas, migraciones, dependencias ni permisos nuevos. El subconjunto de herramientas habilitadas y las confirmaciones operativas siguen vigentes. Este cambio no habilita nuevas consultas de ventas.

## Contrato y límites

- Multipart `files`: máximo cinco archivos, 10 MiB por archivo y 20 MiB combinados con audio. Hasta diez páginas PDF en total; documentos cifrados, corruptos, vacíos o fuera de límites se rechazan antes de crear el turno. Sin comentario, se propone analizar los archivos adjuntos.
- Validador de firmas/MIME existente más verificación real de imágenes. Para PDF, pdfplumber/pdfminer ya instalados leen metadatos en proceso separado: timeout 5 s, CPU 3 s y 512 MiB de espacio de direcciones en Linux, sin heredar entorno/credenciales. No se ejecuta contenido del PDF.
- Original en ruta UUID privada existente, SHA256, bytes, MIME y páginas; cuotas de almacenamiento, propietario, conversación y acceso vigente comprobados. La clave histórica `images` y ruta `/images/` se conservan por compatibilidad; ahora cada elemento proyecta `mime` para distinguir PDF de fotografía. Nunca se proyecta ruta de storage o base64.
- Responses recibe el PDF original como `input_file` inline con `detail=high`; lee texto y páginas escaneadas. `store=False`, sin Files API persistente ni URL externa. La lógica de texto acota el transporte inline por separado, pero la reserva requiere conteo de tokens del proveedor antes de **cada** generación. Se mantienen límites de turno/contexto/tiempo/participantes/gasto. Si falla el conteo o falta presupuesto, no se genera la respuesta.
- Descargar PDFs requiere la misma autorización que las imágenes; entrega como attachment, `private, no-store` y `nosniff`. No se embeben como `img` ni como HTML en origen ERP.

[Documentación oficial de archivos](https://developers.openai.com/api/docs/guides/file-inputs): el PDF aporta texto e imágenes de página, por lo que no debe estimarse por bytes comprimidos ni tratarse como texto gratis.

## Seguimiento y controles conservados

Imágenes/PDF pueden asociarse desde su mensaje original a un seguimiento o compra preparada sin segunda copia. Se reutilizan SHA, idempotencia de carga, versión/hash de propuesta y recibos/auditoría. El original se conserva antes del proveedor; la recuperación posterior sólo consulta historial y no reenvía archivos o generación.

`expected_file_count` conserva el contrato legado de fotos mínimas y `evidence_count` sigue declarando fotos prometidas. Un PDF no satisface una foto pendiente. `expected_attachment_count` registra además el mínimo de originales recibidos, monotónico entre turnos y ligado al hash; un PDF recibido debe asociarse antes de confirmar el seguimiento. No cambia el hash de propuestas legadas hasta que se actualizan normalmente.

Una factura, PUE o forma de pago impresa no prueba pago, fecha real de terminación, recepción, ejecución o validez SAT. El modelo recibe contenido como datos no confiables; el catálogo/servidor conserva la autoridad. El seguimiento conserva proveedor, cotización y costo real; sólo la tarjeta humana puede actualizar el trabajo. No se crean gastos, pagos ni CFDI contables en este corte.

## Verificación

Pruebas con PostgreSQL aislado y proveedor sintético: PDF textual/escaneado, corrupción/cifrado, máximo de páginas agregado, duplicados, original íntegro/privado, conteo y límites, reutilización con confirmación sólo del trabajo, PDF que no sustituye fotos prometidas, mínimo de original asociado, multipart sin comentario, UI y recuperación. Los tests de runtime/audio/piloto y PWA siguen activos. Prueba en navegador: un POST de stream, asociación desde source_message_id, recarga con propuesta/PDF persistentes, sin consola JS; el proveedor local no prueba comprensión real del modelo.

| Before | After | Why |
| --- | --- | --- |
| Sólo imágenes en el selector | Imágenes y PDFs, con límites | Aceptar el documento original sin conversión manual |
| Toda evidencia se renderiza como foto | PDF enlazado con nombre, imagen sólo para MIME de imagen | Evitar previews rotas y contenido activo en origen ERP |
| Conteo de archivos podía sustituir fotos | Mínimos independientes de fotos y originales | Conservar evidencias prometidas y confirmación segura |

## Publicación y rollback

Bump coordinado del SW global y registros en base/login; comportamiento de caché/red sin cambios. Revisión completa, CI, merge y deploy oficial. Verificación autenticada en producción separada del entorno local y preparación del reporte real para confirmación; documentar resultado en artefactos privados de la tarea. Rollback mediante revert del único corte de código, conservando archivos, propuestas, historial y auditoría. No borrar originales ni modificar .env o presupuesto para revertir.
