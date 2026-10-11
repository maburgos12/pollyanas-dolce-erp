# Notas de voz — mismo Agente ERP

## Uso

En IA privada: Grabar nota → Detener grabación → escuchar → Enviar. También puede adjuntarse WAV PCM de 16 bits, hasta tres minutos y 8 MB. Puede enviarse texto y hasta cinco imágenes con la voz, hasta 20 MB combinados. La transcripción automática aparece en el historial junto con el audio privado. Una corrección por texto continúa la misma conversación.

El navegador solicita micrófono únicamente al pulsar Grabar nota. Al detener, llegar al límite o cambiar de pestaña, se detiene la captura y se liberan las pistas del dispositivo. AudioWorklet y HTTPS son necesarios para grabar; adjuntar WAV permite trabajar sin micrófono. No usa reconocimiento de voz de terceros en el navegador.

## Contratos y datos

El POST existente `/ia-privada/api/conversations/{id}/stream/` conserva JSON de texto y multipart de imágenes; añade un archivo `audio`. La ruta GET privada `/ia-privada/api/conversations/{id}/messages/{id}/audio/` y su alias de orquestacion requieren sesión, propietario, permisos READ actuales e integridad SHA. No hay URL pública del audio ni del proveedor.

La referencia del original se guarda en metadata_json.audio: nombre, path, SHA, bytes, duración comprobada en servidor y estado pending/complete/error. Complete añade modelo y transcripción. El archivo no se duplica en otros módulos. Los estados de procesos y las confirmaciones siguen a cargo de los servicios actuales. Una voz no puede confirmar las tarjetas ni cambiar roles.

## Proveedor y gasto

El SDK OpenAI instalado y la misma clave existente llaman `/v1/audio/transcriptions` con `gpt-transcribe`, contexto de idioma español, timeout de hasta 30 segundos y cero reintentos automáticos. Se envían únicamente los frames PCM validados, sin chunks adicionales del contenedor; el original se conserva íntegro. La transcripción pasa al Agent Core GPT-6.1 Sol existente dentro del límite total del turno (60 segundos).

Documentación consultada el 2026-10-11: [transcripción](https://developers.openai.com/api/docs/guides/speech-to-text) y [precios](https://developers.openai.com/api/docs/pricing). Tarifa publicada: USD 0.0045/minuto. Se reserva USD 0.03 por archivo antes de la llamada, con margen para hasta tres minutos y sobrecargo regional. Se reserva además capacidad suficiente para el siguiente request del Agent Core; su reserva efectiva se compromete antes de Responses. La reserva es conservadora, no la factura final.

Ciclo 0 registra transcripción; ciclos 1–6 conservan Responses. Todos pertenecen al mismo UUID de turno y al mismo ledger de 20 turnos/USD 5. Una reserva fallida bloquea la llamada. Un timeout conserva la reserva: no se presume que no hubo facturación. No se aumentan límites ni participantes, ni se borra historial para restablecer saldo.

## Protección, validación y reversión

WAV real se valida con wave estándar: PCM16, uno/dos canales, 8–48 kHz, duración 0.5–180 segundos, cuerpo completo y tamaño acotado. Repetir el SHA en una conversación devuelve 409 antes de guardar/cobrar otro original. Cuota conjunta de staging y propiedad se comprueban bajo bloqueo del usuario; ledger bajo lock PostgreSQL compartido. Los fallos conservan audio y señalan error, sin ejecutar el Agent Core con texto inventado.

Transcripción y capturas pueden contener errores o instrucciones maliciosas: no elevan permisos ni acreditan una operación. No se incluyen textos ni secretos en el audit de transcripción. Mantenimiento, compras, pagos y recepciones conservan sus controles.

Las pruebas usan PostgreSQL aislado y dobles del proveedor; la prueba real y navegador deben registrarse por separado. No afirmar validación de micrófono físico por una prueba de WAV o una emulación móvil.

Rollback: retirar UI/ingesta de voz mediante revert selectivo y flujo oficial Git/deploy; conservar la lectura compatible del ciclo 0 y reservas existentes. Revert completo a código previo con ledger de voz bloquearía el piloto por integridad; no borrar sus reservas para reabrirlo. Sin migraciones ni cambios de entorno.

Fuera del corte: MP3/M4A/OPUS, PDF, Telegram, WhatsApp, llamadas en vivo y respuesta hablada. Añadir codecs requiere evaluar conversión y validación de duración en el servidor; este corte usa WAV nativo para evitar dependencias y una segunda arquitectura.
