# IA privada: recuperación, consumo y lecturas repetidas

Un corte de recepción podía mostrar error aunque el servidor ya hubiera guardado
la transcripción y la respuesta. El cliente ahora consulta el estado persistido;
no vuelve a enviar el turno a OpenAI ni confirma acciones operativas.

## Contrato y recuperación

- El envío JSON/multipart admite `client_request_id`, UUID opcional para clientes
  anteriores. Se valida antes de admitir archivos o crear mensajes, se conserva
  en los metadatos del asistente y se proyecta en el historial del propietario.
  Es una referencia de correlación, no una credencial ni una nueva autorización.
- Una interrupción de recepción provoca un GET de la conversación existente.
  Sólo una respuesta con el UUID exacto y estado `complete`/`error` resuelve la
  incertidumbre. Una respuesta distinta o en curso no habilita un reenvío.
- Si no se puede verificar, aparece «Consultar resultado guardado». Conserva
  audio y fotos; bloquea nuevos envíos, grabaciones y creación de conversaciones
  hasta verificar. El botón sólo consulta. No hay reintentos POST automáticos.
- Al recuperar se conservan respuestas de error del servidor, permisos vigentes,
  tarjetas y confirmaciones. Tampoco se reintentan escrituras de evidencia: las
  fotos permanecen en el historial y se indica revisar su asociación antes de
  confirmar. El envío normal conserva la asociación por la vía existente.
- El UUID no sustituye los controles actuales de duplicación de audio ni concede
  idempotencia a endpoints operativos. No introduce tablas ni migraciones.

## Reserva y estimación

El encabezado identifica límite, turnos y **reserva preventiva**. Cada respuesta
del piloto permite abrir **Consumo estimado de esta consulta**. El historial
expone `cost` y el detalle incluye `runtime_status` para actualizar el piloto.

La estimación usa exclusivamente tokens registrados, modelo/tier conocido y,
para voz completa, duración PCM validada. Referencia estándar verificada el
11 de octubre de 2026: GPT-6.1 Sol USD 2/M entrada y USD 10/M salida; GPT Transcribe
USD 0.0045/min. Fuentes: [tarifas](https://developers.openai.com/api/docs/pricing)
y [lanzamiento](https://developers.openai.com/api/docs/changelog#september-2026).

**No es facturación conciliada.** No infiere descuentos/escrituras de caché,
recargos regionales ni consumo ausente. Si faltan mediciones se indica parcial;
si no existe ninguna se indica «no disponible», nunca cero por defecto.
La reserva comprometida permanece intacta, también ante timeout. No cambia
participante, modelo, límite de 40 turnos, USD 5, ni comprobantes históricos.
Actualizar tarifas requiere verificar la fuente y revisar este cálculo.

## Redundancia

Dentro de un único turno, lecturas idénticas y exitosas de activos y requisitos
pueden reutilizar su evidencia. Se revalidan permisos y recursos antes de cada
petición. Se devuelve una referencia al resultado ya presente en el contexto,
con su fecha original; no una segunda copia de datos ni un recibo de ejecución
ficticio. El historial conserva la ejecución real y el contador de reutilización.
Reutilizaciones también cuentan para el máximo de diez intentos de tools.

No se reutilizan borradores, transiciones, consultas financieras, errores ni
resultados de otros turnos. Filtros distintos se consultan. Los límites de
argumentos y contexto siguen aplicándose. El modelo recibe la indicación de
buscar la persona mencionada desde la primera consulta de requisitos: no hay
clasificador por palabras ni nuevos formularios conversacionales.

## Validación y reversión

Pruebas: corte SSE con estado final, estado pendiente, respuesta ajena al turno,
un único POST, retiro de audio aceptado, UUID inválido antes de admisión,
propiedad, precios parciales, reserva intacta, reutilización con alcance revocado,
argumentos excesivos y límites de tools. El entorno de navegador usa PostgreSQL
16 sintético, proveedor doble sin red y un stream deliberadamente truncado.
No se reenvía la nota real ni se crean compras/fallas para validar producción.

| Antes | Después | Motivo |
| --- | --- | --- |
| Error local aun con respuesta guardada | Recuperar el resultado exacto por GET | Evitar reenvío y cobro repetido |
| Reserva confundida con consumo | Importes y límites identificados | Mostrar la incertidumbre del cálculo |
| Repetir una lectura y sus datos | Referencia a evidencia del mismo turno | Reducir redundancia sin omitir controles |

El service worker global sólo usa red para estas solicitudes; se incrementa su
versión con este cambio visible. Rollback: revertir el commit mediante PR y
`deploy_web_safe.sh`, conservando mensajes, archivos, reservas y metadatos.
Clientes anteriores siguen siendo admitidos sin UUID; no requiere cambiar `.env`.
La prueba controlada cubre la recuperación, no identifica la excepción histórica
exacta del iPhone ni demuestra un porcentaje de ahorro del modelo real.
