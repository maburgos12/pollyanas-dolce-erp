# Aviso de compra realizada al solicitante

Cuando Compras guarda «Registrar compra realizada», el ERP avisa por correo y
WhatsApp a **la persona que creó la solicitud** (`solicitud.solicitante`), no al
comprador ni a los responsables del área. La compra y la entrega siguen siendo
pasos separados: el aviso dice «Comprado, pendiente de entrega» y nunca afirma
una fecha de entrega.

## Cola persistente

`AvisoCompraDepartamental` es la cola: un renglón por compra y canal, con
`UniqueConstraint(compra, canal)`. Los renglones se crean **dentro** de la misma
transacción que la compra —si la transacción se revierte no queda cola ni
envío— y el envío se despacha en `transaction.on_commit` hacia la tarea Celery
`compras.enviar_avisos_compra_realizada`. Si Redis o el worker no responden, el
aviso queda en `PENDIENTE` y se reintenta desde la pantalla; nunca se pierde.

La migración es aditiva y sin backfill: las compras registradas antes de este
cambio no tienen renglones de aviso y, por lo tanto, **no generan envíos
retroactivos** en migración ni en despliegue.

## Cola dedicada

El worker por omisión corre `--pool=solo --concurrency=1` y atiende una tarea a
la vez. Una sincronización de ventas de Point ocupa ese worker más de diez
minutos, así que un aviso ruteado a la cola `celery` se forma detrás de la
automatización de navegador y puede tardar horas en salir (caso real: 9 compras
con sus 18 avisos en `PENDIENTE` detrás de 45 tareas de Point).

Por eso `compras.enviar_avisos_compra_realizada` va a la cola `notificaciones`,
con su propio consumidor `worker_notificaciones` en `docker-compose.yml`, igual
que `pos_bridge.catalog_recipe_sync` usa `recipes`. **Rutear la tarea sin
levantar su consumidor deja los avisos encolados para siempre**; hay una prueba
que verifica que el `docker-compose.yml` declara un worker con `-Q notificaciones`.

## Estados

| Estado | Significa |
| --- | --- |
| `PENDIENTE` | En cola, aún sin intento. |
| `ENVIADO` | El proveedor del canal **aceptó** el mensaje y devolvió identificador. No significa entregado. |
| `FALLIDO` | El proveedor respondió rechazando el mensaje. Reintentar no duplica. |
| `SIN_CONTACTO` | El solicitante no tiene ese contacto, o lo tiene con formato inválido. |
| `INCIERTO` | No hubo respuesta (timeout, red) o llegó sin identificador. Debe reconciliarse antes de reenviar. |
| `SIN_CANAL` | El canal no está configurado en este ambiente. |

No existe estado «entregado»: ni Resend ni Meta confirman entrega al aparato por
esta vía, así que la pantalla no lo afirma.

## Contacto de trabajo vs. contacto personal

El ERP guarda dos cosas distintas que **no deben mezclarse**:

| | Campo | Qué guarda |
| --- | --- | --- |
| **Trabajo** | `User.email`, `UserProfile.telefono` | Correo `@pollyanasdolce.com` y línea propiedad de Pollyana's Dolce, asignada a un puesto o departamento. |
| **Personal** | `Empleado.email`, `Empleado.telefono` | Gmail y celular propios del colaborador. Existen para Capital Humano. |

`core/contactos.py` resuelve **solo el contacto de trabajo**. No hay fallback al
expediente: el celular y el correo personales no son un respaldo del contacto de
trabajo, y mandar un asunto operativo ahí es mezclar dos cosas distintas.

Si la persona no tiene contacto de trabajo, el aviso queda `SIN_CONTACTO` con el
motivo «Sin correo de trabajo registrado» o «Sin teléfono de trabajo registrado».
**Nunca** se sustituye por el contacto de otra persona ni por el personal del
propio titular.

Ambos valores se validan: el correo con `validate_email`, el teléfono
normalizado a E.164 (un número de 10 dígitos recibe la lada 52).

Nota operativa: las líneas de la empresa que ya usaba el sistema Agente DG se
registraron en `UserProfile.telefono`; los números del expediente de RRHH se
conservaron intactos porque son personales.

## Canal de WhatsApp

`core/whatsapp.py` habla directo con Meta Cloud API
(`graph.facebook.com/v21.0/{phone_number_id}/messages`), revisa el código HTTP y
conserva el identificador del mensaje.

No se reutilizaron los helpers de `seguimiento/tasks.py` ni `fallas/tasks.py`:
ambos hacen `httpx.post` contra `https://api.pollyanasdolce.com/api/send-message/`
y solo registran algo si se levanta una excepción. Ese endpoint **no existe**
(responde 404 y no aparece en el OpenAPI del servicio Maya), así que esas
llamadas nunca envían nada y jamás lo reportan.

Requisito vigente del canal: un aviso iniciado por el negocio fuera de la
ventana de 24 horas solo puede enviarse con una **plantilla aprobada por Meta**.
Por eso este módulo solo manda `type: template`.

### Variables de ambiente

| Variable | Para qué |
| --- | --- |
| `WHATSAPP_ENABLED` | Interruptor del canal. Sin él, el aviso queda `SIN_CANAL`. |
| `META_WHATSAPP_TOKEN` | Access token del System User con permiso sobre el WABA. |
| `META_WHATSAPP_PHONE_NUMBER_ID` | Número emisor del WABA. |
| `WHATSAPP_TEMPLATE_COMPRA_REALIZADA` | Nombre de la plantilla aprobada. |
| `WHATSAPP_TEMPLATE_LANGUAGE` | Código de idioma; `es_MX` por omisión. |

Mientras esas variables no existan en el ambiente, el canal queda inactivo y la
pantalla lo dice con todas sus letras en lugar de simular un envío.

### Plantilla requerida

Cuerpo con cuatro parámetros, en este orden: nombre, artículo, folio y fecha.

```
Hola {{1}}, ya realizamos la compra de {{2}}, de tu solicitud {{3}}.
Fecha de compra: {{4}}.
Estado: Comprado, pendiente de entrega.
Consulta el seguimiento en el ERP.
```

## Reintento

`POST /compras/departamentales/avisos/<pk>/reintentar/` — solo Compras
(`puede_gestionar_compras_departamentales`) o Dirección (`_es_direccion`). Un
aviso `ENVIADO` responde sin reenviar. Un `INCIERTO` de correo se reconcilia
primero contra la API de Resend con su identificador; si se confirma, se marca
`ENVIADO` sin volver a mandar nada. Cada intento suma `intentos` y guarda
`ultimo_intento_en` y `ultimo_intento_por`.
