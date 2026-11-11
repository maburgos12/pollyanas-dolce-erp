# Ficha de fuentes — avisos de clima por Meta

Fecha y ambiente consultado: 2026-10-13; PostgreSQL local aislado y configuración de producción consultada en solo lectura.

## Necesidad y unidad de análisis

Un aviso representa un intento de invitación o recordatorio para un participante de una campaña. La respuesta individual se registra en otra tabla, sin relación desde la respuesta al empleado.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Personal y teléfono personal | `rrhh.Empleado` | Expediente RRHH | PK empleado | Padrón productivo previo: 68 activos elegibles; local fresco: 0 | RRHH y clima |
| Campaña y participación | `rrhh.CampanaClima`, `rrhh.ParticipanteClima` | Preparación de clima y formulario | PK campaña; único campaña/empleado | Producción previa: 0 campañas; local fresco: 0 | Panel de clima y acceso individual |
| Resultado anónimo | `rrhh.RespuestaClima` | Formulario público | UUID sin FK a participante | Producción previa: 0 respuestas | Agregados de clima |
| Transporte Meta | `core.whatsapp.enviar_plantilla` | API Cloud de Meta | ID externo `wamid` al aceptar | Canal ERP configurado; dos plantillas clima aprobadas por Meta en `es_MX` | Compras y clima |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| `Empleado.telefono` / teléfono de trabajo en `UserProfile` | Distinta | El primero es contacto personal de RRHH; el segundo corresponde a la cuenta operativa | Ninguna equivalencia nueva |
| `ParticipanteClima` / `RespuestaClima` | Distinta | La respuesta no identifica al participante; solo el estado `respondio` marca cobertura | Conservar separación |

## Decisión de diseño

Reutilizar empleado, campaña, participante y el remitente de Meta. Extender participante con autorización individual apagada por defecto. Crear `AvisoClima` para registrar una invitación y un recordatorio como máximo por persona y campaña; no duplicar el expediente ni vincular respuesta a persona. El envío también exige habilitación global, campaña abierta y número personal único en el padrón.

Consultas o procedimiento reproducible: `python3 manage.py inventario_fuentes_datos --term clima --term empleado --term aviso --term whatsapp --limit 4 --presence` con PostgreSQL local. La consulta léxica no prueba equivalencia.

Riesgos y pendientes: un `ENVIADO` del API indica aceptación, no entrega al teléfono. Antes de diciembre se requiere aprobar destinatarios de prueba, verificar entrega real y definir apertura de campaña. No se habilita el flag en producción ahora.

## Extensión de cierre de encuesta — 2026-11-11

Unidad de análisis: una respuesta completa por participante y, si tiene WhatsApp autorizado, un intento de confirmación. Se reutilizan `rrhh.Empleado` como origen del teléfono, `CampanaClima` y `ParticipanteClima` para cobertura, `RespuestaClima` para las 26 respuestas sin FK personal, `AvisoClima` como registro único por participante y tipo, y `core.whatsapp.enviar_plantilla` como transporte. No se crea una segunda fuente de personas ni de respuestas. El formulario público es quien crea la respuesta; el estado `respondio` y el aviso se actualizan en la misma transacción. La confirmación no contiene respuestas ni liga.

Consulta reproducible: `python3 manage.py inventario_fuentes_datos --term clima --term empleado --term aviso --term whatsapp --limit 4 --presence` en PostgreSQL local aislado. El inventario encontró los modelos candidatos; una instalación local nueva no contiene registros operativos. La evidencia productiva consultada en esta tarea fue una campaña piloto, un participante, cero respuestas y una invitación aceptada por Meta. `clima_laboral_confirmacion_v1` quedó en revisión de Meta el 11 de noviembre; no se puede afirmar entrega hasta aprobación y prueba en el destinatario autorizado.
