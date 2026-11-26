# Ficha de fuentes — control de campañas de clima

Fecha y ambiente consultado: 2026-11-26, PostgreSQL 16 local aislado `clima_control_20261126`; la base se creó vacía para validar migraciones y pruebas. El padrón productivo se revisa en el ERP al abrir cada campaña; esta ficha no certifica titularidad de teléfonos.

## Necesidad y unidad de análisis

Una campaña conserva una selección de empleados y un cuestionario. Cada participante es una persona del padrón de RR. HH. por campaña; cada aviso es un intento de plantilla de WhatsApp por tipo; cada respuesta es anónima y solo se liga a la campaña. La autorización de contacto es una decisión administrativa por participante, separada de la respuesta.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Padrón y contacto vigente | `rrhh.Empleado` | RR. HH. | `Empleado.id`, persona actual | Inventario de metadatos local y lectura del modelo; base local sin datos operativos | Alta/baja y demás módulos RR. HH.; clima consulta, no replica maestro |
| Selección y foto de campaña | `rrhh.CampanaClima`, `rrhh.ParticipanteClima` | Preparación de clima | `campana_id`, `empleado_id` únicos en conjunto | `inventario_fuentes_datos --term clima`: modelos y unicidad confirmados; migraciones locales aplicadas | Formulario público, cobertura, lista y avisos |
| Intento de envío | `rrhh.AvisoClima` | Servicio Meta de clima | participante y tipo únicos | Inventario local y contrato `services_clima_avisos` | Invitación, recordatorio, confirmación y tablero |
| Respuesta anónima | `rrhh.RespuestaClima` | Formulario público al completar | UUID; sin FK a empleado o participante | Inventario local y modelo | Resultados agregados al cerrar |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Empleado de RR. HH. y destinatario de clima | Confirmada solo para el `Empleado.id` seleccionado | FK directa y restricción por campaña; el nombre congelado es una foto histórica | Ninguna equivalencia nueva |
| Teléfono con formato válido y WhatsApp propio | No resuelta | El formato y unicidad en una campaña no acreditan titularidad | Confirmación humana por destinatario antes de autorizar |
| Aceptado por Meta y entregado al teléfono | Distinta | `wamid` y HTTP exitoso muestran aceptación; la ampliación recibe `delivered/read` solo cuando Meta configure y firme el webhook | Verificar suscripción y evento real en producción |

## Decisión de diseño

Reutilizar `Empleado` para estado y contacto vigente, `ParticipanteClima` para selección y autorización, `AvisoClima` para intentos y `RespuestaClima` para anonimato. Extender campaña y participante con actor, fecha y huella del teléfono autorizado; registrar decisiones en `EventoCampanaClima`. La huella evita copiar el número completo al control y obliga a renovar autorización si cambia. El servicio de avisos queda condicionado a una solicitud explícita por campaña; ningún dato maestro se copia ni corrige aquí.

Consultas o procedimiento reproducible: `manage.py inventario_fuentes_datos --term clima` con `DATABASE_URL` del PostgreSQL aislado; `manage.py showmigrations rrhh` y pruebas `rrhh.tests_clima`. No se extrajeron teléfonos ni respuestas de producción para esta ficha.

Riesgos y pendientes: el número puede estar mal asignado aunque tenga formato válido; se necesita confirmación humana. El estado `ENVIADO` representa aceptación por Meta; la entrega y lectura dependen del callback firmado y de la suscripción efectiva. Las campañas anteriores no tienen evidencia de autorización con huella y no se autorizan retroactivamente.
