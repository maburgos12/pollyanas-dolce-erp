# Continuidad, recibos y presentación del agente

Corte del 9 de octubre de 2026. Corrige el runtime existente de Responses; no cambia
proveedor, modelo, participantes, cupos, permisos ni el mecanismo de confirmación.

## Problema comprobado

La evaluación real conservó el borrador y creó una sola falla con confirmación humana,
pero «¿Qué folio quedó?» recibió una explicación de fuera de alcance. El runtime
excluía EXECUTED del contexto, no enviaba la conversación anterior y reemplazaba
la explicación del modelo por `_closure`. Además, la UI ocultaba el texto debajo
 de un detalle cuando había herramientas.

## Fuentes y límites

- Reutiliza ChatMessage de la conversación propia: hasta 20 mensajes / 20.000
  caracteres, sólo pares completos anteriores a la solicitud actual. Excluye
  mensajes sin prueba READ, turnos inconclusos y ambos mensajes de un par revocado.
- Las pruebas anteriores se revalidan con el usuario actual y sus recursos.
  Sus IDs se incorporan a la prueba del nuevo turno: una revocación durante la
  llamada impide volver a enviar o presentar lo materializado.
- El historial aporta continuidad, no vigencia. Las fichas operativas requieren
  lecturas actuales. No se recupera memoria DG ni conversación de otro usuario.
- Reutiliza hasta 10 incident.prepare propios del mismo chat. Incluye borradores
  incompletos y reportes confirmados, proyectados por agent_incidents.project.
  Un recibo exige que el ReporteFalla exista y coincida con equipo, sucursal y
  colaborador confirmante. Se revalida antes y después de las llamadas y al abrir
  el historial. Los recibos referenciados se validan por identidad, incluso si
  posteriormente salen de la lista de los diez más recientes.
- No agrega tablas, migraciones, una segunda captura ni herramientas de escritura.

## Respuesta y contratos

Responses solicita Structured Outputs con `answer`, `evidence_ids`, `action_claim`
(none/proposal/receipt) e `incident_ids`. El backend valida tipos, tamaño, unicidad,
referencias disponibles y estados: un borrador pendiente no respalda receipt.
Las salidas libres, referencias desconocidas o declaraciones estructuradas sin
respaldo conservan el cierre técnico seguro. Una explicación de fuera de alcance
sigue siendo final del servidor; no admite una cifra o acción inventada del modelo.
La narración válida aparece primero, escapada como texto; los datos consultados
 y comprobantes aparecen debajo. El JSON técnico queda en un detalle accesible.

La serialización añade `presentation` y `receipts` sin retirar campos existentes.
No entrega el razonamiento, instrucciones ni metadatos de autorización al cliente.
Las tarjetas y confirmaciones siguen procediendo exclusivamente de DTOs del servidor.

**Límite explícito:** la validación de referencias no demuestra el significado ni
la exactitud de cada oración generada. La prosa está identificada como «Respuesta
IA»; nunca constituye comprobante de ejecución. El estado, folio, permiso y botón
proceden del ERP. La confirmación CSRF autenticada, el hash/versionado, bloqueo,
idempotencia y auditoría atómica permanecen independientes del modelo. No se usa
un filtro de palabras para interpretar solicitudes ni se exige una frase al usuario.

## Controles preservados

El registro de reservas se compromete antes de cada llamada. Historial y schema
entran en el cálculo de bytes y gasto; no hay reinicio de cupo, reembolso por error,
llamadas adicionales de revisión ni ampliación a otros usuarios. Permanecen los
límites de 6 llamadas / 10 tools / 60 segundos y las puertas de READ, activos,
workflows e incidents. Se permite terminar sin otra tool cuando ya hay historial
para una aclaración o recibos actuales; eso no autoriza otras operaciones.

## Validación y reversión

Pruebas PostgreSQL 16 de continuidad, límites, omisión de pares revocados,
propiedad, recibos borrados o reasignados, folio sin segunda escritura, replay sin
segunda llamada, prosa con referencias incorrectas, XSS y orden visual. Se ejecutan
además las suites existentes de runtime, workflows, incidentes, piloto y Gateway.

Rollback: revertir el commit de este corte por PR y ejecutar el despliegue oficial.
No se revierten mensajes, reservas, recibos ni datos de Fallas. No hay migración.
La puerta de incidentes permanece apagada en producción hasta su autorización
específica; el despliegue de esta corrección no la habilita.
