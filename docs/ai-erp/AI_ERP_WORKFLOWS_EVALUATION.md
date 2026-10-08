# F4.3 — evaluación de continuidad READ

Fecha: 2026-10-07. Estado: candidato local conservado; sin PR, merge, despliegue ni activación. El corte agrega contexto autorizado de pendientes y un contrato LLM exclusivo al Agent Core existente. Reutiliza servicios, PostgreSQL, Gateway, SDK y contratos REST; no agrega dependencias ni migraciones. No incluye Telegram, voz, archivos, acciones operativas o rediseño de UI.

## Resultado y límite de aceptación

| Medición del candidato final | Modelo | Interacciones correctas | Seguridad observada |
| --- | --- | --- | --- |
| Primera repetición congelada | gpt-4o-mini | 12/16 | 16/16 |
| Segunda repetición congelada | gpt-4o-mini | 12/16 | 16/16 |
| Muestra nueva congelada antes de consultar | gpt-4o-mini | 3/7 | 7/7 |
| Comparación final | gpt-4.1-mini | 16/16 | 16/16 |

Las dos repeticiones con gpt-4o-mini fallan en continuación parcial entre conversaciones, selección entre pendientes y una consulta de costos autorizados. La muestra nueva también falla en continuidad y ambigüedad. El backend mantiene los límites de seguridad, pero eso no prueba que el modelo opere el proceso correctamente.

GPT-4.1 mini pasó una medición completa del candidato final. Faltan dos mediciones consecutivas del mismo código y corpus, y la muestra nueva con ese modelo; una corrida no satisface esa puerta. No se cambia la configuración productiva a partir de esta comparación. Las tres mediciones exploratorias anteriores fueron 12/16, 11/16 y 13/16; la última precede al cierre con evidencia del servidor. No se mezclan con las repeticiones del candidato final.

## Evidencia verificable

- 129 pruebas locales seleccionadas con PostgreSQL 16: workflows, runtime READ y Gateway de activos. Cubren contrato exclusivo/REST compatible, datos faltantes, propiedad, revocación, límites de contexto, paginación con recursos revocados, historial con F4 apagado, replay y evidencia del servidor. Esta selección es distinta de las 150 pruebas documentadas para F4.1/F4.2.
- `check`: cero errores; `migrate --check`: cero pendientes; `makemigrations --check --dry-run`: sin cambios. No se crean ni modifican migraciones.
- Las llamadas reales utilizan Responses API con la clave ya autorizada, `store=False`, sin reintentos automáticos, datos ficticios y PostgreSQL aislado. Las reservas se registran antes de cada llamada. El total incluye 217 solicitudes previas y 196 nuevas: 413 acumuladas.
- Las tablas operativas Activo, PlanMantenimiento, OrdenMantenimiento y ReporteFalla conservan su hash antes/después de cada lote. No aparecen sugerencias ni herramientas en ejecución pendientes. Los controles de replay hacen cero llamadas al proveedor. Los 103 turnos medidos entre los siete lotes satisfacen los controles observados de seguridad; no equivale a una garantía general.
- El evaluador exige UUID original, selección correcta, versión y estado, conservación de pendientes hermanos y prueba persistida de rechazos. Cuando usa el contexto inicial como evidencia, su JSON visible debe coincidir con el snapshot autorizado del proveedor y con las referencias de la fuente. Incluye contraejemplos para evitar aprobar un UUID nuevo, modificaciones colaterales, un snapshot vacío o una denegación sin evidencia.
- La revisión posterior recalcula las siete mediciones sin llamadas a OpenAI y conserva resultados originales. Los fingerprints de los tres archivos de código permiten identificar exactamente el candidato evaluado.
- Navegador local autenticado con usuario ficticio: el chat muestra UUID/opciones reales y fuente de pendientes; el API de la conversación responde 200 y conserva `tool_calls: []` cuando no hubo ejecución. Sin errores ni advertencias de consola. Esta revisión usa un doble de proveedor y no demuestra UI en producción. La UI presenta el JSON y tiene desbordamiento horizontal; F5 debe mejorar esa presentación antes de considerarla experiencia final.

Corpus: 15 escenarios/16 interacciones; SHA-256 `aadc4ba81c74367707c08676546df832ec8811454e09c1a3222d8f353479eb4e`.
Muestra adicional: 6 escenarios/7 interacciones; SHA-256 `d14cc430805adeada4531b08bf23fb7e77ad20cc3d9e60e2a03a5b09638640b1`.

Evidencia privada: `~/.codex/task-artifacts/ai-erp-workflows-strict-20261007/`, incluyendo ledgers, resultados, respuestas del proveedor con fixtures, `measurement-review.json`, checks, fingerprints y cierre del entorno. No contiene una copia de la base productiva y no se publica en Git.

## Presupuesto y propuesta revisable

El techo autorizado de reserva conservadora acumulada es USD 1.00, incluyendo USD 0.46257440 previos. Se reservaron USD **0.99570040** acumulados. Quedan USD 0.00429960: no alcanza para otro lote completo. La estimación basada en tokens de las llamadas nuevas es USD 0.10188300; acumulada con la evaluación anterior, USD 0.18326510. Reserva y estimación no son la factura del proveedor.

La siguiente decisión propuesta es ampliar únicamente el techo acumulado de evaluación a **USD 1.50** y usar gpt-4.1-mini para dos lotes consecutivos del corpus congelado y la muestra nueva. Esto no autoriza cambiar el modelo en producción, activar el agente, conceder usuarios ni ejecutar escrituras. Reutilizar la misma clave ya autorizada y el mismo código congelado. Detenerse al techo o ante un fallo material; no adaptar los prompts de prueba después de conocer las respuestas.

Si esa puerta pasa, presentar la decisión de modelo para su aprobación y continuar el flujo oficial de publicación con gates apagados. Si falla, conservar los resultados y diagnosticar antes de ampliar alcance o presupuesto. Hasta entonces el candidato permanece pendiente de validación.

## Recuperación y recursos

Rama `codex/ia-erp-workflows-strict`; worktree `ai-erp-workflows-strict-20261007`; responsable `codex-erp-ai-root`. El código se conserva para revisión. No se ha desplegado, por lo que producción no necesita rollback. Si posteriormente se publica, revertir este cambio de runtime/contrato LLM por Git manteniendo modelos, migración y datos técnicos existentes.

Entorno descartable: Compose `erp_ai_workflows_strict_20261007`, PG 55547, preview 18098 y volumen exclusivo `erp_ai_workflows_strict_20261007_postgres_data`. Su cierre y respaldo verificado se registran fuera de Git; no usar limpieza global ni retirar recursos ajenos. Un respaldo local de fixtures no demuestra recepción en el NAS.
