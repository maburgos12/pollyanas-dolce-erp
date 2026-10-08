# F4.3 — evaluación de continuidad READ

Fecha: 2026-10-07. Estado: candidato local conservado; sin PR, merge, despliegue ni activación. El corte agrega contexto autorizado de pendientes y un contrato LLM exclusivo al Agent Core existente. Reutiliza servicios, PostgreSQL, Gateway, SDK y contratos REST; no agrega dependencias ni migraciones. No incluye Telegram, voz, archivos, acciones operativas o rediseño de UI.

## Resultado y límite de aceptación

| Medición del candidato final | Modelo | Interacciones correctas | Seguridad observada |
| --- | --- | --- | --- |
| Primera repetición congelada | gpt-4o-mini | 12/16 | 16/16 |
| Segunda repetición congelada | gpt-4o-mini | 12/16 | 16/16 |
| Muestra nueva congelada antes de consultar | gpt-4o-mini | 3/7 | 7/7 |
| Comparación final | gpt-4.1-mini | 16/16 | 16/16 |
| Repetición autorizada A | gpt-4.1-mini | 14/16 | 16/16 |
| Repetición autorizada B | gpt-4.1-mini | 16/16 | 16/16 |
| Muestra nueva con el mismo candidato | gpt-4.1-mini | 4/7 | 7/7 |

Las dos repeticiones con gpt-4o-mini fallan en continuación parcial entre conversaciones, selección entre pendientes y una consulta de costos autorizados. La muestra nueva también falla en continuidad y ambigüedad. El backend mantiene los límites de seguridad, pero eso no prueba que el modelo opere el proceso correctamente.

La ampliación aprobada a USD 1.50 se ejecutó sin cambiar los tres archivos de código medidos ni las preguntas. GPT-4.1 mini produjo 14/16 y 16/16 en las dos repeticiones consecutivas, y 4/7 en la muestra nueva. **La puerta de aceptación no pasó**: no hay dos lotes consecutivos completos y falla la generalización. No se cambia el modelo productivo ni se publica este candidato. Las tres mediciones exploratorias anteriores fueron 12/16, 11/16 y 13/16; la última precede al cierre con evidencia del servidor. No se mezclan con las repeticiones del candidato final.

Las fallas nuevas son `partial_cross_chat:0/1`, `fresh_missing:0` y `fresh_partial:0/1`. En los turnos iniciales fallidos, el proveedor responde en texto sin llamar la herramienta que debe crear o actualizar el estado. En el segundo turno de la primera repetición usa una continuación vacía y el servidor conserva WAITING_INFORMATION; mantiene el UUID y no duplica, pero no resuelve el proceso. Mostrar el snapshot autorizado no equivale a guardar el dato aportado ni completar la consulta. La respuesta narrativa del modelo se conserva en los artefactos para diagnóstico; no se usa como evidencia de ejecución.

## Evidencia verificable

- 129 pruebas locales seleccionadas con PostgreSQL 16: workflows, runtime READ y Gateway de activos. Cubren contrato exclusivo/REST compatible, datos faltantes, propiedad, revocación, límites de contexto, paginación con recursos revocados, historial con F4 apagado, replay y evidencia del servidor. Esta selección es distinta de las 150 pruebas documentadas para F4.1/F4.2.
- `check`: cero errores; `migrate --check`: cero pendientes; `makemigrations --check --dry-run`: sin cambios. No se crean ni modifican migraciones.
- Las llamadas reales utilizan Responses API con la clave ya autorizada, `store=False`, sin reintentos automáticos, datos ficticios y PostgreSQL aislado. Las reservas se registran antes de cada llamada. El primer corte incluyó 217 solicitudes previas y 196 nuevas: 413 acumuladas antes de la ampliación posterior.
- Las tablas operativas Activo, PlanMantenimiento, OrdenMantenimiento y ReporteFalla conservan su hash antes/después de cada lote. No aparecen sugerencias ni herramientas en ejecución pendientes. Los controles de replay hacen cero llamadas al proveedor. Los 103 turnos medidos entre los siete lotes satisfacen los controles observados de seguridad; no equivale a una garantía general.
- El evaluador exige UUID original, selección correcta, versión y estado, conservación de pendientes hermanos y prueba persistida de rechazos. Cuando usa el contexto inicial como evidencia, su JSON visible debe coincidir con el snapshot autorizado del proveedor y con las referencias de la fuente. Incluye contraejemplos para evitar aprobar un UUID nuevo, modificaciones colaterales, un snapshot vacío o una denegación sin evidencia.
- La revisión posterior recalcula las siete mediciones sin llamadas a OpenAI y conserva resultados originales. Los fingerprints de los tres archivos de código permiten identificar exactamente el candidato evaluado.
- Navegador local autenticado con usuario ficticio: el chat muestra UUID/opciones reales y fuente de pendientes; el API de la conversación responde 200 y conserva `tool_calls: []` cuando no hubo ejecución. Sin errores ni advertencias de consola. Esta revisión usa un doble de proveedor y no demuestra UI en producción. La UI presenta el JSON y tiene desbordamiento horizontal; F5 debe mejorar esa presentación antes de considerarla experiencia final.

Corpus: 15 escenarios/16 interacciones; SHA-256 `aadc4ba81c74367707c08676546df832ec8811454e09c1a3222d8f353479eb4e`.
Muestra adicional: 6 escenarios/7 interacciones; SHA-256 `d14cc430805adeada4531b08bf23fb7e77ad20cc3d9e60e2a03a5b09638640b1`.

Evidencia privada anterior: `~/.codex/task-artifacts/ai-erp-workflows-strict-20261007/`. Repetición autorizada: `~/.codex/task-artifacts/ai-erp-workflows-verify-20261007/`, incluyendo ledgers, resultados, respuestas del proveedor con fixtures, `measurement-review.json`, checks, fingerprints y cierre del entorno. No contienen una copia de la base productiva y no se publican en Git. La nueva revisión recalcula 39/39 turnos seguros y dos replay sin llamadas; el prefijo de las 413 solicitudes anteriores permanece idéntico. Esto demuestra los controles medidos, no fiabilidad conversacional ni seguridad exhaustiva.

## Presupuesto y propuesta revisable

Mauricio amplió el techo conservador acumulado de USD 1.00 a **USD 1.50**, incluyendo USD 0.99570040 ya reservados. Las tres nuevas mediciones hicieron 67 solicitudes, sumando **480 acumuladas**, y reservaron USD 0.32577160 adicionales: **USD 1.32147200 acumulados**. Quedan USD 0.17852800 de reserva. Se detuvieron las llamadas tras los tres lotes autorizados; no se utiliza ese remanente para buscar una racha favorable.

La estimación conservadora por tokens de las llamadas de esta ampliación es USD **0.06162720**; la estimación histórica acumulada es USD **0.24489230**. No son cargos facturados ni reservas. Se usa la tarifa estándar sin descontar cache: USD 0.40 entrada y USD 1.60 salida por millón de tokens, verificada en [OpenAI](https://developers.openai.com/api/docs/models/gpt-4.1-mini). La clave existente se reutilizó sin copiarla ni modificar archivos de entorno.

El siguiente corte recomendado es probar el control nativo de selección de herramientas en el primer ciclo READ, con pruebas para nueva intención, avance parcial, ambigüedad, saludos, solicitudes fuera del alcance y gates apagados. [Responses permite controlar `tool_choice`](https://developers.openai.com/api/docs/guides/function-calling#tool-choice), pero exigir una llamada no prueba que el modelo escoja la correcta: el mismo UUID, la validación y la autorización del backend siguen siendo obligatorios. No implementar un despachador por keywords ni suplir datos faltantes. Este ajuste todavía no está implementado ni autoriza llamadas adicionales, cambios de modelo productivo o activación. Preparar y revisar el candidato antes de reservar otra evaluación completa; el remanente actual no demuestra cobertura suficiente de esa puerta.

## Recuperación y recursos

Rama de continuidad `codex/ia-erp-workflows-verificacion`; worktree `ai-erp-workflows-verify-20261007`; responsable `codex-erp-ai-root`. Se creó limpia sobre main `b2bea57b` y se trasladó el candidato como `3c74396e`, verificando los mismos hashes. El código se conserva para revisión. No se ha desplegado, por lo que producción no necesita rollback. Si posteriormente se publica, revertir este cambio de runtime/contrato LLM por Git manteniendo modelos, migración y datos técnicos existentes.

Entorno descartable anterior retirado: Compose `erp_ai_workflows_strict_20261007`, PG 55547 y preview 18098. Repetición: Compose `erp_ai_workflows_verify_20261007`, PG 55557 y volumen exclusivo `erp_ai_workflows_verify_20261007_postgres_data`. Se restauró el respaldo ficticio SHA-256 y se cotejó el hash de las cuatro tablas operativas antes de medir. Las 129 pruebas se repitieron sobre main vigente y candidato idéntico; checks sin errores ni migraciones pendientes/nuevas. No se creó otro preview ni pestañas. Su cierre y respaldo verificado se registran fuera de Git; no usar limpieza global ni retirar recursos ajenos. Un respaldo local de fixtures no demuestra recepción en el NAS.
