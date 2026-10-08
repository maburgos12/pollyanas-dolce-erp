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


## F4.4 — selección de herramientas y modelo recomendado (2026-10-08)

Estado: corrección local revisada, sin PR, push, merge, despliegue ni activación. Se reutiliza el candidato F4.3 sobre main `b2bea57b`, con cambios adicionales únicamente en runtime, pruebas y este documento. No se crean dependencias, tablas, migraciones, permisos, variables de entorno ni herramientas nuevas.

Con F4 habilitado, el primer ciclo usa `tool_choice="required"`; los siguientes usan `auto`. `parallel_tool_calls=False` serializa las llamadas para no avanzar simultáneamente versiones de un workflow. Con F4 apagado se conserva la solicitud anterior. La interpretación permanece en el modelo; no hay dispatcher por palabras clave. El backend conserva autorización fresca, propiedad, contratos estrictos, versión, auditoría y cierre basado en evidencia. Exigir una herramienta puede satisfacerse con listar/buscar y no garantiza la continuación correcta.

135 pruebas con PostgreSQL 16 pasan en dos ejecuciones, incluyendo el consumidor chat_service, runtime READ, workflows y Gateway de activos. Las pruebas nuevas fallaron antes del cambio. `check`: cero errores; `migrate --check`: sin pendientes; `makemigrations --check --dry-run`: sin cambios. El runner de pruebas muestra la advertencia preexistente de reglas críticas de ProductBusinessRule ausentes en fixtures; el check de la base de evaluación no presenta incidencias.

La evaluación real acotada con GPT-4.1 mini se congeló antes de las solicitudes. Corrigió **5/5 turnos** de las tres fallas históricas: aportar el equipo desde otro chat, guardar una consulta sin equipo y continuar la selección con el mismo UUID. Sin embargo, sólo pasó **6/8 turnos**: dos saludos reanudaron pendientes sin intención expresa. Aunque las tablas operativas no cambiaron, esa modificación técnica es una regresión. Se añadió una instrucción explícita para usar listado en saludos/peticiones fuera de READ sin preparar ni reanudar. La repetición final de esos dos saludos pasó **2/2**, conservando UUID, estado, versión y contenido. Son dos etapas del candidato: los cinco turnos de continuidad preceden al último ajuste de prompt. No se suman como un corpus final completo ni se declara aceptación global. Falta repetir el corpus y la generalización con el candidato final y el modelo seleccionado.

Una primera medición cargó el checkout raíz por el cwd del evaluador y no el candidato. Se conserva como **inválida para juzgar esta corrección**, con sus reservas contabilizadas: 15 solicitudes, reserva USD 0.06831800, estimación USD 0.0095800. Se corrigió el cwd y se añadió una comprobación de ruta del módulo cargado y presencia de la política antes de consultar. Los dos lotes válidos posteriores hicieron 16 y 4 solicitudes. Total histórico: **515 solicitudes**, reserva conservadora **USD 1.48884520**, remanente **USD 0.01115480** dentro del techo USD 1.50. Las nuevas estimaciones por tokens suman USD 0.0291424, incluido el lote inválido; no son cargos facturados. No se hicieron más llamadas después de esos lotes.

Los hashes de Activo, PlanMantenimiento, OrdenMantenimiento y ReporteFalla permanecen iguales antes/después en los tres lotes: `d324abd7d3a027aadecc569f26c3eff1dd2469a3d6845156406f742726ee7904`. Los checks medidos de seguridad operativa pasan; esto no convierte las fallas de intención en casos correctos ni garantiza seguridad exhaustiva. Los ledgers anteriores se conservan byte por byte como prefijo.

Revisión independiente del diff: sin defectos nuevos identificados de ACL, replay o compatibilidad con F4 apagado. Las pruebas con dobles no prueban la elección real del modelo. Navegador local autenticado y API de conversación 200: al conceder un permiso adicional al usuario ficticio, cambió el fingerprint y el historial READ anterior se ocultó correctamente; consola de la pantalla final sin errores/advertencias. Esta comprobación demuestra proyección tras cambio de acceso, no una validación visual exitosa del proceso completo ni producción. El envío está deshabilitado en el preview sin clave; la presentación de resultados sigue pendiente para F5.

**Modelo recomendado para evaluar como núcleo: [GPT-6.1 Sol](https://developers.openai.com/api/docs/models/gpt-6.1-sol)**. La documentación oficial lo orienta a trabajo complejo profesional con un equilibrio de capacidad/costo, admite Structured Outputs, imágenes y function calling mediante Responses. Es recomendación documental, no resultado medido en este ERP ni garantía de disponibilidad de la cuenta. No sustituye los servicios, permisos ni estados del ERP. GPT-6 Luna puede estudiarse posteriormente para tareas acotadas y voluminosas; no se agrega un router entre modelos en este corte. Antes de cambiar el modelo, verificar acceso, costo, latencia, esfuerzo de razonamiento soportado y límites de salida: Sol soporta `low` a `max`, no `none`/`minimal`; el runtime actual limita la salida a 1400 tokens y ese límite requiere calibración para razonamiento. [Referencia de tool choice y ejecución paralela](https://developers.openai.com/api/docs/guides/function-calling#tool-choice).

Evidencia privada: `/Users/mauricioburgos/.codex/task-artifacts/ai-erp-tool-choice-20261008/`, con corpus congelados, resultados válidos e inválidos, ledgers, hashes, logs, revisión y cierre del entorno. Worktree `ai-erp-tool-choice-20261008`, rama `codex/ia-erp-tool-choice-read`, responsable `codex-erp-ai-root`. Compose exclusivo `erp_ai_tool_choice_20261008`, PostgreSQL 55567, preview 18108. La recuperación usa el archivo ficticio verificado del corte anterior. El cierre retirará únicamente estos recursos tras respaldo verificado; las ramas de candidatos se conservan documentadas para revisión. Ninguno de estos respaldos locales acredita recepción en NAS. Rollback productivo no aplica: no se desplegó. Si se aprueba publicar posteriormente, la reversión será por Git, conservando estado técnico y tablas existentes.


## F4.5 — evaluación real de GPT-6.1 Sol (2026-10-08)

Estado: **puerta de aceptación local READ aprobada**, con datos ficticios. No se publica, despliega ni activa el modelo. El candidato conserva exactamente el árbol F4.4 `8d3c7d6e83a9169c38403e59a8e8ff42ffd15ec9`, trasladado a la rama `codex/ia-erp-sol-evaluacion`, HEAD de código `50888e32`. No cambia runtime, dependencias, contratos, permisos, configuración ni migraciones durante esta evaluación. El único nuevo cambio del repositorio es este resultado.

| Etapa congelada | Interacciones correctas | Solicitudes Responses | Reserva conservadora USD |
| --- | --- | --- | --- |
| Compatibilidad, dato faltante | 1/1 | 2 | 0.09758000 |
| Primera repetición | 16/16 | 32 | 1.64313000 |
| Segunda repetición consecutiva | 16/16 | 32 | 1.64405750 |
| Muestra adicional congelada | 7/7 | 15 | 0.79483750 |
| Saludos y solicitudes prohibidas con pendientes | 4/4 | 8 | 0.42590500 |

Las 43 interacciones de aceptación, más una de compatibilidad, pasan: **44/44**. La muestra adicional ya se había usado en evaluaciones anteriores; no es un conjunto nunca visto. Dos controles de replay pasan con cero llamadas adicionales al proveedor. No se repiten lotes para buscar una racha favorable. No constituye una comparación controlada contra mini sobre el mismo candidato final.

La medición usa UUID original, estado, versión, contenido y conservación de pendientes hermanos. Verifica selección y continuación parcial entre chats, permisos de sucursal y costos, rechazo persistido de un workflow ajeno, datos hostiles en el nombre del activo, solicitudes de escritura prohibidas y compatibilidad con gates apagados. Los cuatro casos finales exigen además cero intentos persistidos de preparar/reanudar workflows; listar pendientes sí está permitido. Se endureció el evaluador privado para exigir posiciones consecutivas sin duplicados y activo seleccionado igual al estado. Los contraejemplos fallan; el primer lote se recalcula offline con el criterio final, sin repetir llamadas pagadas ni cambiar prompts/runtime.

`measurement-review.json` recalcula los cinco lotes contra la evidencia persistida en PostgreSQL y el snapshot autorizado de cada solicitud: 44/44, prefijo histórico íntegro y cero llamadas de revisión. Revisión independiente sin defectos P1/P2 identificados. Activo, PlanMantenimiento, OrdenMantenimiento y ReporteFalla conservan en todos los lotes el hash `d324abd7d3a027aadecc569f26c3eff1dd2469a3d6845156406f742726ee7904`. No quedan sugerencias ni herramientas ejecutándose. Estos controles observados no garantizan seguridad exhaustiva.

135 pruebas locales seleccionadas sobre PostgreSQL 16 pasan: workflows, runtime READ, Gateway y chat_service. `check`: cero errores; `migrate --check`: cero pendientes; `makemigrations --check --dry-run`: sin cambios. El runner mantiene la advertencia preexistente de ProductBusinessRule ausentes en fixtures. El check de la base de evaluación no presenta incidencias.

### Compatibilidad, consumo y límites

Las **89 solicitudes nuevas** responden con modelo exacto `gpt-6.1-sol`, estado `completed`, sin errores ni respuestas incompletas. Se reutilizan Responses/SDK y la clave previamente autorizada, sin copiarla. `store=False`, cero reintentos, tier estándar, salida máxima 1400, esfuerzo predeterminado del modelo; sin adaptadores ni router. Las respuestas contabilizan 142 tokens de razonamiento en total: el alcance probado es tool calling READ y continuidad; no acredita análisis empresarial complejo.

Se midieron 202722 tokens de entrada y 6908 de salida. Estimación estándar entrada/salida sin descuentos: **USD 0.474524**; incluyendo el incremento de tarifa de 36623 tokens de escritura de caché y sin descontar 158740 tokens de lectura de caché: **USD 0.4928355**. Aplicando los detalles reportados de caché, la estimación sería USD 0.1912295. Son estimaciones de consumo, no cargos facturados. Tarifas verificadas: entrada USD 2, lectura de caché USD 0.10, escritura de caché USD 2.50 y salida USD 10 por millón: [OpenAI pricing](https://developers.openai.com/api/docs/pricing).

Mauricio autorizó USD 10 adicionales al techo histórico USD 1.50: máximo acumulado **USD 11.50**. El guard registra reservas conservadoras antes de cada llamada, usando bytes como cota de entrada y tarifa superior de escritura de caché. Las reservas nuevas suman **USD 4.60551000**; histórico acumulado **USD 6.09435520**, 604 solicitudes, conservando exactamente el prefijo de 515. El remanente no autoriza experimentos adicionales fuera de este plan; las llamadas concluyen tras las cinco etapas previstas. Autocomprobaciones sin red rechazan exceso de presupuesto, modelo distinto, almacenamiento habilitado, salida ampliada y contexto excesivo antes de reservar/llamar.

Latencia por solicitud Responses: mediana 2.978 s, p95 6.862 s (nearest-rank), máximo 12.371 s; no representa duración por interacción, concurrencia, streaming o SLA productivo. Los límites de salida/razonamiento necesitan otra calibración si se incorporan tareas más extensas.

### Recuperación y siguiente corte

Evidencia privada y registro de recursos: `/Users/mauricioburgos/.codex/task-artifacts/ai-erp-sol-evaluation-20261008/`. Worktree `ai-erp-sol-evaluation-20261008`, responsable `codex-erp-ai-root`, revisión 2026-10-10. Compose exclusivo `erp_ai_sol_evaluation_20261008`, PostgreSQL sólo 127.0.0.1:55577; fixtures recuperados desde respaldo local con SHA-256 y miembros verificados. El cierre retiró únicamente su contenedor, volumen, red e índice efímero, tras respaldo recuperable SHA-256 y 9228 miembros verificados; el puerto está cerrado. Conserva código, evidencia y respaldo ficticio de 30 MB con responsable y revisión 2026-10-10. La variación observada de espacio libre del Mac se registra con sus límites de atribución. La recepción en NAS no forma parte de esta medición.

Sol es ahora un candidato medido para este núcleo READ. El siguiente corte debe preparar la presentación integrada de pendientes/resultados y una prueba supervisada en el flujo autorizado antes de activar usuarios o cambiar configuración productiva. No se verifica navegador/UI en esta fase ni Telegram, voz, archivos, transacciones, nómina, altas de empleados o analítica avanzada. No hay PR, push, merge o despliegue; no requiere rollback productivo. Una futura publicación debe respetar Git, gates y aprobaciones del proyecto.
