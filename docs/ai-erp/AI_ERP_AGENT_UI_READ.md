# F5.1 — Presentación de consultas y procesos del agente ERP

Fecha: 2026-10-08. Estado: candidato local para revisión, sin publicación ni activación en producción.

## Resultado y alcance

La pantalla existente `/ia-privada/` conserva su sesión, conversaciones, CSRF y endpoints. Presenta los resultados estructurados proyectados por el servidor como fichas de equipos, órdenes, fallas, planes y procesos propios. No interpreta el texto del LLM como una autorización ni como un comando de interfaz.

La presentación F5.1 partió de `origin/main` b2bea57b. El corte de integración local del 8 de octubre incorpora también los candidatos F4.3/F4.4 evaluados con Sol, sin modificar sus contratos REST ni su modelo. La autorización sigue limitada a integración y validación local. No se habilitaron usuarios, proveedor, flags, permisos o escritura operativa en producción.

| Antes | Después | Motivo |
|---|---|---|
| Texto y resumen genérico de herramienta | Ficha o tabla con estado, fuente, fecha y límites | Distinguir información consultada de una ejecución pendiente |
| Pendientes accesibles por herramientas | Sección de procesos propios con filtro y páginas | Conocer el dato faltante y preparar una continuación |
| Compositor y navegación simultáneos | Bloqueo durante apertura, creación o envío | Evitar asociar una respuesta con otra conversación |
| EOF de SSE tratado como final | Exige evento terminal; no reintenta automáticamente | No mostrar una conexión interrumpida como consulta completada |
| Historial expandido al entrar desde móvil | Historial inicialmente colapsado, accesible manualmente | Conservar espacio para la conversación |

## Reutilización y consumidores

- `templates/orquestacion/chat.html`: controlador y markup actuales, sin framework nuevo.
- `static/css/template_modules/templates-orquestacion-chat.css`: estilos locales; tokens y navegación global vigentes.
- `static/js/orquestacion/agent-results.js`: presentación de DTO existentes, todo texto escapado.
- `orquestacion/tests_agent_results_ui.js`: asserts con Node estándar; controlador real en VM y DOM mínimo, sin dependencias.
- `static/erp-sw.js`, `templates/base.html`, `core/templates/core/login.html`: únicamente versión de caché/registro, requerida para distribuir el cambio visible. No cambia la política de fetch ni autenticación.

Fuentes reutilizadas: Gateway de activos y mantenimiento, `orquestacion.AgentWorkflow`, servicios y proyecciones READ existentes. No hay segunda tabla maestra ni captura alternativa. Consultar también `docs/data-reuse/ai-erp-workflows-read.md`.

El navegador usa GET del listado de procesos con `page`, `page_size` y `status`. No usa parámetros inexistentes, PATCH ni comandos de ejecución para el botón Continuar. El botón únicamente prepara una referencia UUID en el compositor y pide revisar y enviar. La autorización y validación efectiva siguen del lado servidor.

## Seguridad y exactitud

- Costos únicamente cuando la proyección incluye `puede_ver_costos === true`; el navegador no concede permisos.
- Datos de una herramienta con error o aprobación pendiente no se muestran como ejecutados.
- Las opciones revocadas conservan su posición; no se selecciona automáticamente otra.
- Se distinguen vencidos, próximos, sin fecha e inactivos/pausados; resultados vacíos conservan horizonte y advertencia sobre ausencia de servicio.
- Una lista vacía significa página sin procesos visibles, no ausencia global de procesos.
- Fuentes y fechas de consulta no se confunden con la fecha de actualización del workflow.
- Texto de modelo y documentos se escapa; no se introduce Markdown HTML ni enlaces arbitrarios.
- EOF, JSON inválido o fallo de transporte no provocan un reenvío automático. Se recomienda abrir la conversación para verificar el resultado persistido.

## Validación local

PostgreSQL 16.11 aislado, migraciones de main aplicadas antes de modificar, `check` sin problemas, `migrate --check` sin pendientes y `makemigrations --check --dry-run` sin cambios.

142 pruebas Django de chat, runtime READ, workflows, Gateway de activos y guardrails; assertions Node de escape/XSS, proyección de costos, estados terminales, fuentes, historial parcial, consulta anidada, planes vacíos, campos faltantes, SSE truncado, ausencia de reintentos, bloqueos de navegación y doble creación.

Preview autenticado con usuario, sucursal y equipos ficticios; proveedor OpenAI simulado, servicios Gateway y DB reales locales. El preview no envió consultas a OpenAI ni accedió al VPS. Vista revisada a 1440×1000, 900×1000 y 390×844, sin desbordamiento horizontal del documento. Envío confirmado con POST SSE, GET de detalle y lista; respuesta persistida y controles recuperados. Continuar no produjo solicitudes de red. Filtro y paginación revisados. Consola sin errores durante el flujo nominal. Interrupción del servidor local usada para comprobar error del listado; recuperación mediante Actualizar.

Evidencias y registro privado: `/Users/mauricioburgos/.codex/task-artifacts/ai-erp-f5-resultados-20261008/`. Capturas y logs no se incluyen en Git. Los recursos locales se registran y retiran conforme al cierre de entornos, conservando respaldo verificado de los fixtures.

## Límites y próximo corte

Esto completa la presentación F5.1, no la fase F5 completa ni el agente operacional completo. El SSE actual sigue ejecutándose sincrónicamente en el servidor y entrega eventos después; no se ofrece cancelación de trabajo ni prueba de streaming durante tool execution. No hay nuevas escrituras, adjuntos, voz o Telegram en este corte.

No se inventó un link al pasaporte del activo: el DTO no contiene el token/ruta canónica requerida. Hace falta una proyección autorizada antes de ofrecer ese enlace. Tampoco se cambia el contrato de paginación para inventar totales o `has_more`.

La compatibilidad local con F4.3/F4.4 se comprobó en el corte descrito a continuación. La publicación y el piloto autenticado sobre datos reales siguen pendientes de aprobación; requieren comprobar caché, permisos y referencias en producción. Una captura de fixture no prueba Telegram ni funcionamiento operativo real.

## Integración F4.3/F4.4 + F5.1 con Sol real

Rama `codex/ia-erp-sol-ui-integracion`, base b2bea57b. Se integraron los cinco commits de los candidatos previamente revisados sin conflictos. Se actualizaron los textos esperados por siete pruebas de `core/tests_ai_private_hub.py`, conservando sus verificaciones de acceso y comprobando el envío deshabilitado cuando falta conexión. La revisión independiente detectó y corrigió además la recuperación de referencias fuera de la primera página, descrita abajo. No se introdujeron tablas, migraciones, endpoints, dependencias o capturas operativas.

Se comprobó desde `/ia-privada/` con usuario no administrador, sucursal y datos ficticios en PostgreSQL 16 aislado, y solicitudes reales al proveedor `gpt-6.1-sol`:

| Interacción | Resultado persistido |
|---|---|
| Conservar revisión de mantenimiento sin identificar máquina | Un proceso propio, `WAITING_INFORMATION`, versión 1 |
| Abrir otro chat y continuar el UUID indicando «Era un horno» | Mismo proceso, `WAITING_SELECTION`, versión 2, dos opciones autorizadas |
| Elegir explícitamente la opción 2 | Mismo proceso, `COMPLETED`, versión 4; lectura del Horno dos y su plan |
| Recargar pantalla y filtrar Completados | Resultado y proceso recuperados, sin duplicar el workflow ni repetir la inferencia |

El servidor revalida actor, permisos y conversación activa al continuar. El workflow conserva la conversación de origen como referencia; no exige retomarlo en ese chat. La entrada `continuation` estricta del LLM se adapta al servicio existente; el navegador sólo prepara el UUID y no ejecuta al pulsar Continuar.

Evidencia: un workflow en dos conversaciones, siete solicitudes Responses, cero errores del proveedor; `option_position: 2` y `expected_version: 2` observados en el tool call real. El modelo solicitó además una lectura del contexto del mismo activo; ambas proyecciones aparecen en la respuesta, sin escritura operativa. El SHA de las cuatro tablas operativas consultadas permaneció idéntico antes/después. El activo de otra sucursal y su costo restringido no aparecen en los mensajes persistidos ni en la pantalla. Las escrituras técnicas de conversaciones, workflow y auditoría son esperadas.

Validación combinada: 150 pruebas Django del núcleo y guardrails, siete de acceso/pantalla y assertions Node de interfaz; `check`, `migrate --check` y `makemigrations --check --dry-run` sin problemas. Consola sin errores; tres POST SSE con respuesta 200 y recuperación posterior mediante GET. Escritorio 1440×1000 y móvil 390×844 sin desbordamiento horizontal. El móvil conserva el historial inicialmente colapsado.

La revisión encontró que un UUID escogido en la página 2 podía faltar entre los primeros 20 pendientes enviados al modelo. El runtime ahora hidrata exactamente un UUID distinto presente en el mensaje mediante `get_workflow`, que revalida propiedad, acceso y recursos vigentes. Lo prioriza antes de aplicar los mismos límites de 20 DTOs/30k caracteres y el mismo registro de IDs/proof. No infiere intención por keywords ni ejecuta acciones al encontrar una referencia. UUIDs múltiples no seleccionan un destino; referencias ajenas, revocadas, desconocidas o terminales no añaden sus datos. No cambia el contrato REST ni el contrato de tools del LLM.

Tres pruebas adicionales cubren el pendiente 21 con versión fresca y contexto grande, referencias denegadas/expiradas y elección ambigua. La regresión falla contra la función original con el fixture JSON correcto; las 160 pruebas combinadas pasan con la corrección. La revisión independiente del diff no dejó hallazgos abiertos.

Sobre la huella corregida se eligió en la pantalla un pendiente de página 2 desde otra conversación. Sol recibió ese UUID primero, usó su versión fresca y completó el mismo proceso (`READY` v1 → `COMPLETED` v3). Los 20 pendientes hermanos quedaron intactos; recargar recuperó la respuesta completa. El primer intento de esta prueba completó la lectura, pero el guard privado de evaluación rechazó el contexto de la respuesta posterior por superar 20k bytes. Se conserva ese intento con mensaje en error; no se contabiliza como flujo completo. Se ajustó únicamente el límite del evaluador privado a 30k bytes, manteniendo todos los techos de gasto/solicitudes, y se repitió el caso con nuevos fixtures. No se relajó ningún límite del producto. La evidencia distingue ambas huellas e intentos; no atribuye a esta corrección los 44/44 turnos de la evaluación histórica de Sol.

El presupuesto acotó esta prueba a 16 solicitudes y USD 1.00 de reserva adicional, dentro del techo acumulado ya autorizado de USD 11.50. Se usaron siete solicitudes, 16,604 tokens de entrada y 520 de salida; reserva adicional USD 0.3725725, acumulada USD 6.46692770. Estimación conservadora sin descuento de caché, incluyendo 4,082 tokens de escritura de caché: USD 0.046572 a las tarifas del plan de evaluación; no es una factura. El prefijo del ledger anterior se verificó por SHA antes y después de añadir las reservas.

Incluyendo la prueba de página 2 y su intento detenido por el evaluador: diez solicitudes, 32,156 tokens de entrada, 832 de salida y 11,092 de escritura de caché; estimación conservadora total USD 0.094816, reserva del corte USD 0.5940100 y acumulada USD 6.68836520. Todos los techos permanecen satisfechos; el hash operativo y el prefijo del ledger anterior permanecen iguales. El ajuste del guard no accedió a producción ni copió la clave a archivos.

Evidencias privadas: `/Users/mauricioburgos/.codex/task-artifacts/ai-erp-sol-ui-integration-20261008/`, particularmente `integration-result.json`, `provider-results.json`, `browser-network.json`, capturas y registro de ejecución. No se versionan credenciales, capturas ni logs. Los recursos locales se retiran con respaldo recuperable y verificación de propiedad; el código y las evidencias se entregan a `codex-erp-ai-root`, revisión 2026-10-10.

## Reversión y puerta de publicación

No hay migraciones ni datos operativos a revertir. Para retirar el candidato local se conserva el commit de la rama dedicada. Si se aprueba una publicación posterior, deberá pasar revisión/PR, merge y script oficial de deploy, seguido de validación autenticada real. La reversión de código debe generar una nueva versión de SW para evitar servir presentación antigua. No cambiar flags ni `.env` como parte de este corte.
