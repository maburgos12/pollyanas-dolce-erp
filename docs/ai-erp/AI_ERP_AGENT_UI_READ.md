# F5.1 — Presentación de consultas y procesos del agente ERP

Fecha: 2026-10-08. Estado: candidato local para revisión, sin publicación ni activación en producción.

## Resultado y alcance

La pantalla existente `/ia-privada/` conserva su sesión, conversaciones, CSRF y endpoints. Presenta los resultados estructurados proyectados por el servidor como fichas de equipos, órdenes, fallas, planes y procesos propios. No interpreta el texto del LLM como una autorización ni como un comando de interfaz.

Este corte parte de `origin/main` b2bea57b; no incluye los candidatos no publicados F4.3/F4.4 ni cambia el modelo evaluado Sol. La autorización es para presentación y validación local. No se habilitaron usuarios, proveedor, flags, permisos o escritura operativa en producción.

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

Antes de publicar junto al candidato F4.3/F4.4, verificar sus contratos de continuación y vinculación de conversación; la prueba del controlador contra main no demuestra compatibilidad con una rama futura. Después, pilotar el flujo autenticado real con el proveedor aprobado y comprobar caché, permisos y referencias en producción. Una captura de fixture no prueba razonamiento Sol, Telegram ni funcionamiento operativo real.

## Reversión y puerta de publicación

No hay migraciones ni datos operativos a revertir. Para retirar el candidato local se conserva el commit de la rama dedicada. Si se aprueba una publicación posterior, deberá pasar revisión/PR, merge y script oficial de deploy, seguido de validación autenticada real. La reversión de código debe generar una nueva versión de SW para evitar servir presentación antigua. No cambiar flags ni `.env` como parte de este corte.
