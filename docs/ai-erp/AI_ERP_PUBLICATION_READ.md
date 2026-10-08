# Publicación del candidato READ y puerta del piloto

Fecha: 2026-10-08. Responsable: `codex-erp-ai-root`. Estado: preparación para revisión; sin merge, despliegue, activación ni llamadas al proveedor en esta tarea.

## Candidato preparado

La rama `codex/ia-erp-publicacion-read` parte de `ad2a6c99`, main vigente al iniciar. Contiene los seis commits del candidato local revisado `6b81936c`, sin cambios en sus quince archivos: continuidad estricta de procesos, selección de herramientas, recuperación de un pendiente fuera de la primera página y presentación de resultados/procesos. Este documento añade únicamente la propuesta de publicación. No agrega modelos, migraciones, dependencias, endpoints ni permisos.

La comprobación byte a byte conserva el código evaluado con Sol y la evidencia anterior. Las 44/44 interacciones de evaluación corresponden al candidato anterior al ajuste de recuperación de página 2; la integración posterior verificó ese ajuste con el proveedor real y mantuvo los veinte pendientes hermanos. No atribuir todo el corpus a una versión distinta. En esta rama pasaron 173 pruebas Django de runtime, workflows, chat legado, Gateway y acceso/pantalla; también las assertions Node de interfaz, `check`, `migrate --check` y `makemigrations --check --dry-run`. No hubo llamadas reales al proveedor. Los resultados se conservan en el expediente de la tarea. El navegador autenticado se verificó en el candidato idéntico anterior; no se abrió un nuevo servidor web para esta preparación.

## Prerrequisitos comprobados y límites

El expediente `ai-erp-nas-backup-completion-20261006` documenta recepción por SHA de los cuatro payloads, restauración PostgreSQL16, 427 referencias comprobadas y recuperación íntegra de 2,163 archivos desde una generación NAS. La cadena diaria y el correo de prueba quedaron verificados; no se repitió aquí la recuperación completa ni se probó un reinicio del NAS o un fallo real HBS. No se retiran originales históricos ni se purga la carpeta de recuperación que permanece en la papelera del NAS.

La inspección de producción de esta tarea comprobó repositorio limpio en `ad2a6c99`, PostgreSQL16.12, `check` sin problemas, `migrate --check` sin pendientes y aproximadamente 18 GiB libres. La clave OpenAI está configurada; sólo se consultó un booleano, sin leerla ni enviar solicitudes. Hay dos usuarios activos con acceso al chat existente, dato que no autoriza a ninguno para el piloto nuevo.

Los settings `AI_GATEWAY_ASSETS_ENABLED`, `AI_AGENT_READ_ENABLED` y `PRIVATE_AI_CHAT_MODEL` no están definidos en el proceso productivo; `AI_AGENT_WORKFLOWS_ENABLED` es False. Los dos primeros gates fallan cerrados por ausencia. Escribir esas variables en `.env` no basta mientras settings no las cargue. El candidato no implementa una lista de participantes ni una cuota monetaria persistente: los límites de seis respuestas, diez tools y sesenta segundos por turno no son un presupuesto económico. El guard privado de evaluación no forma parte del ERP publicado.

El runtime READ y el chat legado consultan actualmente `PRIVATE_AI_CHAT_MODEL`. Cambiar ese setting compartido puede cambiar también el proveedor del chat legado. El piloto debe separar la selección del modelo READ y verificar sus consumidores; no convertir una autorización del modelo evaluado en un cambio global del chat.

## Orden de publicación propuesto

1. Revisar y aprobar este PR; CI completo, diff acotado y validación PostgreSQL. Mantener el nuevo agente desactivado. La UI sí cambia la pantalla del chat existente y comparte el bump de caché del ERP; no afirmar que un gate del runtime oculta ese cambio visual.
2. Tras autorización de publicación, merge a main y `bash scripts/deploy_web_safe.sh`, sin `git pull` previo. Confirmar SHA servido, checks/migraciones, página autenticada, historial legado, solicitudes relevantes y consola, además de actualización del service worker. No hacer inferencias en producción ni activar settings en este paso. Detener el despliegue si aparece suciedad, una migración inesperada o un cambio ajeno sin revisar.
3. Preparar y revisar el corte de controles del piloto descrito abajo, inicialmente apagado. Validar consumidores con gates apagados y habilitados usando datos ficticios. Su aprobación debe incluir explícitamente implementación, publicación y activación limitada; este documento no las concede.
4. Sólo con esos controles aprobados y comprobados, ejecutar la ventana de consultas reales. Verificar desde una sesión autenticada de la cuenta aprobada y documentar cada criterio; ningún éxito de CI, preview o HTTP200 sustituye esa prueba.

## Corte de piloto que requiere aprobación

Alcance propuesto: una cuenta ERP verificada de Mauricio, exclusivamente consultas de activos y mantenimiento con `gpt-6.1-sol`, como en la evaluación. Hasta veinte turnos iniciados o USD1.00 de reserva conservadora acumulada, lo que ocurra primero; sin renovación automática. Este presupuesto productivo es una propuesta independiente del techo USD11.50 autorizado para evaluaciones locales. No garantiza veinte respuestas si se agota antes el presupuesto.

Archivos previsibles: `config/settings.py`, dispatch en `orquestacion/services/chat_service.py`, runtime READ, acceso del Gateway y workflows, tests de esos consumidores y documentación. Cualquier ampliación debe declararse antes de modificar. Reutilizar identidad, permisos, queryset autorizado, tablas técnicas de chat/workflows/auditoría y SDK instalado; revisar las fuentes antes de agregar captura técnica compartida. No se propone una nueva tabla ni migración: si los contratos existentes no permiten la reserva duradera, presentar esa decisión antes de implementarla.

Controles necesarios:

- Settings con defaults apagados y lista explícita de usuarios ERP, aplicada en servidor a chat, catálogo, ejecución y recuperación de procesos. La lista sólo restringe, nunca concede permisos ni sucursales. Mantener el comportamiento legado para usuarios que no participan, sin enviarles Sol ni exponer resultados del piloto.
- Modelo exclusivo READ, sin cambiar el modelo compartido legado. Clave existente sólo en servidor; Responses `store=False`, cero reintentos automáticos y ningún servicio de SQL, web o computer-use.
- Reserva económica persistente y atómica antes de cada request, válida entre conversaciones, reinicios y workers. Rechazo previo al proveedor al alcanzar el límite; no liberar una reserva cuando no pueda probarse que una solicitud fallida no fue cobrada. Reutilizar las cotas y tarifas documentadas del evaluador sólo después de revisar su compatibilidad productiva. Separar reserva, uso observado y factura: una estimación no demuestra el cargo real.
- Limitar datos enviados a DTOs autorizados de activos/mantenimiento y estado técnico acotado. PDFs, fotos, audio, nómina, pagos, ventas y datos DG quedan fuera de este corte.
- Permitir únicamente las tres tools READ y las tools técnicas de sus procesos. Las únicas escrituras previstas son conversación, workflow, resultados y auditoría; no registros operativos, aprobaciones, borradores comerciales o jobs de sincronización.

Pruebas del corte: usuario incluido/excluido/revocado, sucursal y campos financieros, chat legado con modelo intacto, permisos cambiados entre turnos, ambigüedad y datos faltantes, mismo UUID entre chats/páginas, replay, injection, exceso de presupuesto, concurrencia entre dos chats, timeout y error de red. Probar que se rechaza antes de contactar al proveedor y que no se modifican tablas operativas. Fallar cerrado ante configuración incompleta, auditoría fallida o reserva no verificable.

Aceptación en la ventana real: consulta de ficha/mantenimiento con fuente; selección ambigua; continuidad en otro chat con el mismo UUID; recuperación de un pendiente antiguo; rechazo de una operación fuera de READ; cuenta no participante preservada; costos/alcance revalidados; reservas y uso registrados; ninguna modificación operativa. Revisar estados ERROR/incompletos y llamadas del proveedor, no sólo texto de respuesta. Detener ante una filtración, herramienta prohibida, reserva excedida o falta de evidencia.

## Reversión y recursos

Ante un fallo del piloto, detener nuevas inferencias con el gate efectivo del runtime y comprobarlo mediante una solicitud autenticada sin llamar al proveedor. No configurar un fallback hacia tools operativas para una solicitud READ rechazada. Conservar conversaciones, reservas, workflows y auditoría para explicar lo ocurrido.

Para revertir este candidato publicado, usar un revert revisado en Git y el deploy oficial; preservar tablas y migraciones existentes. El rollback visual necesita una nueva versión del service worker para no conservar assets incompatibles. Confirmar SHA, caché y pantalla autenticada después de la reversión. No borrar estado técnico para ocultar un fallo.

Esta preparación usa PostgreSQL16 local propio: proyecto `erp_ai_publicacion_read_20261008`, puerto55601, sin datos de producción ni servidor web. Antes de entrega se detiene y retira exclusivamente ese entorno con respaldo verificado por el helper de tareas cerradas. El código/PR y evidencia quedan a cargo de `codex-erp-ai-root`, revisión2026-10-10. El candidato no está desplegado; producción no necesita rollback por esta tarea.

Telegram, archivos, voz y acciones transaccionales mantienen sus fases posteriores. Este corte no los implementa ni los presenta como disponibles.
