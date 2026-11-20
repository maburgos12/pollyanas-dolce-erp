# Mac-IA-02 — integración automática y seguimiento en Pastelerías

Estado: diseño aprobado expresamente por Mauricio el 2026-11-19 para implementar,
integrar, desplegar y activar; consultar el runbook para la operación vigente.
Solicitud de Mauricio: conectar la Mac configurada a las tareas automáticas de
Pastelerías, reducir uso de modelos de pago y ver qué está haciendo.

## Evidencia y alcance elegido

Producción se consultó en solo lectura con PostgreSQL verificado. Al corte
2026-11-20 00:17 UTC (19 de noviembre en Mazatlán) hay seis agentes activos y
cuatro reglas con corridas en los últimos siete días. En ese intervalo se
observaron 564 tareas de la guardia de ajustes para seis ajuste_id distintos,
sin identificadores ausentes. Una tarea generada no equivale a un caso nuevo.

Las reglas programadas examinadas son Python/SQL y no llaman un modelo. El
recalculo mensual de rentabilidad tampoco invoca IA en su implementación actual;
los comentarios históricos no prueban esa llamada. Hay llamadas de pago en
consultas y diagnósticos, pero no se ha demostrado su consumo facturado actual.

La primera activación será un resumen automático de seguimiento de las tareas
existentes de Orquestación: guardia de ajustes y cadena operativa del plan. La
Mac preparará un texto breve con referencias para que Dirección y Codex consulten
un paquete compacto. No se sustituye el chat financiero ni el piloto financiero,
ni se habilitan reglas de negocio que hoy no tienen runner.

## Alternativas consideradas

1. **Reutilizar Django/Celery, sus tareas y su panel**, con un ejecutor pequeño
   en el VPS que transporte solicitudes al modelo local. Recomendado: conserva
   el orquestador, las identidades, permisos e historial; las credenciales del
   modelo no entran al contenedor web.
2. Conectar HTTP del contenedor directamente a la Mac: menos código de transporte,
   pero exige exponer la escucha al puente Docker e introducir configuración y
   secretos del proveedor dentro de la aplicación.
3. Crear un sistema externo de agentes y dashboard: introduce otra cola, historial
   y permisos que habría que conciliar con el ERP. No se elige.

## Diseño recomendado

### Trabajo y fuentes

- Django/Celery conserva la programación. Una rutina horaria prepara únicamente
  lotes con evidencia nueva o modificada; no llama al modelo si no hay cambios.
- Reutilizar OrchestrationRun y AgentTask para registrar el trabajo local. La Mac
  es un ejecutor identificado como mac-ia-02, no otro dueño del dato operativo.
- Agrupar ajustes por ajuste_id; conservar separados los roles de seguimiento de
  un plan. No inferir identidad por título, nombre o importe.
- Mantener vínculo a los registros fuente. Una tarea pendiente de Orquestación no
  prueba que el ajuste siga sin autorización: mostrar ambas naturalezas con claridad.
- Enviar al modelo sólo referencias, estados y descripciones acotadas. Excluir
  usuarios, nómina, bancos, credenciales, adjuntos y cantidades financieras.
- La salida es un comentario para revisión; no cambia los estados originales.

### Ejecutor y conexión

- Un servicio pequeño en el VPS toma de la cola sólo trabajos de resumen local,
  uno a la vez, con límite de tiempo, tamaño y salida.
- Usa comandos de gestión Django con parámetros fijos y JSON por stdin; no hay
  endpoint público nuevo para ejecutar tareas ni comandos elegidos por el modelo.
- Un segundo túnel SSH saliente, independiente del de administración, publica
  únicamente 127.0.0.1:18125 del VPS hacia 127.0.0.1:18124 de la Mac.
- Cuenta de transporte propuesta erp_mac_ia_02_llm: llave exclusiva, sin consola,
  contraseña, TTY, forwarding local ni sockets Unix; sólo ese puerto privado.
- La credencial de inferencia se guarda en un archivo privado del VPS, fuera de
  Git. No se copian al VPS las llaves de acceso a la cuenta personal de la Mac.
- No se cambian puertos Compose ni .env del ERP, y no se abre el modelo a Internet.
- Si la Mac está apagada, sin red o rechaza la respuesta, se registra bloqueado o
  fallido y se reintenta con límite. No se llama automáticamente a una API de pago.

### Idempotencia y recuperación

- Firma de entrada basada en entidad y evidencia estable; los nuevos IDs de
  corridas repetidas no obligan a analizar de nuevo el mismo caso.
- Un trabajo tiene una sola reclamación vigente. Una respuesta tardía de un
  intento vencido no puede sobrescribir el resultado de un intento posterior.
- Reintentos acotados para trabajo de lectura. Nunca se repiten operaciones de
  inventario, pagos o autorizaciones como parte de este flujo.
- La caída del ejecutor no bloquea las reglas originales ni el ERP.

### Visibilidad en el ERP

Añadir al panel de Orquestación un bloque Mac-IA-02 con:

- trabajo actual y fase real: en cola, preparando fuentes, esperando modelo,
  validando, terminado, bloqueado o fallido;
- inicio, duración y última actividad conocida;
- historial reciente, referencias a fuentes y comentario resultante;
- modelo, tokens locales de entrada/salida y motivo de fallo sin secretos;
- casos agrupados y trabajos omitidos por entrada sin cambios.

Actualizar ese bloque sin recargar toda la página ni perder filtros o scroll.
Se conserva el permiso existente para ver Orquestación. No se muestran porcentajes
inventados de avance ni se presenta el éxito de la IA como cierre de un ajuste.

Los tokens locales procesados son medibles. El ahorro de facturación y las llamadas
pagadas evitadas se presentan como no demostrados hasta tener una comparación real;
una regla que ya era Python no cuenta como tokens de pago ahorrados.

## Archivos y contratos previstos

- orquestacion: servicio de preparación/reclamación/resultado, tarea Celery,
  comando de gestión y proyección del estado para el panel.
- templates/orquestacion/dashboard.html y su fragmento o JS acotado.
- pos_bridge/management/commands/setup_celery_schedules.py para el alta idempotente
  de la rutina técnica; no activar reglas ajenas.
- ops/erp-mac-ia: ejecutor, unidad/timer e instalación verificable por Git.
- docs/runbooks/mac-ia-02.md y ficha de fuentes de esta integración.
- Tests de idempotencia, reclamaciones, datos inválidos, fallo de red y permisos.

No se prevé una tabla nueva ni migración de datos operativos. Si las fuentes
inspeccionadas no soportan un requisito de integridad, se revisará esa decisión
antes de introducir un esquema nuevo.

## Aceptación y publicación

1. Pruebas con PostgreSQL 16 aislado y recursos registrados para esta tarea.
2. Verificar que duplicados y entradas sin cambios no crean llamadas nuevas.
3. Verificar desconexión, tiempo límite, JSON inválido y resultado tardío.
4. Comprobar que no se invoca proveedor de pago ni se alteran tablas operativas.
5. CI completo del SHA final; integrar y desplegar por el flujo oficial.
6. Activar la rutina técnica y verificar al menos un trabajo real de extremo a
   extremo, con el resultado visible en el panel y sus métricas locales.
7. Retirar el entorno de prueba propio; conservar sólo los servicios operativos
   autorizados y la evidencia de sus propietarios.

## Límites

FileVault puede exigir desbloqueo local después de reiniciar. El modelo 8B tarda
decenas de segundos en tareas pequeñas: se utilizará en lotes acotados. Un panel
con último trabajo exitoso no prueba que la Mac esté conectada ahora; la UI debe
indicar cuándo fue la última comprobación efectiva.
