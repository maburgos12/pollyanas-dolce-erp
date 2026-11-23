# Ficha de fuentes — transporte privado Telegram de horarios

Fecha y ambiente: 2026-11-22 America/Mazatlan; PostgreSQL 16 aislado de la tarea
`telegram-hours-agent-20261122`, base `telegram_hours`, puerto local 55648.
No se consultaron registros de producción en esta fase.

## Necesidad y unidad de análisis

Una instrucción Telegram debe originar la solicitud ERP existente, conservar una vista
previa exacta y requerir aprobación del autor. El transporte necesita además un cursor por
bot y un registro durable por actualización recibida, incluso cuando no crea solicitud
(comandos, audio aún no transcrito, entrada ambigua o callback duplicado).

## Fuentes y decisión

| Concepto | Fuente/modelo | Identidad y consumidores | Evidencia local y decisión |
| --- | --- | --- | --- |
| Sucursal | `core.Sucursal` | PK ERP y código; ERP/tienda/Maya | Reutilizar, sin crear maestros ni equivalencias nuevas. |
| Alias | `SucursalAlias` | Alias activo normalizado; parser existente | 0 registros locales; usar `resolve_branch_token`, probar alias/códigos con fixtures aislados. |
| Google por sucursal | `SucursalPlataformaExterna` | Único sucursal/plataforma; publicador Google | 0 registros locales; selección sólo de diez códigos habilitados con configuración Google activa; `Todas` falla si falta alguno. |
| Intención y revisión | `SolicitudHorarioEspecial` | ID y revisión hash; ERP/API/publicador/lectores efectivos | 0 registros locales; reutilizar v2 `create_request_from_rules`, `approve_request`, `cancel_request`. |
| Auditoría de negocio | `HorarioEspecialBitacora` | Solicitud, actor, acción y revisión; ERP | 0 registros locales; se genera mediante servicios existentes, sin segunda bitácora de aprobación. |
| Cursor de transporte | Nuevo `TelegramHoursCursor` | Único `bot_id` | 0 registros; necesario porque ni solicitud ni auditoría identifican el próximo update Telegram reconocido. |
| Inbox y entrega | Nuevo `TelegramHoursUpdate` | Único `(bot_id, update_id)`, FK solicitud opcional | 0 registros; guarda input acotado, autor/chat/mensaje, estado, revisión/nonce/vencimiento y recibo de entrega; no duplica reglas ni resultados Google. |

## Alias y equivalencias

Los IDs Telegram y el usuario ERP son **identidades distintas**: su vinculación explícita
vive en `owner.json` privado, verificado previamente por el operador. El primer remitente
no crea una equivalencia ni recibe permisos. No se agregó otra persona ni cuenta ERP.
Los nombres de sucursal usan resolución existente; una coincidencia ambigua se rechaza.
La selección `Todas` se congela en el payload de la solicitud, no se recalcula al aprobar.

## Evidencia reproducible y límites

- Grafo MCP del proyecto base: descubrimiento de solicitudes, aprobación y parser;
  contrastado con los archivos vigentes del worktree.
- `manage.py inventario_fuentes_datos --term telegram --term horario`: 7 candidatos
  Django, metadatos PostgreSQL; no extracción de valores personales. El inventario se
  corrió después de aplicar migración 0002 y por eso incluye las dos tablas nuevas.
- Conteos `SELECT count(*)` exclusivamente sobre `special_hours_requests`,
  `special_hours_audit_log`, `special_hours_branch_aliases`,
  `special_hours_platform_configs`, `horarios_especiales_telegramhourscursor` y
  `horarios_especiales_telegramhoursupdate`: todos 0 en la base local de la tarea.
- Las pruebas crean sus propios fixtures en `test_telegram_hours`; esos datos no son
  evidencia de cobertura operativa de producción ni de publicación real.

## Riesgos y pendientes

Falta validar datos/configuración y consumidores en producción antes de activar. Los
recibos Google se leen de la solicitud existente; tienda y Maya permanecen pendientes de
verificación. Se conserva el entorno de esta tarea para audio y validación posteriores.
No hay nueva fuente de horarios: las dos tablas nuevas son exclusivamente transporte.
