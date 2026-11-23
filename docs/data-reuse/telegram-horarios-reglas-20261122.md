# Ficha de fuentes — reglas de horario por Telegram

Fecha: 2026-11-22. Desarrollo en PostgreSQL 16 aislado, proyecto
`erp_telegram_hours_agent_20261122`, dueño `codex-google-hours`, puerto local 55648.

## Necesidad y unidad de análisis

Una solicitud contiene una revisión explícita de reglas por sucursales ERP:
fechas puntuales, periodos inclusivos o recurrencias semanales con fin opcional.
La aprobación autoriza esa revisión; el resultado de publicación en Google es
un evento distinto. No se modifica el horario regular público en esta etapa.

| Concepto | Fuente existente | Identidad | Decisión |
| --- | --- | --- | --- |
| Sucursal operativa | `core.Sucursal`, `sucursales_operativas` | PK ERP y código | Reutilizar, sin altas ni cambios |
| Alias | `SucursalAlias`, `resolve_branch_token` | Alias activo normalizado y FK | Reutilizar, sin inferir equivalencias nuevas |
| Intención y aprobación | `SolicitudHorarioEspecial` | PK, canonical_payload, approved_at | Extender JSON con schema_version=2 y revision SHA256 |
| Evidencia de aprobación | `HorarioEspecialBitacora` | Request + acción APPROVED + revision | Reutilizar para vincular contenido aprobado |
| Publicación por fecha | `HorarioEspecialDetalle` e intentos | Request + sucursal + fecha | Conservar; materialización futura con horizonte explícito |
| Horario regular | `https://www.pollyanasdolce.com/api/branches/` | Código y nombre verificados | Mantener lectura existente sin dependencia inversa |

## Evidencia y límites

Se ejecutó `manage.py inventario_fuentes_datos --term horario --term sucursal`
contra PostgreSQL de esta tarea después de crear el esquema; es inventario de
metadatos, no prueba de cobertura de negocio. Las migraciones base se aplicaron
completas, sin fake. Las pruebas usan sucursales y usuarios sintéticos aislados.

La coordinación principal reportó consulta productiva limitada de solo lectura:
0 solicitudes de horario y 0 configuraciones Google activas; las sucursales se
identificaron por sus PK y códigos existentes. Esta subetapa no consultó ni
modificó datos productivos. CRUCERO y BAMOA no se fusionan ni se crean alias.
Sinaloa de Leyva requiere resolver su código público fuera de este cambio.

## Contrato y consumidores

`create_request_from_rules` recibe IDs explícitos y reglas, valida antes de
escribir y devuelve solicitud/payload. `approve_request` exige
`expected_revision` para v2. Correcciones crean otra solicitud; no se permite
revalidar ni reaprobar una revisión terminal. El resolver comprueba el hash y
la bitácora de aprobación antes de devolver VERIFIED.

Precedencia v2: fecha > periodo > semanal; empates por approved_at, PK solicitud
y posición de regla. Las solicitudes históricas conservan sus controles
conservadores: sus detalles por fecha dominan cualquier regla v2, incluso fecha puntual.
Una revisión aprobada inválida produce UNKNOWN conservador: si su contenido
fue alterado no se puede confiar en su vigencia para descartarla por fecha.
No se cambia el contrato Maya: identidad, zona horaria, checked_at y fuente
ERP_APPROVED_SPECIAL_HOURS; se agregan revisión, solicitud y procedencia.

Pendiente: transporte Telegram, identidad duradera, interpretación texto/audio,
publicador con horizonte y verificación Google/tienda/Maya. La ejecución v2
rechaza expresamente hasta disponer de ese publicador; no declara éxito vacío.
No se crearon modelos, migraciones, maestros duplicados ni datos operativos.
