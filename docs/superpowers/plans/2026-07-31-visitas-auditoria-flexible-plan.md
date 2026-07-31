# Plan de implementación: auditorías flexibles a sucursal

Fecha: 2026-07-31
Diseño aprobado: `docs/superpowers/specs/2026-07-31-visitas-auditoria-flexible-design.md`
Rama: `codex/visitas-auditoria-flexible`

## Alcance estricto

Archivos permitidos:

- `visitas_sucursal/**`
- `docs/superpowers/specs/2026-07-31-visitas-auditoria-flexible-design.md`
- `docs/superpowers/plans/2026-07-31-visitas-auditoria-flexible-plan.md`
- `docs/ux/action-context-coverage.md`

No se modifican otros módulos, datos históricos, permisos existentes, geocercas, variables de entorno, scripts de despliegue ni producción durante la implementación local. Las integraciones con `core.access`, `core.audit`, `logistica.PuntoLogistico` y el toast global son consumos de contratos existentes, no cambios a sus dueños.

## Contratos compartidos afectados

- `VisitaSucursal.fecha_programada` pasa a aceptar `NULL` solo para extraordinarias.
- Se agrega una migración nueva; no se edita ninguna migración aplicada.
- El cronograma separa proyección planeada y proyección real sin duplicar expedientes.
- La app conserva el permiso `MANAGE` como requisito de ejecución.
- La geocerca canónica sigue perteneciendo a `PuntoLogistico`.

## Estrategia TDD

Cada bloque sigue RED → GREEN → REFACTOR. No se escribe código productivo antes de ejecutar y observar la falla correspondiente.

### 1. Baseline aislado

1. Levantar PostgreSQL 16 con `COMPOSE_PROJECT_NAME` y puerto exclusivos.
2. Ejecutar todas las migraciones de `origin/main`.
3. Confirmar `migrate --check` y `check`.
4. Ejecutar la suite actual de `visitas_sucursal` y guardar su conteo de pruebas.
5. Detener si el baseline falla por una causa propia del módulo.

### 2. Modelo y migración

Pruebas primero en `visitas_sucursal/tests_flexible.py`:

- una visita planeada requiere fecha programada;
- una extraordinaria permite fecha programada nula;
- una extraordinaria requiere causa y detalle;
- la desviación es negativa, cero o positiva según fechas;
- `__str__` tolera extraordinarias sin fecha;
- una visita realizada expone el radio de geocerca usado.

Implementación mínima:

- agregar `BORRADOR`;
- agregar causas `SEGUIMIENTO_PENDIENTES`, `QUEJA` y `OTRO`;
- volver nullable `fecha_programada`;
- agregar `motivo_extraordinaria`, `detalle_extraordinaria` y `gps_radio_geocerca_m`;
- agregar propiedades derivadas sin efectos laterales;
- crear una migración nueva mediante `makemigrations visitas_sucursal`;
- inspeccionar manualmente el archivo generado.

No se fabrica GPS ni se reescriben visitas históricas.

### 3. Servicio de dominio

Crear `visitas_sucursal/services.py` solo después de pruebas fallidas para:

- listar programaciones pendientes de una sucursal en orden vencidas/hoy/futuras;
- crear un borrador extraordinario idempotente;
- rechazar causa o detalle incompletos;
- ejecutar una visita antes, el mismo día o después sin modificar `fecha_programada`;
- registrar `fecha_real`, `realizada_en` y `realizada_por` desde servidor;
- validar GPS presente, precisión `0 < precisión <= 100`, geocerca existente y distancia dentro del radio;
- calcular distancia en servidor e ignorar distancias del cliente;
- persistir `gps_radio_geocerca_m`;
- impedir ejecutar visitas `REALIZADA` o `CANCELADA`;
- impedir que la sucursal seleccionada difiera de la visita;
- proteger doble envío con transacción y `select_for_update()`;
- registrar un evento con el contrato existente de auditoría.

El servicio valida GPS antes de marcar la visita como realizada o consolidar evidencia. Un error deja la visita pendiente o el extraordinario en borrador.

### 4. App del auditor

Pruebas de vista primero:

- la tarjeta y ruta requieren `MANAGE` como hoy;
- seleccionar sucursal muestra solo sus programaciones pendientes;
- una programación futura, de hoy o vencida puede elegirse;
- una visita realizada o cancelada no ofrece cierre;
- `iniciar_extraordinaria` crea un solo borrador con causa y detalle;
- la ejecución registra al usuario real aunque exista otro auditor planeado;
- errores GPS conservan contexto y no cambian el estado;
- respuestas HTML y progresivas usan la misma lógica de negocio.

Implementación:

- convertir la cabecera de la app en selector progresivo de sucursal;
- agrupar pendientes con etiquetas claras `Vencida`, `Hoy` y `Próxima`;
- ofrecer `Auditoría extraordinaria` inline, sin modal;
- exigir causa y explicación antes de crear el borrador;
- mostrar fecha programada como referencia, nunca como bloqueo;
- mostrar precisión, distancia y estado de geocerca;
- usar el contrato existente `data-async-action` y toast accesible cuando sea compatible con la app standalone;
- conservar fallback POST y fragmento estable.

No se restaura captura para empleados de sucursal ni se amplían sus permisos.

### 5. Cronograma mensual

Pruebas primero:

- una visita puntual aparece una sola vez;
- una visita tardía conserva marcador en fecha planeada y proyecta ejecución en fecha real;
- una anticipada muestra desviación negativa;
- ambos marcadores enlazan el mismo expediente;
- el plan del mes se calcula por `fecha_programada` y excluye extraordinarias;
- la actividad del mes se calcula por `fecha_real`;
- una ejecución entre meses cumple el mes planeado y aparece como actividad del mes real;
- extraordinarias se muestran como `E` y no alteran el denominador;
- estados no dependen solo del color.

Implementación:

- separar consultas de plan y actividad;
- construir proyecciones por celda deduplicadas por id;
- conservar el cronograma existente y sus variantes móvil/escritorio;
- agregar textos accesibles `P`, `✓`, `✓ +N`, `✓ −N` y `E`;
- corregir indicadores para cumplimiento, puntualidad y extraordinarias;
- conservar acciones de programación existentes sin mover fechas durante la ejecución.

### 6. Contexto de acciones y accesibilidad

- Registrar la cobertura en `docs/ux/action-context-coverage.md`.
- Mantener el foco y la posición después de errores.
- Bloquear únicamente el botón presionado.
- Mostrar estados de GPS con texto, no solo color.
- Verificar contraste WCAG AA de mensajes y controles nuevos.
- Incluir estados `hover`, `focus`, `active`, `disabled`, `loading` y `error` de los controles añadidos.
- Respetar `prefers-reduced-motion`; no agregar movimiento decorativo.

### 7. Verificación automática

Con PostgreSQL aislado y una base limpia:

1. `python3 manage.py makemigrations --check`
2. `python3 manage.py migrate --check`
3. `python3 manage.py check`
4. `python3 manage.py test visitas_sucursal --noinput`
5. pruebas focalizadas de concurrencia sin `--keepdb` si existe contaminación;
6. `git diff --check`;
7. revisión de migración y diff limitado al alcance declarado.

### 8. Validación visible

- Levantar el servidor local contra PostgreSQL aislado.
- Autenticar un usuario con `MANAGE`.
- Revisar cronograma de escritorio y móvil.
- Ejecutar caso puntual, anticipado, atrasado y extraordinario.
- Simular GPS válido, impreciso, ausente y fuera de geocerca.
- Revisar consola y solicitudes de red.
- Confirmar que no existe Service Worker propio de `visitas_sucursal`; si aparece un consumidor cacheado, aplicar el bump coordinado exigido por el repositorio.

La validación local no se presentará como evidencia de producción.

## Criterio de entrega

- Diff exclusivo del alcance declarado.
- Ningún dato productivo modificado.
- Ninguna migración histórica editada.
- Pruebas RED observadas antes de cada bloque productivo.
- Suite final, checks y migraciones en verde con salida reciente.
- UI validada en navegador real local.
- Commit quirúrgico, sin artefactos temporales.
- PR, merge y despliegue solo después de revisar el diff y cumplir el protocolo productivo.
