# Ingesta Hik-Connect confiable e idempotente

Fecha: 2026-07-28

Estado: aprobado para implementación

Responsable: Codex

Rama: `codex/rrhh-hik-ingesta-confiable`

## Objetivo

Mantener las checadas de Hik-Connect disponibles en el ERP en menos de diez
minutos, sin pérdidas silenciosas, duplicidad, errores parciales ocultos ni
dependencia de intervención manual para reintentos, renovación de sesión,
recuperación de conectividad o reconciliación ordinaria.

El sistema no puede impedir una caída externa de internet o Hik-Connect. Sí debe
absorberla: conservar lo descubierto, reintentar automáticamente, recuperar el
atraso al volver la conexión y escalar únicamente cuando exista una decisión que
no pueda automatizarse con seguridad.

## Alcance

- Agente versionado en `ops/agente-hikconnect/`.
- Receptor de asistencia Hik en `rrhh/`.
- Ledger PostgreSQL por evento cloud.
- API v2 con acuse individual por GUID.
- Outbox SQLite durable en el agente.
- Polling y reconciliación sin depender de `deviceTime` como cursor.
- Proyección transaccional de asistencia.
- Reprocesamiento automático después de vincular una identidad.
- Ejecuciones systemd observables, con exclusión mutua y recuperación.
- Estado de salud y recuperación silenciosa.
- Pruebas de contrato, concurrencia, caídas e idempotencia.
- Runbook y despliegue reproducible por Git.

## Fuera de alcance

- Corregir automáticamente asistencia histórica.
- Recalcular nómina, sanciones, incidencias o bonos históricos.
- Modificar `bono_extra`, `ajuste_positivo` o `ajuste_negativo`.
- Inferir o cambiar horarios declarados.
- Vincular empleados mediante coincidencia difusa de nombres.
- Reemplazar Hik-Connect, Maya, correo o PostgreSQL.
- Introducir Kafka, RabbitMQ, Prometheus u otra plataforma.

La reconciliación histórica podrá detectar divergencias y producir un reporte,
pero cualquier escritura correctiva que afecte RRHH o bonos requerirá una
autorización separada.

## Problemas confirmados

1. La nube ordena por momento de subida, mientras el agente filtra por
   `deviceTime`. Los marcajes subidos tarde pueden quedar detrás del cursor.
2. El corte después de páginas “secas” sigue siendo una heurística.
3. `sync_once()` adelanta `last_sync` aun con errores parciales o transporte
   fallido.
4. La clasificación entrada/salida modifica `employee_day_state` antes de que el
   ERP confirme el evento.
5. El ERP recibe `cloud_record_guid` pero no lo persiste.
6. La deduplicación por cercanía de cinco minutos puede colapsar dos checadas
   legítimas y no resuelve exactamente el crash entre commit y acuse.
7. El acuse agregado por lote no permite confirmar cada evento.
8. Un lote de solo duplicados puede reintentarse indefinidamente.
9. Los duplicados vuelven a ejecutar evaluación de reglas y sincronización de
   bonos.
10. Una identidad resuelta no tiene una bandeja de eventos rechazados que pueda
    reprocesar.
11. El daemon puede continuar `active/running` mientras sus ciclos fallan.
12. El catch-up puede aparecer como caída total aunque Hik y ERP hayan funcionado
    parcialmente.

## Invariantes

1. Todo GUID descubierto se persiste antes de avanzar cualquier frontera.
2. Un GUID solo termina cuando el ERP confirma individualmente `accepted` o
   `duplicate` para el mismo contenido.
3. Timeout, error HTTP, respuesta incompleta o agotamiento de páginas nunca
   elimina ni da por entregado un evento.
4. El payload original permanece inmutable y auditable.
5. El mismo GUID con payload diferente es un conflicto, no una actualización.
6. La recepción durable no depende del éxito de reglas, horas extra o bonos.
7. Una identidad desconocida queda pendiente sin crear asistencia ni afectar
   bonos.
8. Al vincular la identidad, los eventos pendientes se reprocesan
   automáticamente.
9. Cada empleado-día se proyecta bajo transacción y bloqueo.
10. Cada versión de la proyección dispara efectos posteriores como máximo una
    vez.
11. Los marcajes de Hik, Point, Excel y captura manual conservan procedencia.
12. Ningún proceso normal y reconciliador modifica simultáneamente el mismo
    estado local.

## Arquitectura

```text
Hik-Connect Cloud
        |
        v
descubridor cada 5 minutos
        |
        v
outbox SQLite por GUID
        |
        v
POST /rrhh/api/asistencia-hik/v2/
        |
        v
ledger PostgreSQL por fuente + GUID
        |
        +--> identidad pendiente --> replay al vincular
        |
        v
proyección transaccional empleado-día
        |
        v
efectos auditables posteriores al commit
```

El outbox protege frente a indisponibilidad del ERP. PostgreSQL es la fuente
canónica de recepción y procesamiento. SQLite no sustituye la base operativa del
ERP.

## Ledger PostgreSQL

Se agregará un modelo aditivo en `rrhh` para conservar:

- fuente;
- `event_id`/`cloud_record_guid`;
- hash canónico del payload;
- payload mínimo recibido;
- código externo;
- fecha/hora ocurrida con zona;
- dispositivo y metadatos no sensibles;
- empleado resuelto, nullable;
- estado de recepción;
- estado de proyección;
- código y detalle sanitizado del último error;
- intentos y marcas de tiempo;
- versión de proyección aplicada.

La restricción única será `(fuente, event_id)`.

Resultados:

- mismo GUID y mismo hash: `duplicate`;
- mismo GUID y hash distinto: `payload_conflict`;
- identidad desconocida: `deferred/identity_unresolved`;
- evento válido y durable: `accepted`;
- payload inválido no reintentable: `rejected`;
- fallo interno recuperable: estado durable pendiente/fallido.

## API v2

La v1 se conservará durante la transición. La v2 recibirá:

```json
{
  "contract_version": 2,
  "batch_id": "uuid",
  "events": [
    {
      "event_id": "cloud_record_guid",
      "source": "hikconnect_cloud",
      "employee_external_id": "328",
      "occurred_at": "2026-07-28T08:00:00-07:00",
      "kind": "check_in",
      "device_id": "opcional"
    }
  ]
}
```

Responderá por evento:

```json
{
  "contract_version": 2,
  "batch_id": "uuid",
  "results": [
    {
      "event_id": "cloud_record_guid",
      "outcome": "accepted",
      "reason_code": "ok",
      "retryable": false,
      "receipt_id": "id-ledger",
      "projection": "applied"
    }
  ]
}
```

Los contadores agregados podrán mantenerse como conveniencia, pero el agente no
los usará como acuse.

La API validará:

- versión de contrato;
- `batch_id`;
- GUID no vacío;
- fuente permitida;
- timestamp ISO-8601 con zona;
- límites temporales razonables;
- código externo;
- tipo de evento permitido;
- tamaño máximo del lote;
- conflictos de payload.

## Proyección de asistencia

- Se preservarán todos los eventos crudos, incluso checadas legítimas cercanas.
- La fila `AsistenciaEmpleado` seguirá siendo una proyección diaria.
- La proyección se calculará desde eventos ordenados y recibos ya persistidos.
- Se usará `transaction.atomic()` y bloqueo para serializar un empleado-día.
- La deduplicación temporal podrá señalar anomalías, pero no eliminará evidencia
  del ledger.
- Una captura manual o de otra fuente no perderá su procedencia.
- Los efectos posteriores solo se programarán después del commit y únicamente
  cuando cambie la versión efectiva de la proyección.
- La ingesta no ejecutará recalculo histórico masivo.

## Identidades pendientes

Cuando el código externo no tenga empleado:

1. El recibo queda `deferred`.
2. Se actualiza la identidad pendiente existente.
3. No se crea ni modifica asistencia.
4. No se ejecutan reglas, horas extra ni bonos.
5. Al vincular la identidad, una tarea `on_commit` reprocesa todos sus recibos
   pendientes.
6. El replay usa el payload durable; no depende de que Hik aún conserve el
   registro.

No se permitirá vinculación difusa automática.

## Outbox y descubrimiento

SQLite conservará por GUID:

- payload canónico;
- estado `pending`, `delivered` o `error`;
- intentos;
- último intento;
- categoría y mensaje sanitizado del último error;
- acuse individual recibido.

El agente:

1. Obtiene páginas recientes por orden cloud.
2. Persiste todo GUID nuevo, sin descartar por antigüedad de `deviceTime`.
3. Continúa hasta una frontera robusta de GUID ya conocidos y aplica un límite
   explícito observable.
4. Si agota el límite, no declara el ciclo completo y programa continuación.
5. Entrega pendientes en lotes acotados.
6. Elimina de la cola activa solo GUID confirmados individualmente.

El estado entrada/salida local no se modificará antes del acuse. La clasificación
será determinista desde los eventos del día o se resolverá en la proyección ERP.

## Recuperación silenciosa

- Timer normal cada cinco minutos.
- Reintentos dentro de la ventana a los 30 segundos, 1, 2 y 5 minutos.
- Renovación automática de sesión Hik.
- Recreación automática de Chromium ante bloqueo.
- Persistencia del outbox durante fallos ERP o red.
- Timeout por ejecución.
- Lock compartido entre sincronización y reconciliación.
- Reinicio limpio por systemd ante proceso atascado.
- Reconciliación profunda periódica.
- Recuperación automática del atraso al volver la conectividad.

El SLO operativo es:

- información normal en el ERP en menos de diez minutos;
- un ciclo sin éxito durante diez minutos se considera atraso crítico interno;
- el sistema continúa intentando y recuperando sin requerir Codex.

## Alertas

Los fallos y recuperaciones ordinarias se registran, pero no generan ruido.
WhatsApp, correo y dashboard solo se usan después de agotar la recuperación
automática y cuando se requiere una acción humana segura, por ejemplo:

- credenciales revocadas;
- identidad nueva no vinculable automáticamente;
- mismo GUID con payload conflictivo;
- indisponibilidad externa prolongada.

Los incidentes se deduplican. La recuperación cierra el incidente sin crear una
cascada de avisos repetidos.

Se reutilizarán:

- Maya para WhatsApp;
- correo Django;
- `core.Notificacion` para dashboard.

## Systemd

Se preferirán ejecuciones `oneshot` mediante timer sobre un daemon que absorbe
excepciones indefinidamente.

- Timer cada cinco minutos.
- Timer de reconciliación profunda.
- Lock nativo compartido.
- Timeout explícito.
- Códigos de salida por categoría.
- Una línea de resumen estructurado por ejecución.
- Journald como salida principal; no se duplicará indefinidamente en un archivo
  sin rotación.
- Las unidades y el código se desplegarán desde Git.

## Pruebas

### Agente

- registro recién subido con `deviceTime` antiguo;
- páginas secas seguidas de un registro válido;
- agotamiento de páginas sin avanzar frontera;
- timeout antes y después del commit;
- reinicio con outbox pendiente;
- lote compuesto solo por duplicados;
- lote mixto 42 aceptados, 3 diferidos y 2 duplicados;
- conflicto de payload;
- lock concurrente;
- renovación de sesión;
- clasificación estable después de un rechazo.

### ERP/PostgreSQL

- unicidad fuente + GUID;
- mismo GUID/mismo hash;
- mismo GUID/hash diferente;
- dos eventos legítimos separados por menos de cinco minutos;
- acuse individual;
- identidad pendiente y replay posterior;
- dos requests concurrentes para el mismo empleado-día;
- conservación de procedencia;
- fallo posterior de reglas, horas extra o bonos;
- ningún efecto repetido por reintento;
- validación temporal y de tamaño de lote.

### Contrato extremo a extremo

- nube simulada -> outbox -> API -> ledger -> asistencia;
- ERP caído y recuperación;
- caída después del commit y antes del acuse;
- proceso terminado por timeout;
- reconciliación de divergencia;
- información visible antes de diez minutos.

## Despliegue

1. Implementar y validar en worktree con PostgreSQL 16 aislado.
2. Ejecutar `manage.py check`, `migrate --check`, pruebas de agente, RRHH,
   concurrencia y contrato.
3. Revisar migración y diff completo.
4. Crear PR único y esperar CI.
5. Mergear a `main`.
6. Ejecutar `scripts/deploy_web_safe.sh` sin `git pull` manual previo.
7. Verificar migración, checks, contenedores y endpoint v2.
8. Cambiar el agente al contrato v2 desde Git.
9. Validar varios ciclos y la ausencia de pendientes inesperados.
10. Validar la siguiente reconciliación.
11. Mantener v1 para rollback durante la transición.
12. Cerrar y limpiar rama/worktree conforme al protocolo.

## Rollback

- Restaurar unidades y agente anteriores desde Git.
- Mantener el ledger y la migración aditiva; no borrar recibos.
- Volver temporalmente a v1 si v2 falla.
- No restaurar una copia antigua de `state.db`.
- No borrar ni recalcular asistencia durante rollback.

## Criterios de aceptación

- 100% de GUID descubiertos tienen estado durable.
- 0 GUID se marca entregado por un contador agregado.
- 0 efectos duplicados por reintentos.
- 0 eventos legítimos se eliminan por cercanía temporal.
- Recuperación automática de fallos ordinarios.
- Información normal disponible antes de diez minutos.
- Identidades pendientes se reprocesan al vincularse.
- Nube, outbox y ledger reconciliables.
- No se alteran datos manuales de nómina o bonos.
- Producción queda desplegada y validada en el flujo real.
