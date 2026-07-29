# Plan de implementación: ingesta Hik-Connect confiable

Especificación: `docs/superpowers/specs/2026-07-28-hik-ingesta-confiable-design.md`

## Entorno de trabajo

- Worktree: `/Users/mauricioburgos/Downloads/codex_worktrees/hik-ingesta-confiable`
- Rama: `codex/rrhh-hik-ingesta-confiable`
- PostgreSQL: 16.14 aislado en `127.0.0.1:55433`
- `DATABASE_URL=postgresql://postgres@127.0.0.1:55433/pastelerias_erp`
- `APP_ENV=development`
- `ALLOW_INSECURE_LOCAL_SECRET_KEY=1`

Cada tarea sigue RED → GREEN → REFACTOR. No se escribe código productivo antes de
observar el fallo esperado de su prueba.

## Tarea 1: ledger y contrato API v2

### Objetivo

Persistir cada evento cloud antes de proyectarlo y responder un acuse individual
por GUID.

### Archivos previstos

- `rrhh/models.py`
- `rrhh/migrations/0039_eventohikcloud_estadointegracionhik.py`
- `rrhh/services_hik_ingesta.py`
- `rrhh/api_receptor.py`
- `rrhh/urls.py`
- `rrhh/tests_hik_ingesta_v2.py`

### Ciclos TDD

1. Mismo GUID/mismo payload devuelve `duplicate`.
2. Mismo GUID/payload diferente devuelve `payload_conflict`.
3. Identidad desconocida queda `deferred`, conserva payload y no crea asistencia.
4. Lote mixto devuelve un resultado correlacionado por evento.
5. Timestamp sin zona, fuente inválida y lote sobredimensionado se rechazan.
6. La restricción PostgreSQL impide carreras de creación duplicada.

### Verificación

```bash
python3 manage.py makemigrations --check
python3 manage.py migrate
python3 manage.py test rrhh.tests_hik_ingesta_v2 --keepdb
python3 manage.py migrate --check
python3 manage.py check
```

## Tarea 2: proyección transaccional y replay de identidades

### Objetivo

Reconstruir la asistencia desde el ledger sin perder eventos cercanos, serializar
empleado-día y reprocesar automáticamente recibos diferidos al vincular una
identidad.

### Archivos previstos

- `rrhh/services_hik_ingesta.py`
- `rrhh/services_identidad.py`
- `rrhh/services_hikvision.py`
- `rrhh/services_bonos_checador.py`
- `rrhh/tests_hik_ingesta_v2.py`

### Ciclos TDD

1. Dos eventos legítimos separados por menos de cinco minutos permanecen en el
   ledger y proyectan determinísticamente.
2. Dos requests concurrentes no pierden marcas.
3. Reenviar un GUID no modifica asistencia ni repite efectos.
4. Vincular una identidad programa replay `on_commit`.
5. El replay usa el payload persistido aunque el evento ya no esté en la nube.
6. Una captura de otra fuente conserva su procedencia.
7. Fallo de reglas/bonos no revierte ni desacredita el recibo.
8. Ningún camino toca datos manuales de bonos.

### Verificación

```bash
python3 manage.py test rrhh.tests_hik_ingesta_v2 rrhh.tests_asistencia_reglas --keepdb
python3 manage.py test bonos_produccion bonos_ventas --keepdb
```

## Tarea 3: outbox y descubrimiento sin pérdida

### Objetivo

Persistir todo GUID descubierto antes de cualquier cursor, entregar por acuse
individual y recuperar errores sin reclasificar el evento.

### Archivos previstos

- `ops/agente-hikconnect/state.py`
- `ops/agente-hikconnect/hikconnect_client.py`
- `ops/agente-hikconnect/erp_client.py`
- `ops/agente-hikconnect/main.py`
- `ops/agente-hikconnect/catchup.py`
- `ops/agente-hikconnect/test_outbox.py`
- `ops/agente-hikconnect/test_pagination.py`
- `ops/agente-hikconnect/test_marcado.py`

### Ciclos TDD

1. Un GUID se guarda en outbox antes de intentar POST.
2. Timeout deja el evento `pending`.
3. `accepted` y `duplicate` del mismo GUID cierran el evento.
4. Respuesta incompleta o GUID desconocido falla cerrada.
5. Evento con `deviceTime` antiguo pero subida reciente se conserva.
6. Agotamiento de páginas deja continuación pendiente y no declara éxito.
7. Reinicio del proceso conserva payload, intentos y error.
8. La clasificación no cambia entre reintentos.
9. Un lote solo de duplicados termina correctamente.

### Verificación

```bash
cd ops/agente-hikconnect
python3 test_pagination.py
python3 test_marcado.py
python3 test_outbox.py
```

## Tarea 4: systemd, recuperación silenciosa y salud

### Objetivo

Ejecutar ciclos observables y auto-recuperables, evitar solapamientos y escalar
solo incidentes que requieren intervención humana.

### Archivos previstos

- `ops/agente-hikconnect/main.py`
- `ops/agente-hikconnect/health.py`
- `ops/agente-hikconnect/systemd/agente-hikconnect.service`
- `ops/agente-hikconnect/systemd/agente-hikconnect.timer`
- `ops/agente-hikconnect/systemd/hik-catchup.service`
- `ops/agente-hikconnect/systemd/hik-health.service`
- `ops/agente-hikconnect/systemd/hik-health.timer`
- `ops/agente-hikconnect/test_health.py`
- `rrhh/models.py`
- migración de la tarea 1
- `rrhh/services_hik_alertas.py`
- `rrhh/tasks.py`
- `rrhh/tests_hik_ingesta_v2.py`

### Ciclos TDD

1. Códigos de salida distinguen éxito, parcial de identidad, Hik, ERP e interno.
2. Un lock impide sync y reconciliación concurrentes.
3. Estado sano se mantiene silencioso.
4. Una falla ordinaria conserva datos y continúa reintentando.
5. Atraso menor de diez minutos no genera incidente humano.
6. Incidente prolongado se deduplica.
7. Recuperación cierra el incidente.
8. Identidad pendiente no se etiqueta como falla de conexión.
9. WhatsApp, correo y dashboard se invocan solo para acción humana.
10. Las unidades pasan `systemd-analyze verify`.

### Verificación

```bash
cd ops/agente-hikconnect
python3 test_health.py
systemd-analyze verify systemd/*.service systemd/*.timer
```

En macOS, si `systemd-analyze` no existe, validar sintaxis en un contenedor Linux
aislado o en CI; no ejecutar ni instalar unidades locales.

## Tarea 5: contrato extremo a extremo y documentación operativa

### Objetivo

Demostrar el flujo nube simulada → outbox → API → ledger → asistencia y dejar un
despliegue/rollback reproducible.

### Archivos previstos

- `rrhh/tests_hik_ingesta_v2.py`
- `ops/agente-hikconnect/test_contract_v2.py`
- `ops/agente-hikconnect/README.md`
- `docs/operations/hik-connect-ingestion.md`

### Ciclos TDD

1. ERP caído y recuperación posterior.
2. Caída después del commit y antes del acuse.
3. Lote 42 aceptados, 3 diferidos y 2 duplicados.
4. Replay tras vincular identidad.
5. Reconciliación detecta una divergencia sin corregir historia automáticamente.
6. El ciclo normal cumple el contrato de diez minutos bajo reloj simulado.

### Verificación final local

```bash
python3 manage.py test rrhh.tests_hik_ingesta_v2 --keepdb
python3 manage.py test rrhh bonos_produccion bonos_ventas --keepdb
python3 manage.py migrate --check
python3 manage.py check
git diff --check
```

## Revisiones por tarea

Después de cada tarea:

1. revisión de cumplimiento contra esta especificación;
2. corrección de cualquier brecha;
3. revisión de calidad, concurrencia, seguridad y alcance;
4. corrección y nueva revisión;
5. commit quirúrgico con solo los archivos de la tarea.

## Cierre

1. Revisión adversarial completa.
2. PR borrador con resumen, archivos, pruebas y riesgos.
3. CI verde.
4. Merge a `main`.
5. Deploy ERP mediante `scripts/deploy_web_safe.sh`, sin `git pull` manual.
6. Deploy del agente únicamente desde los archivos versionados.
7. Migración y checks productivos.
8. Validación del endpoint v2 y varios ciclos.
9. Validación de la reconciliación sin reparación histórica.
10. Limpieza mediante `task_workspace_close.sh --state merged`.
