# ERP Doctor

`scripts/erp_doctor.py` es una auditoria ejecutable para el ERP de Pollyana's Dolce.
No modifica base de datos, no toca `.env`, no hace deploy y no asume React ni `package.json`.

## Uso rapido

```bash
./.venv/bin/python scripts/erp_doctor.py --quick
```

`--quick` corre los checks pensados para pre-commit o pre-deploy local:

- sanidad de entorno Django (`APP_ENV` + `DEBUG`)
- `python manage.py check`
- `python manage.py migrate --check`
- guards de calidad existentes en `scripts/check_pointdailysale_usage.py` y `scripts/check_protected_sales_readers.py`
- inventario basico de templates
- sintaxis JS/PWA propia si `node` esta disponible
- carga del registro Celery y tareas criticas
- `docker compose config -q` si Docker esta disponible
- placeholder explicito para validacion real de navegador

## Auditoria completa

```bash
./.venv/bin/python scripts/erp_doctor.py --full
```

`--full` agrega herramientas opcionales y smoke tests criticos:

- `ruff check .`
- `pip-audit -r requirements.txt`
- `semgrep` con reglas Python/Django/security
- `djlint` en modo `--check`, sin reformatear templates
- `scripts/run_tests_local.sh` con tests priorizados

Si una herramienta opcional no esta instalada, el doctor reporta `SKIPPED` con una instruccion de instalacion.
No se agregan estas herramientas a `requirements.txt` en esta primera fase porque son dependencias de desarrollo/auditoria, no runtime del ERP.

## Salida JSON

```bash
./.venv/bin/python scripts/erp_doctor.py --quick --json
```

El JSON tiene esta forma:

```json
{
  "ok": false,
  "status": "WARN",
  "checks": [
    {
      "name": "Django check",
      "severity": "OK",
      "status": "OK",
      "command": "./.venv/bin/python manage.py check",
      "exit_code": 0,
      "duration_ms": 1200,
      "summary": "OK",
      "details": []
    }
  ]
}
```

## Produccion solo lectura

```bash
./.venv/bin/python scripts/erp_doctor.py --production-readonly
```

Este modo usa la ruta operativa documentada para el VPS:

- host: `root@68.183.165.47`
- llave: `~/.ssh/agente_dg_ops`
- directorio: `/opt/pastelerias-erp`
- compose: `docker compose -f /opt/pastelerias-erp/docker-compose.yml`

Los comandos remotos son diagnosticos de solo lectura:

- `docker compose ps`
- `python manage.py check`
- `python manage.py migrate --check`
- carga del registro Celery

No ejecuta `migrate`, no reinicia servicios, no hace `git pull`, no modifica `.env` y no corre comandos de escritura como `setup_celery_schedules`.

## Interpretacion

Estados posibles:

- `OK`: check exitoso.
- `WARN`: hallazgo que requiere revision pero no necesariamente bloquea el trabajo local.
- `FAIL`: fallo bloqueante o comando obligatorio fallido.
- `SKIPPED`: herramienta o contexto no disponible; el reporte explica como habilitarlo.

`migrate --check` puede fallar si hay migraciones pendientes ajenas al cambio actual. En ese caso el doctor lo reporta como evidencia y no intenta corregirlo.

Si `.env` no define `APP_ENV`, `config/settings.py` usa `production` por default. Con `DEBUG=True` eso bloquea Django antes de cargar apps; para uso local debe definirse `APP_ENV=local` o `APP_ENV=development`, o apagar `DEBUG` si realmente es staging/production.

## Navegador

El doctor no reemplaza la validacion real de flujos UI indicada en `AGENTS.md`.
Cuando el cambio afecte templates, formularios, navegacion, autenticacion o integraciones visibles, validar con Chrome DevTools MCP o con el wrapper local:

```bash
UI_CHECK_USERNAME=usuario UI_CHECK_PASSWORD=password \
./scripts/ui_check_safe.sh --route "/ruta" --expect-text "Texto esperado"
```
