# Operación: rutina diaria ERP

Comando unificado:

```bash
DEBUG=1 .venv/bin/python manage.py ejecutar_rutina_diaria_erp \
  --month 2026-03 \
  --point-dir "/Users/mauricioburgos/Downloads/INFORMACION POINT" \
  --drive-create-aliases
```

## Qué ejecuta
1. `sync_almacen_drive`
2. `sync_point_catalogs`
3. `ejecutar_hardening_post_import`
4. `auditar_flujo_erp`

Genera resumen en `logs/rutina_diaria_erp_<timestamp>.md`.

## Modos útiles

### Simulación end-to-end (sin cambios)
```bash
DEBUG=1 .venv/bin/python manage.py ejecutar_rutina_diaria_erp \
  --month 2026-03 \
  --dry-run
```

### Continuar aunque falle una integración
```bash
DEBUG=1 .venv/bin/python manage.py ejecutar_rutina_diaria_erp \
  --month 2026-03 \
  --continue-on-error
```

### Solo hardening + auditoría (sin Drive/Point)
```bash
DEBUG=1 .venv/bin/python manage.py ejecutar_rutina_diaria_erp \
  --skip-drive --skip-point
```

## Ejecución en Railway (job manual)

```bash
railway run bash -lc 'cd /app && python manage.py ejecutar_rutina_diaria_erp --month 2026-03 --continue-on-error'
```

## Criterio operativo
- Si el resumen termina con `Rutina diaria completada`, flujo OK.
- Si termina con `Rutina diaria completada con errores`, revisar sección `## Errores` del markdown y ejecutar remediación por comando puntual.

## Snapshot DG de operación integrada

Comando manual:

```bash
.venv/bin/python manage.py generar_snapshot_dg_operacion \
  --format json \
  --output-dir storage/dg_reports
```

Exporte operativo en XLSX:

```bash
.venv/bin/python manage.py generar_snapshot_dg_operacion \
  --format xlsx \
  --output-dir storage/dg_reports \
  --dg-group-by week
```

Automatización local macOS (`launchd`):

```bash
chmod +x scripts/run_dg_operacion_snapshot.sh \
  scripts/install_dg_operacion_snapshot_launchd.sh \
  scripts/uninstall_dg_operacion_snapshot_launchd.sh

./scripts/install_dg_operacion_snapshot_launchd.sh
```

Configuración opcional por `.env`:

```bash
DG_OPERACION_GROUP_BY=day
DG_OPERACION_START_DATE=
DG_OPERACION_END_DATE=
DG_OPERACION_FECHA_OPERACION=
DG_OPERACION_OUTPUT_DIR=storage/dg_reports
DG_OPERACION_EXPORT_FORMATS=json,xlsx
DG_OPERACION_SNAPSHOT_HOUR=6
DG_OPERACION_SNAPSHOT_MINUTE=15
```

Artefactos generados:
- snapshots: `storage/dg_reports/`
- logs launchd: `storage/dg_reports/logs/`
