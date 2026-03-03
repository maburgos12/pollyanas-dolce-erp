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
