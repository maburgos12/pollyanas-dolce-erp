# Bloque 1 - Dato Maestro Unico

Este bloque estandariza homologacion y control de catalogos maestros para evitar desalineacion entre Point, almacen y ERP.

## Objetivo

1. Homologar catalogos Point -> ERP (proveedores, insumos, productos).
2. Consolidar diccionario maestro de nombres y pendientes de unificacion.
3. Generar reportes accionables de inconsistencias.

## Comando unico

```bash
.venv/bin/python manage.py ejecutar_bloque1_dato_maestro \
  --point-dir "/Users/mauricioburgos/Downloads/INFORMACION POINT" \
  --output-dir logs \
  --fuzzy-threshold 90 \
  --apply-point \
  --apply-point-name-aliases
```

## Modo seguro (sin aplicar Point)

```bash
.venv/bin/python manage.py ejecutar_bloque1_dato_maestro \
  --skip-point \
  --dry-run \
  --output-dir logs
```

## Salidas

- `logs/bloque1_dato_maestro_<timestamp>/resumen_bloque1.md`
- `logs/bloque1_dato_maestro_<timestamp>/point_sync/*.csv`
- `logs/bloque1_dato_maestro_<timestamp>/diccionario/*.csv`

## Integracion Google Drive

El flujo de Drive para inventario sigue vigente y complementa este bloque:

- Playbook: `.agent/skills/30-integrations/skill-google-sheets-bridge/playbooks/google_drive_inventory_sync_playbook.md`
- El resultado de sync Drive alimenta pendientes/aliases que luego se consolidan en este bloque.

## Nota operativa

Ejecutar este bloque antes de ajustes masivos de recetas, costos y reabasto para asegurar que todos usan el mismo dato maestro.
