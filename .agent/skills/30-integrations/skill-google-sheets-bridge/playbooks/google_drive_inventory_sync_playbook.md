# Playbook - Google Drive Inventory Sync

## Estado
Este proyecto ya contempla conexión de inventarios vía Google Drive. Este playbook formaliza la operación.

## Objetivo
Consumir archivos de una carpeta Drive mensual y ejecutar sincronización idempotente al ERP.

## Fuentes
- Carpeta raíz de Drive (por mes).
- Archivos Excel/Sheets con estructura esperada de inventario/sucursal.

## Flujo
1. Descubrir archivos de la carpeta objetivo (mes actual o seleccionado).
2. Validar esquema de columnas y hoja objetivo.
3. Normalizar nombres (alias -> catálogo oficial).
4. Importar con idempotencia (evitar duplicados por llave natural).
5. Generar resumen:
   - registros creados/actualizados
   - pendientes de homologación
   - errores por fila
6. Registrar corrida en bitácora técnica.

## Guardrails
- No sobreescribir manualmente datos confirmados sin marca explícita de reemplazo.
- Toda corrida debe tener `run_id` y timestamp.
- Si cambia esquema de archivo, la corrida debe fallar con mensaje claro.

## Recuperación
- Mantener snapshot anterior para rollback lógico.
- Permitir rerun del mismo archivo sin duplicar movimientos.
