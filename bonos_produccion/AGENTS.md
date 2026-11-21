# Bonos de producción

Leer también [AGENTS raíz](../AGENTS.md).

### Datos de usuarios — NUNCA pisar
`bono_extra`, `ajuste_positivo`, `ajuste_negativo` son datos de nómina real capturados
por las jefas (Carolina Cayetano en producción, Johana López en ventas). Un borrado
accidental es crítico.
- **NUNCA** resetearlos, sobreescribirlos ni incluirlos en seeds o inicializaciones
- **NUNCA** hacer update masivo a 0 sin confirmación explícita de Mauricio
- `recalcular()` y `recalcular_todos()` NO deben tocar esos campos — verificarlo
  antes de cualquier cambio en models.py de bonos. Si un endpoint llama
  `recalcular_todos()` automáticamente (ej. resumen), revisar que no pise datos
  manuales capturados por las jefas.
- Verificar antes y después de cualquier cambio:
  ```sql
  SELECT COUNT(*), SUM(bono_extra), SUM(ajuste_positivo)
  FROM bonos_produccion_bonoproduccionempleado bp
  JOIN bonos_produccion_configbonoperiodo cp ON bp.periodo_id=cp.id
  WHERE cp.mes=X AND cp.anio=Y AND (bono_extra>0 OR ajuste_positivo>0);
  ```
