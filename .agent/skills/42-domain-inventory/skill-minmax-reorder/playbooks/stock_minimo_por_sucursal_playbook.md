# Playbook - Stock Mínimo por Sucursal

## Objetivo
Calcular solicitud sugerida diaria para CEDIS con base en:
- Stock mínimo oficial por sucursal/producto.
- Stock final reportado al cierre.

## Fórmula base
`solicitud_sugerida = max(stock_minimo - stock_final, 0)`

## Reglas
- Aplica a producto terminado para reabasto de sucursales.
- No mezclar materia prima/almacén central en este flujo.
- Si no existe stock mínimo para un producto/sucursal, marcar pendiente de configuración.

## Flujo operativo
1. Sucursal captura stock final.
2. Sistema calcula sugerido por renglón.
3. Sucursal confirma o ajusta con justificación.
4. CEDIS consolida total por producto y planea producción/distribución.

## Controles
- Bloquear envío sin sucursal y fecha.
- Registrar usuario y timestamp de captura.
- Registrar diferencias entre sugerido y solicitado final.
