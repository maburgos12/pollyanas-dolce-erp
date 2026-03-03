# Playbook - Inventory Ledger Reconciliation

## Alcance
Reconciliar inventario entre:
- Ledger ERP (movimientos)
- Conteo físico/sucursal
- Archivo interno de control (Excel/Drive)

## Entradas
- Snapshot de existencias por insumo/sucursal.
- Movimientos del periodo (entradas/salidas/ajustes/merma/producción).
- Reporte interno de control del mismo periodo.

## Procedimiento
1. Validar periodo y zona horaria de los tres orígenes.
2. Agrupar por `sucursal + insumo`.
3. Calcular: `saldo inicial + entradas - salidas +/- ajustes = saldo sistema`.
4. Comparar `saldo sistema` vs `saldo control` y marcar variación.
5. Clasificar variaciones:
   - Error de captura
   - Alias no homologado
   - Movimiento faltante
   - Ajuste pendiente de autorización
6. Generar bitácora de conciliación con responsable y fecha compromiso.

## Criterio de salida
- Variación total por sucursal dentro de tolerancia definida.
- Ninguna variación crítica sin plan de corrección.
- Evidencia archivada para auditoría.
