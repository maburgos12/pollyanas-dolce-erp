# DG Daily Operating Cycle (Pollyana's Dolce)

## Objetivo
Asegurar operación diaria con control enterprise: inventario confiable, producción correcta, compras trazables, y reportes consistentes.

## Ciclo sugerido
1. **Cierre de sucursales (noche):** captura stock final por sucursal/producto.
2. **Sincronización nocturna:** ingesta validada (Google Drive/archivo) con bitácora de ejecución.
3. **Inicio CEDIS (mañana):** consolidado de reabasto + plan de producción + faltantes críticos.
4. **Operación diaria:** movimientos inventario/producción/compras con trazabilidad.
5. **Corte del día:** reconciliación sistema vs control interno y revisión de excepciones.

## Checkpoints obligatorios
- Stock negativos: 0 permitidos.
- Movimientos sin referencia: 0 permitidos.
- Solicitudes sin proveedor sugerido: revisar lista.
- Productos con vida de anaquel > 2 días: excepción explícita aprobada.
- Jobs fallidos: 0 pendientes al arranque del día.

## Escalación
- P0: descuadre inventario masivo, exportes no determinísticos, permisos críticos incorrectos.
- P1: alias sin homologar en insumos clave, reabasto sin consolidar por sucursal.
- P2: ajustes de UI/reportes no bloqueantes.
