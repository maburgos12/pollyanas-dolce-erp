# Fuentes — reparto histórico autorizado enero–agosto 2026

## Necesidad
Integrar el informe conciliado en Estado de resultados, con cálculo mensual por sucursal y exportación, sin segunda captura ni mutaciones operativas. Mauricio autorizó participación en ventas y corrección Crucero/Bamoa, seguida de integración al ERP.

## Fuentes
| Concepto | Fuente oficial y unidad | Identificador / evidencia | Consumidores |
|---|---|---|---|
| Fondos y gastos directos | LineaPresupuestoMensual ORIGINAL, rubro activo; mismo filtro fuente_real del estado de resultados | Período/rubro/sucursal; ocho meses conciliados, utilidad enero–agosto 3,312,006.78 en consulta 01/11/2026 | Estado de resultados y exportación |
| Ventas | PointDailySale, importe bruto menos descuentos | Fecha/producto/PointBranch; conserva base de ingresos del P&L, incluidos impuestos de origen | Participación mensual, no cambia ventas |
| Identidad histórica | core.Sucursal, PointBranch y fecha de venta | Externo Point 2 (PointBranch 5) tiene nombre normalizado crucero y relación vigente ERP17; ventas enero–junio 1,117,532.88 pertenecen a Crucero ERP4 | Solo lectura de este reparto autorizado |
| Apertura | core.Sucursal.fecha_apertura | Bamoa 14/07/2026; última venta anterior del externo 2: 21/06; hueco hasta 13/07. Guamúchil primera venta 24/03 y referencia del proyecto, campo maestro vacío | Etapas visibles, no edición del catálogo |
| Redondeo | services_costeo_logistica.repartir_centavos | Mayores restos, preservación de fondos | Reutilizado sin modificar contratos |

## Identidades
Crucero y Bamoa son distintas; no se fusionan. La asignación temporal del externo Point 2 para este informe fue revisada y autorizada por Mauricio. No se extiende a otros años ni lectores. Ventas de Bamoa anteriores al 14/07 o de Crucero posteriores a junio bloquean la presentación, no se adivinan. Gastos previos conservan centro y período.

## Decisión
Extender la vista existente con servicio de solo lectura y parcial de tabla. Reutilizar los buckets calculados por Estado de resultados para evitar duplicar exclusiones de materia prima/rollups. Ninguna tabla, migración, job o API nuevos. La versión ORIGINAL 2026 muestra únicamente enero–agosto autorizados. Los otros años/versiones conservan su comportamiento.

## Verificación
Inventario léxico: `manage.py inventario_fuentes_datos --term sucursal --term presupuesto` en PostgreSQL16 aislado. Fuente productiva: consultas READ ONLY conservadas en `Documents/Codex/2026-11-01/reparto-enero-agosto/{fuentes,identidad,atipicos}.json`; cálculo corregido y CSV en subdirectorio `corregido`. Validar consumidor HTML y CSV tras desplegar, por cada mes, sucursal y fondo contra ese detalle, y conteos antes/después.

## Límites
Reparto gerencial autorizado, no consumo físico ni costo evitable por cierre. Los importes previos a abrir y el equipo de Guamúchil no se reclasifican. Conciliación no certifica cobertura de capturas. Ante diferencias, faltantes o fondo inválido se bloquea todo el acumulado y su CSV; no se publica un total parcial.
