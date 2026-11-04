# Ficha de fuentes — reporte financiero mensual

Fecha y ambiente: 2026-11-04; producción, PostgreSQL 16, consultas de lectura.

## Necesidad y unidad de análisis
Una fila por mes y sucursal: puente ventas → costos y producción → margen → gastos directos → administración/logística → resultado; escenario mensual de PE. Personal como detalle, no segundo gasto.

## Fuentes candidatas
| Concepto | Modelo / fuente | Identificador y ámbito | Evidencia | Consumidores |
|---|---|---|---|---|
| Cierre real autorizado | LineaPresupuestoMensual ORIGINAL / leer_cierre_conciliado | rubro, mes, versión; identidad histórica Point | 83 filas de reparto enero–septiembre, 0 errores; septiembre resultado 458916.70 | Estado de resultados, dashboard, mensual |
| Gastos y personal directo | registros_del_mes / leer_costos_mensuales | sucursal y periodo | adaptador compartido excluye personal central | mensual y detalle |
| Horas extra operativas | HoraExtra | empleado, fecha, estado | AUTORIZADO/PAGADO; sucursal actual, no prueba de pago ni identidad histórica | detalle informativo |
| Soportes 2025 | NominaImportacion | SHA256; periodo nulo en carga documental | 33 soportes, sin contabilización; diferencia anual 258400.17 | conciliación, no fuente del resultado mensual |

## Alias y equivalencias
| Términos | Estado | Evidencia | Revisión |
|---|---|---|---|
| utilidad_neta JSON legado / resultado operativo | Alias de compatibilidad, no equivalencia contable | lectores existentes conservan clave y alcance; UI usa resultado operativo | no cambia criterio |
| Horas extra autorizadas / costo pagado | Distintas | autorización no acredita liquidación ni relación con recibo | no sumarlas al resultado |
| Total percepciones / costo de personal | Distintas | préstamos incluidos; despensa y fondo de ahorro fuera de ese total documental | cotejo por concepto |

## Decisión
Reutilizar resumen_cierre_mensual para importes y PE. Proyección en memoria, sin modelos, migraciones ni escrituras. Campos financieros aditivos en JSON/CSV; ausencias se exportan nulas/vacías, no como cero. Mantener nombres de columnas legados para consumidores existentes; nuevos campos fin_* explícitos. Totales financieros sólo sobre meses de cierre disponibles. YoY exige misma base y perímetro. No cambiar política de pagos reales ni incorporar provisiones.

Procedimiento: inventario_fuentes_datos --term rentabilidad --term nomina (13 candidatos); leer_cierre_conciliado y resumen_cierre_mensual(2026-09). Pruebas de puente financiero, no escrituras, ausencias, perímetro histórico, exportaciones y permisos.

Límites: costo y producción son fondos gerenciales existentes, no un costeo nuevo de inventarios; PE es escenario de mezcla mensual, no ahorro de cierre. No importar el acumulado anual como una quincena ni homologar empleados sólo por nombre. Fuente fiscal distinta entre cierres y periodos parciales impide comparar esos alcances.
