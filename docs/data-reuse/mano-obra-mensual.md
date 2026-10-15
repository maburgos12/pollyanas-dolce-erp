# Ficha de fuentes — Mano de obra mensual

Fecha y ambiente: 14/10/2026; diagnóstico de producción de septiembre y pruebas PostgreSQL 16 aisladas.

## Necesidad y unidad de análisis
Distribuir las percepciones de Producción entre productos finales elaborados en el mes, por cantidad multiplicada por costo mensual de materia prima. Una fila por receta y mes; tres componentes: elaboración/apoyo, supervisión/control y coordinación de despacho. No es estudio de tiempos ni costo de ventas contable.

## Fuentes candidatas
| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Percepciones | rrhh.NominaLinea / NominaPeriodo | Nómina RRHH | empleado, periodo y departamento_snapshot | Septiembre: periodos 20 y 21 pagados; 48 líneas, 26 personas, $247,511.92 | RRHH y reportes |
| Puestos | rrhh.Empleado | Capital Humano | empleado_id; puesto y puesto_operativo actuales | Tres responsables tienen puesto textual aunque puesto operativo esté vacío | RRHH, clasificación mensual |
| Elaboración | pos_bridge.PointProductionLine | Sincronización Point | branch, production_external_id, detail_external_id | 1,571 líneas; 786 finales, 11,778 unidades, 50 recetas | Inventarios y reportes |
| Identidad y rendimiento | recetas.Receta / maestros.UnidadMedida | Catálogo ERP | receta_id, codigo_point | 50 finales vinculados; recetas por pieza confirmadas por Dirección el 14/10/2026 | Costeo y producción |
| Costo mensual | reportes.RecetaCostoHistoricoMensual | Costeo histórico | receta_id y periodo | 50 costos positivos con cobertura 100%; estructura actual | Reportes financieros |
| Precio observado | pos_bridge.PointDailySale | Sincronización Point | receta_id y fecha | Solo ventas vinculadas; ausencia se muestra sin precio | Comparativo de costos aprobado |

## Alias y equivalencias
| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Puesto vacío vs puesto operativo vacío | Distinta | Tres responsables tienen cargo en Capital Humano | Se lee ambos campos; no se modifica RRHH |
| Envío a Sucursal | Confirmada por Mauricio | Dos personas organizan solicitudes, transferencias y carga | Coordinación de despacho separada de fabricación |
| PZA / U | Confirmada por Dirección | Recetas de producto terminado expresadas por pieza | Mantener validación de rendimientos explícitos y unidades incompatibles |
| Receta con insumo relacionado | Distinta | 821 filas tienen ambas referencias | Contar una vez por línea; excluir preparaciones |
| Sin receta vinculada | Resuelta por alias existente | Producción 12488, 12542 y 12761: código exacto PMH028; alias activo 513 apunta a receta 511 PREPARACION | Consulta productiva de solo lectura el 14/10/2026; reutilizar alias sin actualizar líneas históricas |
| Total receta como unitario | Confirmada por Dirección | Las 50 recetas describen una pieza; el rendimiento de batida es otro análisis | Ausencia de rendimiento significa una pieza para producto final; conservar rendimientos explícitos |

## Decisión de diseño
Reutilizar fuentes sin segunda captura, sin cambiar nómina ni maestros. Fondo aprobado conceptualmente: elaboración/apoyo $186,624.54; supervisión/control $43,111.24; despacho $17,776.14. El cierre nuevo almacena una evidencia derivada inmutable, con revisión, usuario, huella y fuentes; no sustituye los registros originales. Un SELECT PostgreSQL obtiene un snapshot coherente; la aprobación recalcula y rechaza fuentes cambiadas o pendientes. Concurrencia serializada por mes, revisión única e idempotencia.

Solo los cierres aprobados alimentan el comparativo de Finanzas. Fabricación excluye despacho; la diferencia contra precio promedio vinculado no se presenta como utilidad contable ni modifica el estado de resultados, nómina o valuación. Las revisiones conservan evidencia previa. Un cambio de fuente exige nueva revisión explícita.

Procedimiento reproducible: `inventario_fuentes_datos --term nomina --term produccion --term 'mano de obra'` con PostgreSQL configurado; consultas ORM acotadas al mes mediante `leer_fuentes` / `calcular`. Inventario léxico no prueba equivalencia semántica.

## Riesgos y pendientes
Confirmar cobertura de captura Point, unidades explícitas y vigencia histórica de puestos/estructura antes de aprobar. No mide tiempos, cargas patronales completas, inventarios intermedios ni costo de ventas. El redondeo por componente se concilia explícitamente. Un producto sin costo impide repartir el fondo a los demás. Septiembre permanece como simulación hasta resolver los pendientes, sin completar datos por suposición.

## Recursos temporales
Tarea mano-obra-mensual-20261014, propietario codex-01a13c51. Compose erp_mo_mensual_20261014; PostgreSQL local 55514; servidor local 8058; datos sintéticos exclusivos. Configuración y evidencia fuera del repositorio en `.codex/task-runtime/mano-obra-mensual-20261014`. Cierre y retiro sujetos al registro de ciclo de vida de la tarea.

## Corrección por pieza y servicios — 14/10/2026
Alcance: servicio compartido de cálculo, pruebas y esta ficha. Consumidores: pantalla mensual, Excel, aprobación con huella y comparativo financiero. Sin migraciones ni cambios a maestros, nómina o líneas de Point. Versión de cálculo 2 invalida formularios previos a la corrección. Alias exactos activos se incluyen en el snapshot; un alias ausente/inactivo mantiene el pendiente.

Servicios: reutilizar GastoOperativoMensual, ObligacionGasto y ReglaAsignacionGasto, contrastados con LineaPresupuestoMensual. No sumar obligación y gasto derivado como dos gastos. Septiembre tiene un gasto REAL no estimado de producción (1961), $441, enlazado a obligación 544; esto no prueba cobertura de luz, gas y agua. La regla 2 existente para INDIRECTO_PROD / PROD reparte por COSTO_MP. La integración de servicios requiere conciliar cobertura y conceptos antes de presentar costo completo; no sustituir faltantes por cero ni usar presupuesto como gasto real.

Entorno de esta corrección: tarea mano-obra-pieza-20261014, propietario codex-01a13c51, Compose erp_mo_pieza_20261014, PostgreSQL 55515 y web local 8059; datos sintéticos, registro en `.codex/task-runtime/mano-obra-pieza-20261014`.

La consulta ampliada encuentra electricidad compartida: gasto 1323, $39,205.00; regla existente en `reportes/data/mapeo_rubros_fuentes.csv` asigna 65% a producción ($25,483.25). Gas (rubro 1185) y agua potable (1183) tienen monto_real NULL en septiembre; no son cero. Agua Purificada (1206, $2,553.03) proviene de CONSUMO_MP y no debe sumarse otra vez como servicio. La fumigación 544/1961 son dos representaciones del mismo gasto $441. Arrendamiento local ya refleja $43,036.58 del gasto compartido 2121; es otro indirecto, separado de servicios. Estas son fuentes verificadas, no un cierre completo de servicios.
