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
| Identidad y rendimiento | recetas.Receta / maestros.UnidadMedida | Catálogo ERP | receta_id, codigo_point | 50 finales vinculados; rendimientos pendientes | Costeo y producción |
| Costo mensual | reportes.RecetaCostoHistoricoMensual | Costeo histórico | receta_id y periodo | 50 costos positivos con cobertura 100%; estructura actual | Reportes financieros |
| Precio observado | pos_bridge.PointDailySale | Sincronización Point | receta_id y fecha | Solo ventas vinculadas; ausencia se muestra sin precio | Comparativo de costos aprobado |

## Alias y equivalencias
| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Puesto vacío vs puesto operativo vacío | Distinta | Tres responsables tienen cargo en Capital Humano | Se lee ambos campos; no se modifica RRHH |
| Envío a Sucursal | Confirmada por Mauricio | Dos personas organizan solicitudes, transferencias y carga | Coordinación de despacho separada de fabricación |
| PZA / U | Candidata | Se admite únicamente unidad de pieza; no kg/litros | Confirmar rendimiento antes del cierre |
| Receta con insumo relacionado | Distinta | 821 filas tienen ambas referencias | Contar una vez por línea; excluir preparaciones |
| Sin receta vinculada | No resuelta | Tres líneas, 26 piezas de Pan de Muerto Horneado | Bloquea aprobación; no se inventan enlaces |
| Total receta como unitario | No resuelta | Las 50 recetas carecen de rendimiento | Se conserva simulación con advertencia; aprobación bloqueada |

## Decisión de diseño
Reutilizar fuentes sin segunda captura, sin cambiar nómina ni maestros. Fondo aprobado conceptualmente: elaboración/apoyo $186,624.54; supervisión/control $43,111.24; despacho $17,776.14. El cierre nuevo almacena una evidencia derivada inmutable, con revisión, usuario, huella y fuentes; no sustituye los registros originales. Un SELECT PostgreSQL obtiene un snapshot coherente; la aprobación recalcula y rechaza fuentes cambiadas o pendientes. Concurrencia serializada por mes, revisión única e idempotencia.

Solo los cierres aprobados alimentan el comparativo de Finanzas. Fabricación excluye despacho; la diferencia contra precio promedio vinculado no se presenta como utilidad contable ni modifica el estado de resultados, nómina o valuación. Las revisiones conservan evidencia previa. Un cambio de fuente exige nueva revisión explícita.

Procedimiento reproducible: `inventario_fuentes_datos --term nomina --term produccion --term 'mano de obra'` con PostgreSQL configurado; consultas ORM acotadas al mes mediante `leer_fuentes` / `calcular`. Inventario léxico no prueba equivalencia semántica.

## Riesgos y pendientes
Confirmar cobertura de captura Point, rendimiento/unidad y vigencia histórica de puestos/estructura antes de aprobar. No mide tiempos, cargas patronales completas, inventarios intermedios ni costo de ventas. El redondeo por componente se concilia explícitamente. Un producto sin costo impide repartir el fondo a los demás. Septiembre permanece como simulación hasta resolver los pendientes, sin completar datos por suposición.

## Recursos temporales
Tarea mano-obra-mensual-20261014, propietario codex-01a13c51. Compose erp_mo_mensual_20261014; PostgreSQL local 55514; servidor local 8058; datos sintéticos exclusivos. Configuración y evidencia fuera del repositorio en `.codex/task-runtime/mano-obra-mensual-20261014`. Cierre y retiro sujetos al registro de ciclo de vida de la tarea.
