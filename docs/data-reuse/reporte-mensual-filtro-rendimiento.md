# Ficha de fuentes — rendimiento del reporte mensual filtrado

Fecha: 19/11/2026. Base 29c778fb; PostgreSQL16 local exclusivo55635, sin datos operativos. Baseline real único: filtro Bamoa17/hasta2026-10 HTTP504 en122.383s/origen120.040s,18:38:29UTC. El GET global de QA1760 devolvió200/32.331s. No atribuir la diferencia al CSS ni afirmar regresión introducida por1759 sin una medición filtrada anterior.

## Necesidad y unidad

Resultado mensual por sucursal, con doce meses de salida, sus bases anuales cuando aportan un YoY comparable y desglose de personal/alcance. Evitar costear toda la red en bases que el consumidor finalmente descarta; no eliminar meses o fuentes necesarios, no convertir ausencia en cero ni cambiar reglas financieras.

## Fuentes existentes

|Concepto|Fuente y autor|Clave/ámbito|Evidencia y consumidores|
|---|---|---|---|
|Meses candidatos|SucursalRentabilidad + point_sales_evidence_months + selected_sales_facts|periodo y límites existentes; lecturas canónicas|_rentabilidad_por_mes comparte fuentes con HTML/JSON/CSV/CLI|
|Reparto conciliadoEne–Sep2026|leer_cierre_conciliado/resumen_cierre_mensual|mes+sucursal, guardas/errores existentes|Sustituye importes gerenciales; filtro aplicado antes de construir filas|
|Nómina retenida en cierre|_nomina_del_cierre, lectores existentes de personal/gastos y adaptadorcostos_de_sucursal|mes+sucursal, None/0 y perímetro actuales|Campos de nómina del mismo reporte; no writer ni nueva captura|
|Meses operativos/baseYoY|registros_del_mes→leer_costos_mensuales→leer_resultado_mensual|mes, red completa; filtro por sucursal se aplica después|Costeo material/merma y gastos originales; no lector branch-scope equivalente encontrado en estas firmas|
|Comparación anual|_fila_mes/_yoy en build_reporte_mensual_consolidado|mismo mes−12; mismos alcances y conjuntos de sucursales|Solo3derivadosYoY; devuelveNone si actual/base falta o base0|
|Evidencia ya archivada|JSON real de1759 y qa-504-bamoa.json de1760|corte de cada archivo, no se usa como fuente operativa|Bamoa no tiene filas de repartoEne–May; Junio solo fila de extrasSIN_DATOS en el export17:51UTC; no prueba fecha de apertura|

## Trazado y contratos

Grafo intentado en esta sesión: Transportclosed, por lo que se usó búsqueda acotada de símbolos. Callers productivos de build_reporte_mensual_consolidado: view reporte_mensual (HTML/JSON/CSV, doce meses fijos) y comando reporte_mensual_consolidado (meses configurables). _rentabilidad_por_mes se usa por ese constructor y pruebas de componentes; sus nuevos flags internos de1759 ya existen. registros_del_mes comparte semántica con Rentabilidad, por lo que no se agrega una variante incompleta ni un contrato/API nuevo por conveniencia.

El código conserva bases2025 cuando la selección no tiene filas en el cierre2026, aunque el consumidor puede acabar mostrando todos los YoY comoNone. Esa es una hipótesis medible; no se omite ninguna base sin demostrar paridad, especialmente cuando la base es un mes de salida, falta el cierre o sí existen ceros válidos/comparabilidad. No inferir apertura por nombre o ausencia de ventas ni asignar identidades nuevas.

## Decisión y límites previos a implementar

Reutilizar fuentes/consumidores existentes. Primero perfil publicado acotado y pruebas de paridad local; decisión compatible solo sobre trabajo que no aporta al resultado observado. No snapshots antiguos, timeout nuevo, caché/tabla/app nueva, modificación de ventas/nómina/presupuesto, cambios de writer o parámetros públicos. Se conservarán errores de cierre y semántica de None frente0, períodos solicitados y bases comparables. El registro de perfil documentará exactamente los meses y funciones que dominan antes de elegir el cambio.

Inventario automatizado de fuentes ejecutado con términos rentabilidad/ventas/nomina y PG16 propio; produce candidatos léxicos, no equivalencias. Perfil productivo aún no ejecutado al crear esta ficha: script externo con RRREADONLY, caché privada, sockets/callbacks bloqueados y85s totales/15s por fase, pendiente ratificación técnica de raíz. No volver a ejecutar el GET Bamoa de120s como diagnóstico. Una sola recertificación de base/CI tras main estable, coordinada con Cierre y NAS; tareas/recursos separados.


## Perfil publicado y solución seleccionada

Una única ejecución autorizada de29c778fb:26.421s de invocación,23.759s del proceso, RRREADONLY69844482, caché privada y red/callbacks bloqueados. Callplan global6meses y Bamoa12: los6extras sonEne–Jun2025. Mayo2026 seleccionado devuelve0filas/2.051s; Mayo2025 intenta costeo de toda la red y se interrumpe por el límite diagnósticoSQL5s (6.186s observados, no tiempo de finalización). Octubre2026 sí tiene fila válida,1.262s; Octubre2025 se conserva como base necesaria de la comparación y tarda5.569s. No se repitió elGET de120s ni se elevó timeout. Artefactos fuera del repo:profile-published.jsonl yprofile-launch.json,0600.

Las firmas publicadas no ofrecen ámbito por sucursal en leer_resultado_mensual/ventas_para_costeo; registros_del_mes acepta un DTO de costos ya construido. Extender esos lectores compartidos implicaría un contrato y alcance mayor. Se eligió omitir únicamente bases fuera de la salida cuando el mes gerencial aceptado no tiene filas de la selección: _fila_mes(periodo,{},horas) conserva ingresos/nómina/utilidadNone, y_yoy(None,cualquierbase) devuelveNone incluso si la base tiene cantidades, importes o extras válidos. Ningún mes de salida se omite; bases comparables de meses abiertos, ceros registrados, falta de cierre y guardas de errores permanecen.

Prueba previa al cambio: paridad de JSON/CSV completos1/12meses global/seleccionado con base rica, extras y cero actual pasa en el código publicado; la nueva regresión de trabajo descartado falla exactamente por la llamada sobrante aSept2025. Tras el guard,13regresiones pasan. El antiguo test que exigía ese costeo para una sucursal inexistente ahora verifica su contrato observable más fuerte: alcanceSIN_DATOS, todos los importesNone yYoYNone; no se elimina el caso. Se añaden pruebas de base rica que sí es mes de salida13meses y cierre ausente conYoY válido100%. No se define ninguna fecha de apertura ni se cambia fuente/modelo/API.
