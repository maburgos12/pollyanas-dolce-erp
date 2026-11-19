# Ficha de fuentes — lectura del reporte mensual

Fecha y ambiente:19/11/2026, base1efa0e51;PG16 local propio55632. Diagnóstico previo de producción: JSON del reporte12meses504 tras120.196s; no repetido. Esta tarea no escribe fuentes operativas.

## Necesidad y unidad de análisis

Una fila mes/sucursal conserva ingresos, resultado, nómina informativa, horas extra, partidas y alcance fiscal/gerencial. Evitar costear cifras que el cierre conciliado sustituye y evitar construir dos veces el mismoCSV. No ampliar el contrato HTTP ni reducir meses/YoY.

## Fuentes candidatas

|Concepto|Modelo/lector|Productor|Identificador/ámbito|Evidencia|Consumidores|
|---|---|---|---|---|---|
|Cierre conciliado|leer_cierre_conciliado/resumen_cierre_mensual|EstadoResultados y reparto histórico existentes|mes2026enero–septiembre/sucursal|Validación/etapa/partidas del mismo cierre; errores siguen409|Reporte mensualHTML/JSON/CSV y CLI|
|Nómina y cargas|leer_personal_mensual/costos_de_sucursal|RRHH/SUA/cédulas existentes|período/sucursal/familia|Mapeo nomina y cargas_patronales a nomina_directa;None difiere de0|Rentabilidad/reporte; sin cambiar importes|
|Perímetro previo|SucursalRentabilidad/ventas_para_costeo/MermaMensualSucursal/gastos|Fuentes actuales y registros existentes|sucursal/mes/actividad|Costo descartado no permite saltar elegibilidad de una fila|Nómina informativa del cierre|
|Meses con datos|point_sales_evidence_months/selected_sales_facts/SucursalRentabilidad|Lectores canónicos|mes fiscal/Point/persistido|Sin nueva captura ni interpretación de identidad|Meses salida y su baseYoY|
|CSV|Command._render_csv|Reporte ya construido|mismo objeto/columnas|Vista lo reconstruye mediante call_command; serializador ya probado|CLI/vista export|
|Perímetro de avisos|canonical_point_network_branch_qs/Sucursal|Catálogo comercial existente|códigoERP y fecha cierre|CEDIS10 no está en red canónicaOct, evidencia readonly del expediente1756; desconocidos no se suponen centros|Solo faltantes automáticos; filas nómina/SUA reales se conservan|
|TextoBI verificado|financial_projection_verified del DTO existente|AjusteOct1756|octubre2026|44.6% incluye material+reventa+merma, no solo producción|Frase explicativaBI; sin cambiar importes|

## Alias y equivalencias

Nómina informativa incluye percepciones y cargas según adaptador actual: no es un nuevo reparto. Hora extra sigue informativa y no se suma de nuevo. CentroERP CEDIS es identidad documentada, no una coincidencia por nombre. No se agregan alias de productos/personas. Los centros desconocidos conservan avisos; ausencia del catálogo por sí sola no acredita baja ni no-aplicabilidad.

## Decisión de diseño

Reutilizar cierre y lectores/adaptador de personal/gastos para recuperar exactamente la única cifra que sobrevive al cierre. Conservar elegibilidad previa por fuente/registro/actividad/gasto, camposNone/0 y montos al centavo. Meses sin cierre válido mantienen costeo completo; errores de cierre no se silencian. La API interna meses ya existe: procesar solo meses de salida y sus mismos meses del año anterior, sin añadir queryparam/UI ni cambiar default12. CSV serializa el objeto existente; ninguna tabla/caché/timeout nuevo.

Extensión autorizada de alcance: no generar faltantes automáticos de ventas para el código documentadoCEDIS fuera de red comercial; no retirar filas reales ni ocultar centros desconocidos. TextoBI cambia solo si financial_projection_verified, conservando fraselegacy para los demásmeses. Las tres versionesSW se alinearán a versión posterior1758 en el mismo commitHTML.

Consultas/procedimiento: inventario_fuentes_datos --term nomina --term cierre --term rentabilidad enPG16 propio produjo25candidatos léxicos;512migraciones/check0 y0registrosSucursalRentabilidad iniciales. Evidencia operativa procede de lecturas previas acotadas y QA, no de nuevos barridos. Grafo no disponible(Transportclosed); funciones/callers leídos por rutas conocidas. No ejecutar nuevamente elGET12meses para medirbaseline.

## Validación y límites

ParidadJSON/CSV completos antes/después, nómina/cargas/cero/faltantes, actividad/perímetro, fuentes2025/Oct, cierre válido/ausente/inválido, etapa/cobertura yYoY. Probar una sola construcciónCSV y conjunto exacto de meses internos. Medir condiciones equivalentes locales y luegoHTTP real traspublicación, sin interpretar120s de timeout como tiempo exacto del cálculo. Revisor independientePoint y raíz antesCI; único rebase/CI tras1758. No modifica fuentes, maestros, nómina, financieros ni agregaciones guardadas de presupuesto. La ampliación de metaBI y su origen se delimita a continuación.

Primer cotejo controladoPG16 propio con fixture histórico existente: tres pares941→851consultas; mediana0.554→0.504s. JSON completoSHA25c19515737fb7c5597da40617184f861d01b2d96ab23658d36be2653a3b17f2 yCSVSHA49127bf6f7f7a561ffb2f67453bdf4f74293a980599bb674d6f21c9f0a23f91f idénticos; meses1/12 yglobal/Bamoa/Crucero iguales, fuentes sin cambios y0DML/callbacks. No extrapolar ese ahorro local a producción: la aceptaciónHTTP12meses aún debe medirse y resolver el504 real. El perfil real acotado queda coordinado con raíz; no se repitió elGETfallido.

## Ampliación ratificada con evidencia publicada

Una extracción autorizadaORIGINALOct de810líneas y dos meses del lector PUBLICADO, sin candidato: snapshot1efa,hash9167c3234a31dd3c13238eef318f295b4a93f4738ca4cfab395f57772d6f5827,0DML/callbacks/conexionescerradas. PerfilOct2025:6.928s dominado por leer_resultado_mensual6.560s;Sep2026:.875s/resultado.511s. Raíz autoriza omitir solo basesYoY2025 FUERAde los meses solicitados y con contraparte2026 realmente poblada de REPARTO_CONCILIADO: la regla vigente ya impide eseYoY por distinto alcance. Conservar salida2025,YoYOct2026, cierre ausente/sinfilas y errores409.

La misma extracción prueba ventas94rubrosactivos5,316,986 frente a104incluyendo250,290inactivos. Fuente reutilizable: PresupuestoMaestroService._budget_lines_for_kpi(year,ORIGINAL,area=None). BI usa eseQS cuando no hay fuenteGENERAL para meta:0confilas sigue0; ausencia esN/D. No hay nuevoalmacén ni nueva proyección desacoplada. Raíz retiró después la autorización de corregir el ingresoAPI por separado: mezclar ventasactivas con costosOriginal rompería la cohorte delPNL. API/tipos/firmas/writer/importesOriginal permanecen intactos; armonizar elPNL requiere revisar su paquete completo, no forzar igualdadconBI.

No se acreditaron los demás componentesBI de igual definición: materialPNL solo tiene agregadoINACTIVO1,929,739.42; reventa no tiene filas; recurrentesPNL no equivalen al detalle auditadoBI. Sus componentes/resultado presupuestado aparecenN/D, no beneficio calculado restando ausencias0. CAPEX tiene3filas0documentadas y sigue fuera del resultado operativo. No se activan rubros ni se modifica presupuesto. Rubro inactivo de presupuesto NO significa producto ocultoPoint; son conceptos y estados distintos. Configurar el mapeo completo con fuentes existentes queda como decisión pendiente, sin pedir otroExcel.

El mismo DTO deBI conserva fórmulas y añade procedencia de meta; importes presupuestados sinfuente sonNone/N/D solo en ese consumidor, incluidos derivadosYTD si falta un mes de sualcance. API ywriter permanecen byte a byte intactos. Se incrementa únicamente la generación de la sección existente de caché ejecutiva para que no sirva el DTO antiguo: mismo scope/TTL/mecanismo, sin vaciado global ni caché nuevo. Las tres referenciasSWv15 acompañan este mismo HTML.
