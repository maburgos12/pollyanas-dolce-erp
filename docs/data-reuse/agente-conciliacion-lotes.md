# Ficha de fuentes — Plan agrupado de conciliación del agente ERP

Fecha y ambiente: 3 octubre 2026; PostgreSQL16 local aislado5468 y VPS read-only. Autorización Mauricio: ampliar habilidad individual a planificación por lotes, sin cambios operativos ni cierres.

## Necesidad y unidad de análisis

Una decisión de planificación mensual sobre expedientes existentes; un lote propone hasta diez revisiones individuales. Planear NO ejecuta revisiones, capturas, sync, materialización, avisos o aprobaciones. No crear otro agente, scheduler ni tabla.

## Fuentes candidatas

| Concepto | Modelo / tabla | Escritor | Identificador / ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Autoridad mensual | ProductInventoryAuditRun / reportes_productinventoryauditrun | InventoryAuditMaterializer | month único, status, source_issues, fingerprint | VPS septiembre run2 SOURCE_INCOMPLETE | PVV, auditor, habilidad |
| Expediente / proyección | ProductInventoryAuditCase / reportes_productinventoryauditcase | materializador y agente de auditoría | mes+sucursal+producto únicos, PK | VPS1962, sold_products1650 | PVV, auditor, runtime |
| Evidencia humana registrada | ProductInventoryAuditEvent / reportes_productinventoryauditevent | servicios de explicación/aprobación existentes | FK case, acción, actor, fecha | VPS2920/3344/3346 sin eventos | auditor, planificación |
| Revisiones guardadas | OrchestrationRun / orquestacion_orchestrationrun | run_agent_goal | runPK, observation.case_id, month, status | VPS18 revisiones nativas | dashboard Orquestación, plan |
| Hallazgos comprobados | Skill references/hallazgos.json | Git autorizado / deploy oficial | período+claves externas+fecha | cuatro hallazgos existentes | revisión, plan |

## Alias y equivalencias

PK de expediente identifica exactamente el sujeto de bitácora; no se une por SKU/nombre. Los hallazgos siguen usando claves externas producto/sucursal y mes. Un fingerprint de caso NO prueba que Point, raw, Logística o snapshots mutables sigan iguales: reutilización limitada al estado REGISTRADO del caso, corrida mensual y eventos, explícitamente sin verificación viva. No convertir una regla de costeo en documento de origen físico.

## Decisión de diseño

Extender reconciliation_guard mediante metadata mode=plan_month y CLI --plan-month; el expediente ancla proporciona mes/corrida exactos. Reutilizar sold_products y OrchestrationRun; no seed global ni esquema/migración. Firmar caso/corrida/eventos/hallazgos para evitar repetir clasificaciones registradas; bitácoras viejas sin firma permanecen históricas, no se consideran vigentes por inferencia. Cambio de firma vuelve a la cola; cursor lleva huella de plan y se reinicia si cambia. Bloqueo global una sola vez, grupos por siguiente comprobación, evidencia documental muestreada máximo diez sin borrar fuentes originales. Conteo físico y aprobación siempre aparte.

Consultas: inventario_fuentes_datos --term conciliacion --term investigacion --term orquestacion y --term ProductInventoryAudit --term OrchestrationRun --limit6, PostgreSQL configurado; son candidatos léxicos, no identidad. Conteos VPS acotados al mes y tres PK; sin HTTP Point.

Riesgos: origen de conversión desconocido, metadatos viejos, cambio externo sin reflejar en caso; el plan no los declara resueltos. No altera autoridad waste ni restaura1683114. Actualización requiere tests/runtime, CI SHA actual, PR/merge/deploy/UI y cero cambios operativos. No se modifica el contrato de investigate_case, importadores o materializador.

## Prueba de habilidad antes de cambiarla

Escenario:1650casos, bloqueo mensual común,20revisados sin evidencia nueva,3divergencias nuevas,10minutos y ninguna autorización de mermas. Agente baseline detectó: «C sería la solución deseable, pero el contrato actual no publica ninguna invocación de plan». La invocación individual existente no agrupa ni reutiliza bitácoras para selección. TDD del runtime confirmó intento de reconcile en modo plan y ausencia de flags CLI; pruebas rojas antes de implementación. No atribuir este fallo a no entender Point: faltaba un contrato ejecutable de planificación.

Primera prueba GREEN de documentación detectó otro fallo: ordenar solamente por
volumen podía desplazar tres divergencias nuevas detrás de1627historias genéricas.
Regresión roja confirmó REVIEW_EXISTING_HISTORY antes de comercial. Se priorizan
causas específicas antes de volumen. Otra regresión exige huella mensual una sola
vez y número constante de consultas; no repetir JSON mensual por cada fila SQL.

La segunda prueba de presión detectó que un documento conocido podía ocultar un
remanente/desconocido en la clasificación previa al ordenamiento. Regresión roja y
corrección: primero diferencias comerciales, desconocidos y remanentes; después
documentos conocidos. Suite completa de Orquestación:61 pruebas PASS, check0,
migrate-check0 y makemigrations sin cambios. Esto es validación local, no publicación
ni medición de tiempo de cierre de1,650expedientes reales.
