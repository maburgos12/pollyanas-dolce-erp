# Ficha de fuentes — clasificación conservadora del auditor

Fecha y ambiente consultado: 2026-10-01; PostgreSQL de producción, consultas de septiembre 2026 y metadatos Django. Pruebas en PostgreSQL 16 aislado.

## Necesidad y unidad de análisis

Clasificar cada expediente mensual de producto y ubicación sin confundir evidencia de identidad con discrepancias de inventario. Corregir prioridades al reconstruir el mes y ejecutar la investigación automática ya existente.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Expediente | reportes.ProductInventoryAuditCase | InventoryAuditMaterializer | mes, PointBranch, PointProduct; clave única existente | 1,958 casos: 1,012 conciliados, 467 excepciones, 479 incompletos | auditor, pantalla y eventos |
| Investigación | investigation_summary, source_trace del expediente | InventoryAuditAgent e historial Point conservado | expediente y fingerprint | 247 excepciones de saldo cero únicamente con PRODUCT_RESOLVED_BY_SKU/NAME; 43 conciliados con prioridad alta | detalle del expediente y notificaciones agrupadas |
| Evidencia logística | pos_bridge.PointTransferLine; logistica.DiscrepanciaLogistica | sincronización Point y carga/recepción logística | transferencia, detalle, source_hash; relación explícita | 105 casos prioritarios sin asignación, motivo: ausencia de jefatura activa única de Logística | auditor y Logística |
| Responsable | rrhh.Empleado.usuario_erp | maestros RRHH existentes | empleado activo, departamento y nivel organizacional | sin empleados activos en departamento LOGISTICA en la consulta; conservar casos sin inventar responsable | asignación del auditor |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| PRODUCT_RESOLVED_BY_SKU / PRODUCT_RESOLVED_BY_NAME | Evidencia informativa existente | el resolvedor ya eligió la identidad; AMBIGUOUS_PRODUCT y UNRESOLVED_PRODUCT conservan tratamiento de pendiente | no se crean equivalencias nuevas |
| Alias de sucursal Point | Reutilizar resolución vigente | canonical_point_branch_identity ya concentra por sucursal ERP | ningún cambio de maestros |
| Diferencia numérica cero con transferencia/conversión pendiente | Distinta de conciliación completa | el saldo puede cuadrar y aún existir una discrepancia operativa explícita | conservar evidencia y revisión |

## Decisión de diseño

Extender los dos servicios actuales, sin nuevas tablas, capturas, descargas ni modelos de aprendizaje. Mantener los códigos informativos y su evidencia; ignorarlos únicamente para decidir si un saldo cero es excepción. Actualizar clasificaciones obsoletas aunque el fingerprint de cantidades permanezca igual. Preservar aprobaciones y resoluciones humanas cuando la evidencia no cambió.

Un expediente conciliado no se eleva por antecedentes sin discrepancia logística abierta. La reincidencia de inventario requiere diferencia numérica actual y previa, con fuente previa completa. Una fuente incompleta no acredita una diferencia ni un saldo cero. La asignación conserva únicamente responsables explícitos o jefaturas únicas; no se reasignan casos a una persona arbitraria.

La reconstrucción existente ya ejecuta InventoryAuditAgent.run_month mediante transaction.on_commit; se reutiliza ese contrato y se prueba la ejecución efectiva. Las notificaciones mantienen fingerprints para evitar repetición.

Procedimiento reproducible: inventario_fuentes_datos --term auditoria --term inventario --term transferencia; consultas ORM acotadas al mes 2026-09 sobre estados, prioridad, issue_codes y cobertura opening/closing. El catálogo devolvió 28 candidatos léxicos; no demuestra identidad semántica.

Riesgos y pendientes: no subsana cortes faltantes ni conversiones sin origen, no bloquea el cierre mensual ni modifica inventario. La ausencia de responsable de Logística conserva el motivo para designación humana. Se requiere rebuild de septiembre y validación autenticada tras despliegue.
