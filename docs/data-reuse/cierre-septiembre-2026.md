# Ficha de fuentes — cierre gerencial enero–septiembre 2026

Fecha y ambiente: 2026-11-03, PostgreSQL de producción. Lectura previa, simulacros con huellas, aplicación transaccional autorizada y verificación independiente.

## Necesidad y unidad de análisis

Conservar el reporte existente y extender su reparto hasta septiembre. Un renglón del cierre representa rubro, mes y versión ORIGINAL. El reparto por sucursal consume ese renglón una vez, no agrega nuevamente documentos o conceptos de nómina.

## Fuentes candidatas

| Concepto | Modelos | Fuente y clave | Evidencia | Consumidores |
| --- | --- | --- | --- | --- |
| Ventas | PointDailySale | Importación Point; sucursal/producto/fecha | Conciliación mensual contra Ingresos | Estado de resultados, reparto, rentabilidad |
| Personal | NominaLinea, NominaConceptoLinea | Recibo/empleado/periodo; concepto por línea | Nueve meses: suma de percepciones completas y despensa 32 igual al cierre, diferencia cero | LineaPresupuestoMensual ORIGINAL |
| Identidad histórica | NominaLinea snapshots | Departamento y sucursal del recibo | Se conserva la asignación histórica; no se altera RRHH | Distribución de personal |
| IMSS | ExpedienteCedulaIMSS, DocumentoCedulaIMSS, DetalleCedulaIMSS | RP/periodo/tipo/revisión; SHA256 documento; NSS/documento | Expediente 13; 71 detalles únicos y cruzados; XLS y EMA con huellas verificadas | Presupuesto real, rentabilidad |
| Hallazgos | QualityFinding, AuditLog | finding_key y fuente estable | Hallazgos registrados con referencias, estado y evidencia | Orquestación/calidad |

## Alias y equivalencias

- Cédula SUA mensual original y copia modificada son versiones distintas, no dos pagos. La EMA es propuesta y no sustituye al SUA conciliado.
- Pago bancario total y cuota patronal son conceptos distintos: la cuota obrera retenida no es otro gasto patronal.
- Identidad adicional del expediente 13: NSS único y recibo de septiembre del empleado 36; no se reactivó ni se modificó su baja.
- Se reutilizan clasificación de nómina y destinos existentes: CEDIS/elaboración en producción; RRHH/mantenimiento en administración; despacho en logística. Ventas conserva sucursal del snapshot.
- Nombre repetido o importe coincidente no demuestra identidad de personas, recetas, facturas o pagos.

## Decisión

Reutilizar cierre ORIGINAL, importador de cédulas y registro de hallazgos. No crear tablas maestras ni segundas capturas. Las correcciones históricas de personal quedan protegidas de reconsolidación automática y conservan originales y referencias en metadata/AuditLog. La nómina original permanece intacta.

El cambio de código solo extiende el periodo leído, etiquetas y exportación hasta septiembre; conserva fórmula, distribución y estructura visual. Pantalla, informe mensual y CSV comparten el mismo reparto.

## Validación y límites

Inventario de fuentes IMSS/cédula/hallazgo ejecutado en producción; consultas acotadas en transacciones de lectura. Simulacros y respaldos externos en Documents/Codex/2026-11-03/cierre-septiembre. Pruebas de identidad histórica, conservación de fondos, bloqueo ante diferencias, permisos, lectura sin escrituras, septiembre y exportaciones.

La conciliación aritmética no certifica cobertura completa. El pago mensual de septiembre efectuado en octubre no incluye RCV/Infonavit: septiembre–octubre vence el 17 de noviembre de 2026 y no estaba vencido al revisar. Queda abierta la conciliación del componente patronal de los $29,315.14 del Excel; no se consideran un pago bancario confirmado ni se infiere costo cero. Hallazgo 18 aclarado en ERP con calendario oficial de Infonavit. También permanecen identificadas las candidaturas de duplicidad de catálogos/eventos; no se fusionan por nombre ni se borran bitácoras. La consolidación automática de meses posteriores requiere corregir cobertura y asignación histórica; los meses corregidos están protegidos.
