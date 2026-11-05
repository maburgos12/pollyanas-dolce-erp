# Ficha de fuentes — personal en rentabilidad histórica 2025

Fecha y ambiente consultado: 2026-11-05, PostgreSQL 16 de producción, transacción de solo lectura. Autorización: cierre 2025 con pagos y costos reales, sin duplicidad ni asignaciones inferidas; no se modifican nómina, RRHH ni datos maestros.

## Necesidad y unidad de análisis

El costo laboral conciliado existe en el ERP, pero el resultado de empresa de Rentabilidad lee únicamente controles presupuestales centrales y nómina de ventas con sucursal histórica. En los doce meses de 2025 ambos lectores devuelven cero importes laborales, aunque las fuentes canónicas contienen nómina y cédulas conciliadas. La unidad es empresa/mes/componente; una adscripción desconocida no borra el gasto ni autoriza repartirlo.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Nómina real y despensa incluida | `rrhh.NominaPeriodo`, `NominaLinea`, `NominaConceptoLinea` | Importación histórica conciliada CONTPAQi/CFDI, auditoría 42539 | Periodo y empleado canónicos, snapshots históricos | 75 periodos, 1764 líneas, 6819 conceptos; percepciones 8,018,073.26 | `leer_personal_registrado`, planeación, rentabilidad |
| IMSS y RCV/Infonavit | `reportes.ExpedienteCedulaIMSS`, documentos y `pago_conciliado` | Cédulas aplicadas, auditoría 42589 | Registro E5240157100/tipo/periodo aplicado único | 18 expedientes, 1311 detalles; gasto de empresa 1,954,688.81, distinto al pago total | `leer_pago_conciliado`, `build_personnel_plan` |
| ISN y servicio de vales | `ExpedienteISN`, `sat_client.CfdiDescargado` | Expedientes aplicados/CFDI vigente de la entidad | Periodo y UUID, sin repetir saldo de vales | Lectura de los doce meses con el lector existente, sin errores | Planeación y resumen personal global |
| Controles y asignación previa | `LineaPresupuestoMensual`, `leer_personal_mensual` | Consolidación original y nómina directa | Rubro/periodo/versión y sucursal histórica | En 2025 no hay costos centrales ni laborales directos reconocidos | Rentabilidad y sus exportaciones |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Nómina y desglose de conceptos/bonos | Confirmada, misma fuente económica | El lector canónico concilia conceptos y despensa. No se suma el desglose sobre el total | Ninguna equivalencia nueva |
| Pago de cédula y gasto de empresa | Distintos | El control validado excluye retenciones y cuentas por cobrar | Reutilizar regla conciliada existente |
| Sucursal actual e histórica | No equivalentes | Los documentos 2025 conservan sucursal histórica vacía | No inferir ni actualizar RRHH |
| Costo de material y costo laboral | Distintos | El lector de recetas excluye mano de obra e indirectos | No usar un costo integral de producto como material sin comprobar su composición |

## Decisión de diseño

Reutilizar `build_personnel_plan` y `leer_personal_mensual`, exclusivamente en la lectura de 2025. Mostrar nómina, IMSS, RCV/Infonavit, ISN y servicio de vales en el desglose central existente como costo de empresa sin distribución histórica. Descontar lo que ya se reconoce por sucursal. Los controles presupuestales de las mismas fuentes no se suman sobre los recibos; si un control mezcla fuentes laborales y no laborales, conservarlo y dejar pendiente el componente afectado hasta conciliar su desglose.

No crear tablas, migraciones, capturas, equivalencias de personas, prorrateos, provisiones ni escrituras financieras. Conservar el diseño de la pantalla. El reporte mensual y el selector, incluso desde la vista del cierre de 2026, deben descubrir también los meses con ventas históricas canónicas sin snapshot.

Procedimiento reproducible: `inventario_fuentes_datos --term nomina --term cedula --term rentabilidad`; lectura de `personal2025-puente-resultado-comprobacion.json` en el expediente privado de cierre. El grafo MCP devolvió `Transport closed`; descubrimiento acotado sobre los lectores y todos sus consumidores.

Validación: reproducción inicial con 6 fallos de 7 pruebas; nuevos casos de control mixto y exportación histórica fallan antes de corregirlos. Pruebas PostgreSQL del lector compartido, integración con pantalla/reporte, conservación de snapshots y ausencia de escrituras. Los importes de producción se vuelven a contrastar con el lector canónico después del despliegue, no con constantes de esta ficha.

Riesgos y pendientes: el resultado sigue siendo parcial mientras falten costos materiales, soportes operativos y asignaciones históricas. La nómina neta de recibo no equivale al flujo bancario. No se certifica el cierre integral por incorporar estos componentes. Reutilizar el lector de planeación añade su tiempo de consulta; la comprobación anual de fuentes en producción tardó 4.97 s.
