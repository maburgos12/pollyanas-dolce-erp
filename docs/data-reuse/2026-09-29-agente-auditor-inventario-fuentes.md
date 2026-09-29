# Ficha de fuentes — Agente auditor de inventario

Fecha y ambiente consultado: 2026-09-29; PostgreSQL local aislado y PostgreSQL de producción en modo de solo lectura.

## Necesidad y unidad de análisis

El agente necesita investigar, priorizar y asignar el expediente mensual existente. La unidad de análisis sigue siendo `mes + sucursal Point canónica + producto Point`. La asignación, investigación y notificación son atributos del expediente; no constituyen una nueva transacción de inventario.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Expediente mensual | `reportes.ProductInventoryAuditCase` | Materializador mensual de Reportes | Único por mes, sucursal Point y producto Point | Agosto: 2,026 casos; 825 conciliados, 695 por explicar y 506 con fuente incompleta | Auditoría mensual y detalle |
| Corrida mensual | `reportes.ProductInventoryAuditRun` | Materializador mensual | Mes | Agosto: una corrida `SOURCE_INCOMPLETE` | Resumen mensual |
| Historial humano | `reportes.ProductInventoryAuditEvent` | Explicación, aprobación o rechazo | Caso + evento append-only | Agosto: 0 eventos antes de esta ampliación | Detalle del caso |
| Evidencia Point | modelos `Point*Line` referidos en `source_trace` | Réplica existente de Point | ID interno y referencia externa Point | Ya consumida por el balance; no se requiere nueva descarga | Balance, evidencia y trazabilidad |
| Trazabilidad logística | `logistica.RutaCargaChecklistLinea` | Sincronización de rutas y recepción | FK exacta `point_transfer_line` | La línea conserva transferencia y detalle Point | Rutas y revisión logística |
| Aclaración logística | `logistica.DiscrepanciaLogistica` | Flujo de carga/recepción | Línea de carga + origen | Una abierta en agosto: 3 Pecados Chico, folio RUT-202608-0029 | Revisiones de Logística |
| Responsable organizacional | `rrhh.Empleado` y `core.UserModuleAccess` | RRHH y administración de accesos | Usuario ERP, departamento y nivel organizacional | Jefaturas activas identificables para Producción, Ventas y Administración; gestión de Logística ya configurada | Permisos, avisos y asignaciones |
| Notificación | `core.Notificacion` | Servicio común `crear_notificacion` | Usuario + objeto tipo/id | 0 notificaciones del expediente de inventario antes de esta ampliación | Bandeja global del ERP |

## Alias y equivalencias

| Términos o identificadores | Estado: confirmada / candidata / distinta / no resuelta | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| `ProductInventoryAuditCase.source_trace.transfers[]` ↔ `PointTransferLine.id` | confirmada | El balance persiste IDs internos de la misma fuente | No |
| `PointTransferLine.id` ↔ `RutaCargaChecklistLinea.point_transfer_line_id` | confirmada | FK explícita y protegida | No |
| `RutaCargaChecklistLinea` ↔ `DiscrepanciaLogistica.linea_carga_id` | confirmada | FK explícita | No |
| Sucursal Point ↔ sucursal ERP | confirmada cuando `PointBranch.erp_branch_id` existe | La custodia usa FK canónica, no nombre | No |
| Nombre de actor Point ↔ usuario ERP | no resuelta | Texto de Point no es una identidad autenticada | Sí; no autoasignar |
| Usuario con perfil en sucursal ↔ jefatura de sucursal | candidata | El perfil prueba ubicación, no responsabilidad | Sí; no autoasignar |
| Precio actual Point ↔ valor histórico de agosto | distinta | El precio actual puede cambiar y no prueba el valor del mes | Sí, antes de priorización monetaria |

## Decisión de diseño

Extender `ProductInventoryAuditCase` y reutilizar la bandeja de notificaciones. Crear un servicio regenerable de investigación que relacione IDs ya almacenados. No crear otra tabla de movimientos, otra descarga Point ni otra bandeja de pendientes. La discrepancia de Logística continúa siendo el expediente operativo de ruta; la auditoría mensual solo la referencia en su proyección.

Consultas o procedimiento reproducible:

```bash
python3 manage.py inventario_fuentes_datos --term auditoria --term inventario
python3 manage.py inventario_fuentes_datos --term discrepancia --term logistica
python3 manage.py shell -c '<consultas ORM de conteo por mes y estado>'
```

Las consultas de producción fueron agregadas, acotadas a agosto de 2026 y no extrajeron archivos, contraseñas ni contenidos personales.

Riesgos y pendientes: no existe aún un responsable explícito por sucursal para esta auditoría; por eso la primera etapa usa jefaturas canónicas por área y deja sin persona cualquier empate. El valor monetario histórico queda fuera hasta identificar una fuente temporal confiable.
