# Cobertura de acciones sin perder contexto

Inventario inicial: 381 formularios POST en 129 templates; 57 llamadas `fetch()` en 26 templates; 492 usos de Django Messages. Estos conteos son candidatos de auditoría, no equivalen a acciones migradas.

| Módulo / pantalla | Acciones | Async | Fallback con ancla | Pruebas | Estado |
| --- | --- | --- | --- | --- | --- |
| Global / `base.html` | Toast, bloqueo del submitter, doble envío, modal opt-in | Sí | N/A | `core.tests_actions` | Cubierto etapa 1 |
| Logística / detalle de ruta / Revisión administrativa | Autorizar, Rechazar, Marcar corregida | Sí, reemplazo de una fila | Sí, `#revision-entrega-<id>` | `LogisticaRevisionEntregaTests` + Chromium local | Cubierto etapa 1 |
| RRHH | Aprobar, autorizar, rechazar, cancelar, guardar | No inventariado por pantalla | No | No | Pendiente etapa 2 |
| Seguimiento | Resolver, aprobar, entregar a revisión | No inventariado por pantalla | No | No | Pendiente etapa 2 |
| Compras / Recepciones | Cerrar y aplicar inventario desde recepción pendiente o con diferencias | Sí, toast y redirect seguro | Sí, `#recepcion-<id>` | `ComprasOrdenesRecepcionesFiltersTests` + `ERPActionContractTests` | Parcial: solo cierre/aplicación |
| Compras | Solicitudes, órdenes y demás acciones de recepciones | No inventariado por pantalla | No | No | Pendiente etapa 2 |
| Inventario / Ajustes | Aprobar y aplicar ajuste pendiente | Sí, toast y redirect seguro | Sí, `#ajuste-<id>` | `InventarioAjustesApprovalTests` + `ERPActionContractTests` | Parcial: solo aprobación/aplicación |
| Inventario, recetas, bonos, mantenimiento y activos | Demás guardados y cambios de estado | No inventariado por pantalla | No | No | Pendiente etapa 3 |
| Resto del ERP | Acciones mutantes restantes | No inventariado por pantalla | No | No | Pendiente etapa 4 |

No se declarará cobertura total hasta que cada pantalla candidata tenga una fila con evidencia de implementación o una excepción justificada.
