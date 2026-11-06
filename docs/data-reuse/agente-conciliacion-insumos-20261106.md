# Ficha de fuentes — agente de conciliación de insumos

Fecha/ambiente: 06/11/2026; PostgreSQL producción en READ ONLY y worktree desde 947f5e6e. Amplía la ficha `auditoria-compras-produccion-20261105.md` sin cambiar las fuentes operativas.

## Necesidad y unidad de análisis

Expediente persistente por línea de compra, documento de transferencia o causa de cobertura/consumo, identificado por mes y clave estable. Investigación, conciliación documental y solicitud interna de aclaración; no segundo catálogo ni libro de inventario.

## Fuentes candidatas

| Concepto | Modelo / tabla | Creador / actualizador | Identificador / ámbito | Evidencia consultada | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Compra importada | maestros.CostoInsumo | Importador Point existente | PK + source_hash + purchase_id | 17 pendientes de octubre leídos íntegros y cotejados con movimiento | Costos, kardex y auditor documental |
| Representación | inventario.MovimientoInventario | Importadores existentes | hash derivado + costo FK + identidad + ubicación | 17 movimientos con trazabilidad; 11 misma unidad GLI/Gfn, 5 alias U, 1 CJA/pza | Inventario y finanzas, solo lectura |
| Unidades | POINT_UNIT_ALIASES y conversión dimensional existente | Código de integración vigente | Unidad textual, dimensión y unidad base | U=unidad ya existe; igualdad GLI/GLI y Gfn/Gfn no convierte envases | Reporte y agente; CJA/pza sigue sin acreditar |
| Transferencia | pos_bridge.PointTransferLine | Sincronizador Point | documento + detalle + versión vigente | Fuente existente con envío/recepción/cancelación | Conciliación por representación y corte |
| Cobertura y consumo | InsumoAuditSnapshot | Auditor documental diario | periodo + fingerprint | Revisión versionada; cobertura física no acreditada | Reporte y agente; conserva fecha de fuente |
| Ciclos fuente | PointSyncJob | Sincronizadores | tipo + periodo + estado | No hay jobs de purchase; importador recibe archivos | Diagnóstico de cobertura, nunca ausencia de compras |
| Solicitud y respuesta | SeguimientoItem / Comentario / Evidencia | Agente crea; responsable responde; DG revisa | solicitud vinculada al expediente; referencia externa | Modelos y permisos existentes inspeccionados; sin señales de envío al crear ORM | Mi Seguimiento, panel DG, archivos y aprobaciones |
| Responsable | Empleado.usuario_erp y jefatura por departamento | RRHH | persona activa + cuenta activa + jefatura única | Resolver existente InventoryAuditAgent._resolve_assignee; Jorge está inactivo, no asignar por nombre | Agente y Seguimiento |

## Alias y equivalencias

| Términos | Estado | Evidencia y límite | Revisión |
| --- | --- | --- | --- |
| U / unidad / pza | Existente | POINT_UNIT_ALIASES + conversor dimensional ya soportado; no presentación nueva | No modifica maestros |
| GLI→GLI, Gfn→Gfn | Misma unidad | Cantidad y unidad idénticas, sin convertir a litros | No certifica contenido de envase |
| CJA→pza cubrebocas | No resuelta | 10 cajas representadas como 10 piezas; factor de empaque no acreditado | Solicitar factura/ficha y aprobación antes de corregir |
| CAJA CH como empaque o venta | Uso posible aclarado por Mauricio | No inferir error por poco uso o por venta separada | No reclasificar automáticamente |

## Decisión de diseño

Extender el reporte existente y crear solamente expediente/evento de investigación: el snapshot cambia con el tiempo y no tiene identidad persistente por documento ni solicitud. ProductInventoryAuditCase exige producto vendido + sucursal y no representa una compra de insumo. Reutilizar Seguimiento para solicitar, responder, adjuntar y aprobar; no inventar un capturador ni un canal de comunicaciones. Reutilizar asignación por jefatura activa del agente existente. Conservar sin asignar si el organigrama no permite resolver una persona única.

No invocar run_month del agente de productos porque tiene efectos de notificación. No enviar emails, WhatsApp ni notificaciones. No usar API de IA. Los cierres automáticos son de conciliación documental; un expediente conciliado con solicitud se entrega EN_REVISION, sin suplantar la aprobación humana. Leer respuestas no acredita su veracidad ni autoriza equivalencias.

## Procedimiento y validación

`inventario_fuentes_datos --term compra --term insumo --term seguimiento --term aclaracion --term consumo --presence --details --limit 60`, PostgreSQL producción, PGOPTIONS READ ONLY. Metadatos y consultas dirigidas en `/Users/mauricioburgos/Documents/Codex/2026-11-06/agente-conciliacion-insumos/`. Las consultas verificaron campos reales y resolvieron errores iniciales de nombre de campo antes de usar resultados.

La conciliación se programa cada dos horas a los minutos 15, de 07:15 a 19:15 America/Mazatlan; revisa meses actual/anterior y periodos anteriores con casos abiertos. Fuentes de compras/transferencias leídas directamente en transacción consistente; cobertura/consumo conserva fecha de la revisión diaria. No presume actualización instantánea de Point ni ejecuta importaciones operativas. Historial por huella e idempotencia; lock de periodo para evitar solicitudes duplicadas.

## Riesgos y pendientes físicos

La evidencia documental no acredita entrega ni consumo físico. Respuestas con anexos sin equivalencia estructurada aprobada permanecen por verificar. Capturas sin movimientos observados se explican como cobertura, no pérdidas. La falta de compras importadas requiere fuente original, no movimientos fabricados. Se registran solicitudes concretas después de revisar fuentes disponibles.
