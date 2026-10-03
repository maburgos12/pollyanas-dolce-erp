# Ficha de fuentes — carga no realizada en auditoría de inventario

Fecha y ambiente consultado: 2026-10-02; PostgreSQL de producción, lectura acotada.

## Necesidad y unidad de análisis

Distinguir una transferencia administrativa sin carga de un producto cargado cuyo
retorno físico requiere evidencia. Unidad: línea Point vinculada por FK a la
línea de carga y su revisión existente. No modificar cantidades ni aprobar cierres.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Transferencia | PointTransferLine / pos_bridge_pointtransferline | Sincronización Point existente | PK, folio y detalle Point | Consulta limitada a un expediente y sus FK | Materializador y agente auditor |
| Carga real | RutaCargaChecklistLinea / logistica_rutacargachecklistlinea | Logística | FK point_transfer_line; ruta y parada | Cantidad cargada cero y estado FALTANTE | Logística y agente auditor |
| Validación de carga | DiscrepanciaLogistica / logistica_discrepancialogistica | Revisión operativa existente | FK linea_carga, origen CARGA_CEDIS | VALIDADA_REAL, revisor y fecha presentes | Logística; ahora hechos del auditor |
| Investigación | ProductInventoryAuditCase / reportes_productinventoryauditcase | Materializador y agente existentes | Mes, sucursal y producto | Diferencia cero, investigación anteriormente pedía retorno físico | Expediente y reportes |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Transferencia y carga | Confirmada por FK existente | Sin correspondencia por nombre o cantidad | Ninguna equivalencia nueva |
| Retorno Point y devolución física | Distinta | Retorno administrativo no prueba transporte | Conservar aprobación independiente del expediente |

## Decisión de diseño

Reutilizar las tablas y el servicio existentes. Leer la revisión de carga más
reciente aunque esté cerrada, sin incorporarla como incidencia abierta. Reconocer
no-carga sólo si carga y revisión coinciden en cero y cantidad enviada, la revisión
está validada con revisor/fecha y Point tiene recepción cero finalizada. Si la
evidencia contradice esos requisitos, conservar la investigación pendiente.

La diferencia comercial de la fuente sigue conservada como hecho; no se elimina
ni altera el movimiento. La explicación del expediente y su aprobación separada
siguen siendo necesarias; esta proyección no crea eventos ni sustituye al actor.

Procedimiento: `inventario_fuentes_datos --term discrepancia --term transferencia`
y consultas de solo lectura por PK del expediente y discrepancia relacionada.
El comando produce candidatos léxicos; la relación se comprobó por FK. No se
consultó Point ni se creó importación, tabla o captura adicional.

Riesgos: no extender la conclusión a cargas parciales, revisiones abiertas,
registros sin firma temporal o datos contradictorios. Las demás transferencias
mantienen la exigencia de acreditar custodia física.
