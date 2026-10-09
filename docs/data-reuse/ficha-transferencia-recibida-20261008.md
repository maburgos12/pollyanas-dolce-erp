# Ficha de fuentes — estado terminal de transferencias

Fecha y ambiente: 8oct2026, producción leída sin escrituras; PostgreSQL16 local aislado para regresiones.

## Necesidad y unidad de análisis

Una línea de transferencia por folio/detalle y dominio producto. Recibida es el último estado operacional Point; no exigir una finalización adicional ni condicionar retorno administrativo a esa bandera. Autorización DG explícita del hilo.

## Fuentes candidatas

|Concepto|Modelo/tabla|Writer|Identificador/ámbito|Evidencia|Consumidores|
|---|---|---|---|---|---|
|Transferencia|PointTransferLine/pos_bridge_transfer_lines|Extractor y movement sync existentes|folio/detalle, FK_articulo/isInsumo, origen/destino|2157:26 originales recibido true/finalizado false;39577/549605 producto1037 enviado2 recibido0|BranchInventoryTraceabilityService, DailyInventoryBreakService, InventoryAuditAgent|
|Movimientos originales|Stock/GetHistorial y GetHeader/GetDetalle|Point, consulta read-only protegida|producto1037/sucursal8, FK1686766/1687006|Oct1 salida2 y retorno2; cabeceras iguales al original39577, detalle retorno sólo Snickers Mini2|Prueba documental, no nueva importación|
|Investigación guardada|ProductInventoryAuditCase|InventoryAuditAgent|mes/producto/sucursal|3911 y2157 avisos de finalización indebidos|Vista de caso y runtime nativo|

## Alias y equivalencias

Recibida/isRecibido confirmado por DG y frontend Transfer/tab_solicitudes: estado mostrado RECIBIDO; FinalizarTransferencia comentada. isFinalizado es dato legado conservado, no requisito terminal. No nueva equivalencia de productos ni unión temporal de documentos. Ejemplo39577 es octubre y no aporta movimientos a septiembre.

## Decisión de diseño

Reutilizar modelos/raws e identificadores existentes. Cambiar sólo criterio lector de recepción/retorno y pendientes; conservar cantidades, fechas, cancelaciones, integridad, guard mensual, permisos, fuentes y esquema. Mantener fallback legado finalizado para fecha faltante, añadiendo recepción fechada como evidencia equivalente y siempre incidencia explícita. No actualizar banderas ni importar/sincronizar Point.

Procedimiento: inventario_fuentes_datos --term transferencia --term recepción sobre PostgreSQL16; consulta acotada de originales26 y39577. Fuente→lectores→auditor/runtime/pantalla; pruebas de recibido completo sin finalizado, recibido0/retorno2, falta recepción/fecha, cruce mensual, carga0 validada, idempotencia.

Riesgos: recepción es documental, no físico; no sustituir diferencias reales por cerrado. Las investigaciones guardadas requieren refresco oficial tras deploy. La publicación y aceptación no cierran septiembre automáticamente.

Refresco reutilizado: `investigate_inventory_audit_cases --month 2026-09 --case-id ID --no-notify`, sin `--refresh-point-history`. El filtro ahora limita también la investigación conservada; los consumidores mensuales sin filtro mantienen el comportamiento anterior. Segunda ejecución debe dar updated0/notifications0; no cambia cantidades ni estado de cierre.

Aceptación1514 detectó tres reescrituras sin cambio JSON ni fingerprint (2131/2149/2237); transacción revirtió el refresco. Comparar el JSON realmente persistido, no diferencias de tipos tuple/list antes de serializar. No modificar huellas, datos ni contratos de entrada. Regresión conserva detección de un valor verdaderamente distinto. Reparación de aceptación dentro del mismo objetivo, antes de cerrar la tarea.
