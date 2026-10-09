# Ficha de fuentes — cierre documental y retorno administrativo

Fecha: 8oct2026. PostgreSQL16 aislado `erp_auditor_cierre_retornos_documentales`,
puerto55602; evidencia de producción read-only, sin HTTP, posterior a PR1525.

## Necesidad y unidad de análisis

Cerrar documentalmente un producto/sucursal/mes cuando cada rubro y ambos
cortes coinciden con su historial Point íntegro. Una recepción menor que el
envío explica un retorno administrativo, no acredita transporte físico.

## Fuentes candidatas

| Concepto | Modelo / tabla | Escritor | Identidad y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Envío y recepción | PointTransferLine / pos_bridge_transfer_lines | Sincronizador Point existente | ID interno, folio/detalle, FK_articulo original, producto/insumo y sucursal | Diagnóstico posterior1525; filas originales ya guardadas, no recapturar | Traza, agente, cierre individual |
| Efecto de stock | Historial canónico Point | Ingreso/captura oficial existente | FK_Movimiento, importación y par/mes | Cadenas, cortes, unknown0 y cada rubro comprobados por servicio | Traza y cierre |
| Cierre individual | ProductInventoryDocumentaryEvent | ProductDocumentaryCloseService | Par/mes, huella, actor, evento inmutable |149 cierres previos; diagnóstico identifica advertencias adicionales, no autoriza todos83 candidatos | Pantalla de expediente |
| Custodia física | Expediente y logística existentes | Operadores existentes | Transferencia/recepción/carga | Pendientes conservados | Agente y revisión humana; no requisito físico de septiembre |

## Alias y equivalencias

Recibida terminal y retorno enviado−recibido: regla DG8oct ya autorizada y
publicada en la traza. Reutilizarla; no crear equivalencia por nombre/SKU.
PRODUCT_RESOLVED_BY_NAME/SKU, recibido>enviado, fila cancelada/no vigente,
dominio distinto y FK contradictoria siguen bloqueados.

## Decisión de diseño

Extender exclusivamente el cierre documental existente: cargar en una sola
consulta los originales referenciados por advertencia; admitir retorno sólo
recibido/fechado dentro del mes, 0≤recibido<enviado, dominio producto y FK
original exacta. En origen, exigir también su ID en entradas por transferencia.
Conservar advertencia, custodia y fuente originales en la evidencia del cierre.
No bajar controles de cortes, cadena, unknown, remanente, cantidades ni identidad.
No cambiar guard mensual, maestros, flags de Point ni aprobaciones del expediente.

Inventario de fuentes: `inventario_fuentes_datos --term transferencia --term
retorno --term cierre`, PostgreSQL16 configurado; candidatos léxicos solamente.
Diagnóstico producción: `diagnostico-separacion-documental-custodia-20261008.json`,
REPEATABLE READ READ ONLY/HTTP0.83 candidatos hipotéticos incluyen coincidencias
secundarias que esta corrección NO acepta. Pruebas ejercitan cada veto y segunda
ejecución sin eventos nuevos; aceptación requiere deploy y pantalla autenticada.
