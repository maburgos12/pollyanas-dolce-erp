# Ficha de fuentes — guardado seguro de mermas

Fecha y ambiente: 2026-10-08, PostgreSQL local aislado y lectura acotada en producción.

## Necesidad y unidad de análisis
Una captura de merma ya existente debe conservar su identidad al reintentarse. La identidad pertenece al intento de negocio, no a la cantidad, ticket, producto o momento de la petición. Dos capturas legítimas con los mismos datos deben seguir siendo distintas.

## Fuentes candidatas
| Concepto | Modelo / tabla | Creador y actualización | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Merma de producto terminado | mermas.MermaRegistro / mermas_mermaregistro | mermas.views.crear_registro; envío y recepción CEDIS | PK y folio únicos; sucursal y actor | 194 registros en producción | Captura app y ERP, productos, evidencia, CEDIS y dashboard |
| Merma de insumo | mermas.MermaInsumo / mermas_mermainsumo | operacion.views.mermas_insumos_crear_api; servicios de aprobación | PK; sucursal, código Point y actor | 478 registros en producción | App sucursal, aprobaciones, historial, eventos y órdenes de ajuste |
| Evidencia de producto | mermas.MermaEvidencia | Captura y recepción | PK, registro y tipo de foto | 429 registros en producción | Detalle y validación CEDIS |
| Patrón de idempotencia | inventario.ConteoSucursal / inventario_conteosucursal | inventario.services_conteos.preparar_conteo | request_id UUID único y payload_hash | 0 conteos en lectura de producción de auditoría; pruebas existentes de replay y conflicto | App conteos y coordinación ERP |

## Alias y equivalencias
| Términos | Estado | Evidencia / caso contrario | Revisión |
| --- | --- | --- | --- |
| Reintento con mismo UUID, actor, sucursal, datos y fotos | Confirmada por protocolo de captura | Mismo envío; la respuesta puede haberse perdido | No fusiona filas históricas |
| Misma cantidad/producto/ticket con otro UUID | Distinta | Puede ser otra merma legítima | No deduplicar por contenido |
| Merma de producto y merma de insumo | Distinta | Flujos, responsables y evidencia diferentes | No unificar sus tablas |
| Conteo físico y merma | Distinta | El conteo registra observación, no un desperdicio | Reutilizar el patrón, no su registro |

## Decisión de diseño y contrato
Extender los dos registros existentes con request_id nullable único y payload_hash. Las filas históricas conservan clave NULL y hash vacío. No hay tabla, maestro, captura secundaria ni reconstrucción de movimientos.

La interfaz genera/conserva un UUID por captura y lo reutiliza con su borrador y fotos. En una transacción PostgreSQL, un bloqueo advisory por dominio/UUID protege la creación; la restricción única permanece como garantía de unicidad. Una petición concurrente recibe 409 y conserva la captura. Una respuesta perdida puede reintentarse: se devuelve el mismo ID sin crear evidencia, notificaciones ni consultar Point otra vez. Un UUID confirmado con datos diferentes devuelve 409; el hash incorpora actor y sucursal para impedir replay de otra persona.

Producto AJAX con request_id recibe JSON (201 al crear, 200 al recuperar) con id, folio, request_id y redirect_url local al detalle. Sólo esa confirmación permite retirar el borrador/navegar. Insumos conserva su respuesta de creación y añade request_id. Las peticiones antiguas sin UUID mantienen su contrato; no garantizan idempotencia. El bump de ambas cachés PWA y del recurso JS actualiza los clientes.

Los folios de producto se asignan e insertan bajo un bloqueo transaccional PostgreSQL común, incluido el primer registro del día. Se conserva el formato existente. Los borradores usan IndexedDB en el mismo navegador/dispositivo; producto se separa por usuario e insumo por usuario/sucursal. Los campos se bloquean durante el envío para no descartar cambios posteriores a la foto de la captura enviada.

## Procedimiento y límites
`manage.py inventario_fuentes_datos --term merma --term captura --term request_id` se ejecutó con PostgreSQL 16 configurado. El resultado sólo es descubrimiento léxico. La lectura de producción utilizó `SET TRANSACTION READ ONLY` y conteos de los tres modelos anteriores y migraciones mermas (0001–0006 presentes). No se extrajeron credenciales ni detalles de capturas. El entorno local se creó vacío; las reproducciones usan fixtures sintéticos.

No se alteran reglas de stock, ajustes Point, responsables, permisos ni capturas anteriores. El formulario muestra los mensajes específicos por campo y enfoca el primero, conservando el borrador. La disponibilidad de IndexedDB y su cuota son límites del dispositivo: el formulario informa si no puede conservar el borrador y pide mantenerlo abierto.

## Referencia Point de conteos (hallazgo 4)
Fuente adicional: `pos_bridge.PointBranch` y snapshots Point de inventario. El extractor utiliza el ID del selector Point (`inventory_extractor.py`); `point_branch_canonical_sort_key` prioriza `external_id.isdigit()` y la captura canónica de insumos exige un único registro numérico activo. La lectura de producción de la auditoría encontró un registro numérico y otro textual activos, ambos ya enlazados por `erp_branch_id`, en las 11 sucursales consultadas (incluidos Matriz `1`/`Matriz` y Sinaloa `14`/`Sinaloa de Leyva`). No se introduce una equivalencia nueva ni se cambia ese enlace: se reutiliza la identidad oficial existente al leer.

`referencia_conteo` mantiene el único registro activo cuando sólo existe uno. Si hay varios, acepta exclusivamente un único ID numérico activo; dos IDs numéricos activos o varios alias sin ID numérico siguen siendo ambiguos. Conserva la verificación de ciclo anterior al inicio, código crudo, cantidad, unidad y evidencia; una fuente faltante permanece faltante. No mezcla snapshots de distintos ciclos, no toma el más reciente arbitrariamente, no ajusta existencias ni escribe maestros. Los consumidores son revisión y exportación de conteos físicos; pruebas cubren alias textual más reciente, dos identidades numéricas y un ID numérico inactivo.
