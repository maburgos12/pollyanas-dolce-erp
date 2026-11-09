# Ficha de fuentes para edición y retiro de solicitudes departamentales

Fecha y ambientes consultados · 2026-11-09 · PostgreSQL local aislado y producción en solo lectura.

## Necesidad y unidad de análisis

Corregir la captura de un folio departamental y sus artículos antes de que Compras lo atienda. La autoría y el estado se evalúan para el folio completo.

## Fuentes candidatas

| Concepto | Modelo y tabla | Quién lo crea y actualiza | Identificador y ámbito | Evidencia en producción | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Folio departamental | `SolicitudCompraDepartamental` · `compras_solicitudcompradepartamental` | Captura web y confirmación asistida. Compras actualiza el seguimiento. | PK, folio SCD único, área, solicitante y comprador | 43 registros | Mis solicitudes, Bandeja de Compras, decisiones DG, agente de compras |
| Artículo solicitado | `ItemCompraDepartamental` · `compras_itemcompradepartamental` | Captura con el folio. Compras gestiona cotización, compra y recepción. | PK y FK al folio | 52 registros | Cotizaciones, órdenes, recepción, presupuesto y destinos documentales |
| Autor y revisión | `AuditLog` · `core_auditlog` | Servicio de creación y vista de edición | Modelo, PK y acción. La creación registra a quien hizo la captura. | 17 eventos de creación en esta bitácora | Auditoría del ERP y control de autoría |
| Transición del folio | `EventoCompraDepartamental` · `compras_eventocompradepartamental` | Vistas y servicios del módulo | PK, folio, actor, tipo y motivo | 180 eventos | Detalle y trazabilidad |

## Alias y equivalencias

- «Solicitud departamental» y «folio SCD» son el mismo registro. La solicitud de insumos `SolicitudCompra` tiene otro contrato.
- El solicitante coincide con el autor en la captura web. La confirmación asistida permite representar a otra persona y deja al autor en la bitácora. Para folios legados sin evento de creación se usa el solicitante registrado.

## Decisión

Reutilizar los modelos, áreas y permisos de captura existentes. La edición conserva el folio y registra la versión anterior. El borrador eliminado se oculta del listado y el folio permanece reservado. Al retirar una enviada, el estado y el motivo siguen disponibles. Cotizaciones, órdenes, pagos, recepciones, compromisos y evidencia documental bloquean ambas acciones.

La búsqueda de metadatos se ejecutó con `inventario_fuentes_datos` usando los nombres de los cuatro modelos y los términos «cotizacion» y «bitacora». Los conteos se obtuvieron con consultas agregadas en una transacción de solo lectura. El inventario por términos solo se usó para localizar candidatos.
