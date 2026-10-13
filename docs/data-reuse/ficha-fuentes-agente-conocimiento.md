# Ficha de fuentes — conocimiento transversal y lectores del agente

Fecha y ambiente consultado: 12 octubre 2026, código main `e7536db3` y PostgreSQL de producción mediante consultas acotadas de presencia READ ONLY. Evidencia reunida antes de implementar, sin extraer valores personales ni modificar registros. La presencia registrada es una observación de ese momento, no validación de vigencia o despliegue de estos cambios.

## Necesidad y unidad de análisis

Descubrir servicios y contratos existentes según permisos. Cada capacidad es un contrato, no una nueva captura. Los datos se leen nuevamente en las fuentes existentes: rango de ventas, producto/sucursal/captura Point, insumo/costo/fecha, solicitud u orden clásica. Compras departamentales y especiales conservan su servicio independiente.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Ventas | `ventas.VentaAutoritativaPoint` / `ventas_autoritativas_point` | Integración y cierres Point existentes | branch + sale_date + product_code únicos | Tabla presente y al menos una fila | `ventas.services.sales_read_service`, reportes, Gateway |
| Existencias Point | `pos_bridge.PointInventorySnapshot` / `pos_bridge_inventory_snapshots` | Sincronización Point existente | Producto/sucursal/fecha; último snapshot por pareja | Tabla presente y filas | Gateway, inventario/reportes |
| Costo de compra | `maestros.CostoInsumo` / `maestros_costoinsumo` | Importación/captura canónica de compras existente | Insumo + fecha; source_hash único | Tabla presente y filas | Recetas/costos/Gateway |
| Solicitud clásica | `compras.SolicitudCompra` / `compras_solicitudcompra` | Flujo clásico de compras | ID y folio único | Tabla presente, sin filas | Gateway clásico |
| Orden clásica | `compras.OrdenCompra` / `compras_ordencompra` | Flujo clásico de compras | ID y folio único | Tabla presente, sin filas | Gateway clásico |
| Solicitud departamental | `compras.SolicitudCompraDepartamental` | Flujo departamental existente | ID y folio único; área/solicitante | Tabla presente y filas | Agent Core purchase, Compras departamentales |
| Orden departamental | `compras.OrdenCompraDepartamental` | Flujo departamental existente | ID y folio único | Tabla presente y filas | Compras departamentales |
| Capacidades | `api.ai_gateway_services.TOOLS`, serializers y catálogos efectivos | Código versionado | Key + schema; permiso, candados y gate del servidor | Inspección de código, no tabla nueva | Descubrimiento/runtime/mapa técnico |
| Conversación y evidencia | `orquestacion.ChatConversation`, `ChatMessage`, `ChatToolCall`, `ChatToolResult` | Agent Core existente | Propietario, UUID, turno y herramienta | Reutilización del contrato existente | Historial/propuestas/recibos |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Compra clásica/departamental/especial | Distinta | Modelos y flujos diferentes; tablas clásicas vacías no significan ausencia de compras | No se fusionan |
| Fecha de consulta/fecha de snapshot | Distinta | `as_of` frente a `captured_at`; antigüedad de captura informada | No presentar snapshot como capturado ahora |
| Insumo/costo de receta | Distinta | `CostoInsumo` es costo de compra por unidad base | No inferir costo terminado |
| Nombre/código/Point de sucursal | Identificación por relación existente; candidato ambiguo cuando hay varias coincidencias exactas | Resolución exacta mediante la FK existente; un nombre desconocido nunca significa toda la empresa | Sin nuevas equivalencias operativas |
| Insumos duplicados/aliases | Reutilización canónica existente | `canonical_insumo` y `canonical_member_ids`; búsqueda truncada nunca prueba una identidad única | Sin merge ni actualización de maestros |
| Relación de código/permiso de ejecución | Distinta | Una arista estática permite navegar código, no acceder a datos | Autorización fresca del servidor |

## Decisión de diseño

Reutilizar servicios, modelos, registro, auditoría y controles existentes. Extender internamente los lectores con alcance exacto y proyecciones estrictas sin alterar capturas ni contratos públicos legacy. Generar el mapa desde metadatos y el conocimiento desde los contratos permitidos, sin bases duplicadas ni maestros nuevos. Las cinco lecturas alcanzan ventas, alertas de inventario, costos de compra y compras clásicas; las solicitudes departamentales/especiales siguen en su flujo existente.

Procedimiento reproducible con PostgreSQL 16 y conexión verificada:

```bash
python manage.py inventario_fuentes_datos --term venta --term inventario --term costo --term compra --limit 20 --presence
python manage.py inventario_fuentes_datos --term VentaAutoritativaPoint --term PointSalesDailyFact --term PointInventorySnapshot --term CostoInsumo --term SolicitudCompra --term OrdenCompra --limit 30 --presence
```

La primera consulta produjo 113 candidatos léxicos, 20 mostrados; no es un inventario completo. La focalizada mostró 8/8 candidatos. `PointSalesDailyFact` es una etiqueta del servicio, no prueba de un modelo con ese nombre exacto; el servicio también selecciona `FactVentaDiaria` y facts Point por cobertura. Los artefactos `business-source-inventory.json` y `reader-source-inventory.json` conservan metadatos y presencia bajo `/Users/mauricioburgos/.codex/task-artifacts/ai-erp-system-knowledge-20261012/`; `FICHA_FUENTES_LECTORES.md` es la ficha inicial de evidencia de esta tarea.

Riesgos y pendientes: presencia no prueba completitud temporal, capturas recientes ni importes conciliados. La proyección agregada de ventas no conserva conteo verificable de tickets; el cero legado no se presenta como conteo verdadero. Capturas de inventario y fechas de costo no se sustituyen por la fecha de consulta. El grafo técnico no demuestra cobertura funcional de todo el ERP. La verificación autenticada y el despliegue del cambio se realizan posteriormente por el flujo oficial; esta ficha no los da por realizados.
