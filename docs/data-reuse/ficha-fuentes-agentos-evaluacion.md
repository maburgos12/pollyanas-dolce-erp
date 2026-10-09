# Ficha de fuentes — evaluación AgentOS

Fecha y ambiente: 2026-10-09 UTC; PostgreSQL local aislado y consultas públicas reales a servicios existentes de Maya.

## Necesidad y unidad de análisis

Equipos autorizados por usuario/sucursal y contexto de mantenimiento; producto/variante/sucursal para disponibilidad y precio; sesión/turno/tool call para evidencia conversacional. No se captura nuevamente inventario, precios ni activos productivos.

## Fuentes candidatas

| Concepto | Modelo / tabla | Crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Equipo | activos.Activo / activos_activo | ERP activos | PK, código, sucursal, vigencia | Dos registros sintéticos LAB-A-3 y LAB-B-3 en la base aislada | Gateway, pasaporte, mantenimiento, laboratorio |
| Mantenimiento | activos.PlanMantenimiento, OrdenMantenimiento, fallas.ReporteFalla | Flujos existentes del ERP | PK, activo_ref, sucursal, alcance del usuario | Herramienta devuelve historial incompleto y cero planes registrados en fixtures; no equivale a ausencia de servicio | UI operativa y Gateway |
| Producto / variante | Product / ProductVariant de Maya, códigos Point | Sincronización comercial existente | UUID, slug, variant ID, product_code; producto activo | Catálogo público real: 27 productos; Pay de Queso mediano, código Point 0002 | Tienda, Maya, herramientas públicas |
| Sucursal pública | Branch y BranchService de Maya | Catálogo comercial existente | UUID, nombre canónico y ciudad | 9 activas en la consulta; Guamúchil no localizada en esa fuente | Maya y atención pública |
| Stock | CustomerReadTools → adapter.check_inventory → ERP/Point | Point y adaptadores existentes | Código producto/tamaño/sucursal; frescura verificada | Consulta real de Pay de Queso mediano en Matriz: disponible; timestamp conservado en la evidencia | Pickup, tienda y Maya |
| Precio | CustomerReadTools → _point_sale_price → ERP pos-bridge | Point | Código Point 0002, identidad, moneda y frescura | HTTP 503 en prueba directa; valor desconocido, jamás cero ni precio inventado | Maya comercial |
| Conversación | PostgresDb, esquema local agentos_evaluation | Agno runtime de laboratorio | user_id firmado, session_id, run/tool call | Reanudación entre instancias y aislamiento probado | AgentOS, Agent UI, evaluación |

## Alias y equivalencias

| Término | Estado | Evidencia / caso contrario | Revisión |
| --- | --- | --- | --- |
| «La otra», «la de Matriz» | Referencias conversacionales a IDs previamente consultados | Cambió de activo 1 a 2; no creó ni fusionó entidades | Sólo laboratorio |
| «El mismo» tras cambio de sucursal | Producto y tamaño conservados | Tool args mantienen Pay de Queso / Mediano | No modifica maestros |
| Guamúchil ↔ sucursal pública de Maya | No resuelta | No aparece entre las 9 activas de esa fuente | Verificar fuente/identidad; no agregar por suposición |
| No registrado ↔ cero / cerrado / sin existencias | Distinta | Historial incompleto, precio 503 y sucursal ausente conservan incertidumbre | Nunca convertirlo a cero |

## Decisión

REUTILIZAR Gateway READ y servicios públicos de Maya. Crear únicamente almacenamiento de sesiones y ledger en el laboratorio; no crear una segunda tabla maestra ni equivalencias entre registros productivos. Los dos activos son fixtures sintéticos descartables, identificados como laboratorio.

Inventario ejecutado con PostgreSQL configurado: `manage.py inventario_fuentes_datos --term activo --term mantenimiento`; 58 candidatos léxicos, 15 mostrados. Las coincidencias léxicas no prueban duplicidad ni equivalencia. Las consultas posteriores se limitaron a esas herramientas/servicios, catálogos públicos y fixtures.

Pendientes: catálogo real ERP con roles productivos; sucursal Guamúchil en la fuente comercial; disponibilidad de precio Point; reemplazo del puente SSH por credencial de servicio acotada antes de producción.
