# Conocimiento del ERP: mapa técnico y proyección del agente

## Arquitectura y límites

El mapa para desarrollo y el conocimiento del agente son dos productos separados. `orquestacion.services.erp_system_map.build_system_map()` describe declaraciones Django, rutas resueltas y referencias explícitas de `api.ai_gateway_services.TOOLS`. No consulta filas, permisos de un usuario, secretos, valores por defecto ni variables de entorno. No sirve como autorización ni se carga en el prompt del agente.

`api.ai_gateway_business_reads.knowledge(actor)` genera la proyección pequeña del runtime: conceptos, unidad de análisis, relaciones de negocio, sucursales y capacidades que el actor puede consultar en ese momento. La versión depende de contratos, descriptores y contenidos de las fuentes del código; no depende de Git HEAD ni de rutas del Mac. No contiene cifras actuales: éstas se vuelven a leer al invocar una capacidad. Producción no necesita MCP, el grafo de desarrollo, Graphify ni Neo4j.

El runtime ofrece `knowledge.discover` como función nativa de Responses. El modelo selecciona entre claves permitidas; el servidor valida la selección y ofrece los contratos de las capacidades descubiertas en la siguiente ronda. Esto no crea un router por palabras clave, ni ejecuta consultas de módulos no ofrecidos, ni concede permisos. La pregunta de negocio se conserva, incluyendo seguimientos sobre otra sucursal.

## Mapa reproducible para desarrollo

Con PostgreSQL 16 configurado y conectividad verificada conforme a `AGENTS.md`:

```bash
python manage.py build_erp_system_map > /ruta/fuera/del/checkout/erp-system-map.json
```

La salida es JSON por stdout. No se añade un snapshot enorme al repositorio. Incluye aplicaciones aunque no exista una capacidad preparada para el agente, modelos y columnas, unicidad, FK declaradas, relaciones M2M con modelo/tabla intermedia y FK del intermedio, rutas recursivas con namespace, handlers, controles de acceso y fuentes de esquema del registro. Los esquemas efectivos del puente estricto se distinguen del esquema legacy del registro. Las referencias propias tienen ruta relativa y SHA-256; bibliotecas externas o fuentes no resolubles se marcan explícitamente sin publicar rutas locales.

`source_commit` conserva contexto de la revisión. `source_tree_hash` resume, en orden, rutas y hashes del inventario de archivos de código tracked; `content_hash` resume los metadatos excluyendo `source_commit`. Un cambio de código cambia la huella aunque HEAD sea el mismo; cambiar HEAD por un commit que sólo guarde una salida generada no invalida código intacto. Se excluyen snapshots JSON, secretos de `.env`, directorios privados/generados y enlaces simbólicos; archivos tracked ausentes quedan declarados en cobertura. Los archivos de código nuevos aparecen en el inventario tracked después de incorporarse a Git; las referencias de metadatos ya pueden apuntar a sus contenidos durante el desarrollo.

FK, M2M, rutas y referencias de `TOOLS` son declaraciones verificables. `call_relationships` separa referencias declaradas, llamadas estáticas, inferidas y no resueltas. Este constructor no hace otro escáner AST: las últimas categorías están vacías y `static_scan=not_performed`. Tampoco declara que un módulo esté listo para el runtime a partir de sus modelos o rutas. Los conteos de archivos y metadatos no prueban cobertura funcional completa del ERP.

La revisión de desarrollo cuenta además con el grafo MCP del baseline `e7536db357a894d07cf871051fbcddbf1f03fabe`: 2,496 archivos tracked de código seleccionados, aproximadamente 33 MB de entrada segura; 2,485 indexados y 11 faltantes explícitos. Se excluyeron previews privados y secretos. El proyecto MCP es `Users-mauricioburgos-.codex-task-artifacts-ai-erp-system-knowledge-20261012-graph-source`; `graph-coverage.json` conserva las exclusiones y faltantes. Es evidencia estática de ese baseline, no prueba semántica ni dependencia del runtime. Debe regenerarse para revisar cambios posteriores.

## Capacidades incorporadas al puente de lectura

| Capacidad | Unidad y fuente | Límites que se conservan |
| --- | --- | --- |
| `erp.get_sales_summary` | Rango de fechas y sucursales; `ventas.services.sales_read_service` | Selección por cobertura; razones y días disponibles; conteo de tickets no disponible en la proyección agregada, nunca sustituido por cero. Sin fuente, importes/cantidades quedan null. |
| `erp.get_inventory_low_stock` | Producto y sucursal Point vinculada al ERP; snapshot más reciente por pareja | Alertas bajo mínimo, límite y truncación; captura, última captura de la fuente y antigüedad separadas de `as_of`. Sin alertas no equivale a ausencia de inventario; sin capturas no equivale a cero. |
| `erp.get_current_input_cost` | Insumo canónico, unidad base y costo de compra | Costo decimal exacto, moneda, fecha y proveedor. Se excluyen fechas futuras; costo faltante queda null. Opciones ambiguas o búsqueda truncada requieren precisión, incluso si sólo queda una opción canónica. No es costo de receta. |
| `erp.get_purchase_requests` | Solicitud clásica e insumo | Filtros y cantidad acotada, folio/estado, truncación y aviso de fuente. No incluye solicitudes departamentales o especiales. |
| `erp.get_purchase_orders` | Orden clásica y proveedor; solicitud opcional | Filtros y cantidad acotada, folio/estado/fechas/monto estimado y truncación. No incluye órdenes departamentales o especiales. |

Se reutilizan los servicios y las fuentes existentes. El helper de búsqueda de insumos ofrece metadatos de truncación sólo mediante una opción confiable interna; los callers legacy conservan su retorno de lista y comportamiento. No hay equivalencias nuevas, tablas maestras ni capturas duplicadas. Los lectores de activos y mantenimiento, incidentes, seguimiento y solicitudes especiales existentes conservan sus contratos y controles. Conocer otras aplicaciones del ERP no habilita sus consultas automáticamente.

## Convención para ampliar capacidades

Cada incorporación requiere una ficha de fuentes, un servicio existente o justificación de la extensión, una clave registrada, serializer estricto, unidad de análisis, relaciones declaradas, fuentes y límites verificables, DTO acotado y auditoría. Hay que probar lectura nueva, alcance exacto, ambigüedad, truncación, ausencia de datos, fallos de auditoría y revocación en ejecución e historial. Un identificador, esquema o texto aportado por el modelo no sustituye esa revisión.

El actor se refresca del servidor; gate de lectura, permisos efectivos, candados y alcance se comprueban antes de emitir conocimiento, antes de leer y después de la lectura. Los cambios de acceso bloquean la evidencia. La prueba guardada de cada turno de negocio vincula versión de conocimiento y huella de acceso; se exige al proyectar respuestas e historial. Las pruebas de activos antiguas conservan compatibilidad. Resultados idénticos pueden reutilizar evidencia dentro del mismo turno después de revalidar acceso; un seguimiento vuelve a consultar las fuentes.

No se cambian identidad piloto, modelo, presupuesto, flags, límites de herramientas, permisos, variables de entorno, migraciones ni datos operativos. El esquema ofrecido contiene sólo las claves autorizadas del actor; los argumentos desconocidos o el intento de invocar una capacidad no descubierta fallan y se auditan.

## Presentación y verificación

La interfaz usa tablas, escape de texto, fechas y fuentes existentes. Descubrimiento muestra nombres; las consultas muestran hechos y límites, incluso con prosa natural. No se imprime JSON, SQL, contratos, hashes ni identificadores técnicos de selección. La elección de una opción se precisa por nombre y unidad en la conversación. No se añaden botones de ejecución. `chat.html` y `erp-sw.js` versionan juntos el cambio visible.

Pruebas locales: `orquestacion.tests_agent_business_reads`, `orquestacion.tests_erp_system_map`, `orquestacion/tests_agent_results_ui.js` y `orquestacion/tests_agent_voice_ui.js`. Las pruebas de mapa usan `SimpleTestCase` y no permiten consultas de filas; incluyen rutas recursivas, FK/M2M, referencias de esquema y ausencia de defaults. Las de UI cubren null/moneda exacta, límites, captura antigua, XSS y conservación del controlador de conversación/voz/evidencias. La prueba de Compras usa ACL VIEW explícita, actor sin DG y sucursal asignada, con candado o revocación durante el proveedor y supresión posterior de contenido/evidencia en historial.

Este documento describe la implementación y evidencia local. La publicación y el resultado visible en producción requieren PR/checks, merge, deploy oficial y verificación autenticada del flujo, por separado. No declara completado ese despliegue.

## Reversión

Para retirar estos lectores, detener primero el gate `AI_AGENT_READ_ENABLED` mediante el procedimiento de configuración autorizado, o conservar el validador de pruebas de negocio que impide exponer evidencia histórica fuera de acceso. No revertir sólo el puente/proyección dejando al runtime legacy presentar resultados guardados sin la comprobación de versión y permisos. El mapa de desarrollo puede dejar de generarse sin afectar producción. No borrar conversaciones, auditoría ni datos operativos como parte de una reversión.

Véase [ficha de fuentes](../data-reuse/ficha-fuentes-agente-conocimiento.md) y los protocolos [Agent Core READ](AI_ERP_AGENT_CORE_READ.md), [activación del piloto](AI_ERP_PILOT_ACTIVATION.md) y [solicitudes especiales](AI_ERP_SPECIAL_PURCHASES.md).
