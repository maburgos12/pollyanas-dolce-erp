# Ficha de fuentes — conteos históricos de cierre por sucursal

Fecha y ambiente consultado: 2026-11-06; PostgreSQL local aislado
`erp_conteos_cierre_oct26` en `127.0.0.1:55480`, sin consultar producción ni el
Excel real.

## Necesidad y unidad de análisis

Importar evidencia histórica ya capturada de cuatro conteos físicos de sucursal.
Cada `ConteoSucursal` representa una sucursal y fecha; cada
`LineaConteoSucursal` identifica un producto o insumo activo del catálogo y cada
`LecturaConteoSucursal` conserva la cantidad decimal exacta de una celda fuente.
Es un histórico de evidencia, no una existencia operativa, conciliación ni ajuste.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Conteo físico de sucursal | `inventario.ConteoSucursal` y líneas, lecturas y eventos relacionados | App operativa y servicio `inventario.services_conteos`; este importador solo agrega evidencia histórica validada | `request_id` único; sucursal + fecha + estado; UUID determinista por digest SHA-256 declarada, fecha y código de sucursal | El inventario de fuentes confirmó las cuatro tablas. La base local limpia contenía 0 conteos; las pruebas crean cuatro conteos y ocho líneas/lecturas. | Captura `/app/conteos/`, revisión central, impresión, conciliación nocturna y auditoría documental |
| Catálogo de producto Point | `pos_bridge.PointProduct` / `pos_bridge_products` | Sincronización de catálogo Point | `catalogo_id` + `sku` + nombre exactos; `external_id` se comprueba cuando viene | La base local limpia contenía 0 productos activos codificados; fixtures PostgreSQL prueban identidad exacta y SKU duplicado | Conteos, catálogo, ventas y referencias Point |
| Catálogo de insumo | `maestros.Insumo` / `maestros_insumo` | Catálogo maestro y sincronización Point | `catalogo_id` + `codigo_point` + nombre exactos | La base local limpia contenía 0 insumos activos codificados; fixtures PostgreSQL prueban identidad exacta | Conteos, inventario, recetas y compras |
| Conteo mensual CEDIS | `inventario.ConteoFisicoMensual` y `LineaConteoFisico` | Flujo mensual CEDIS | Periodo mensual único | El inventario de fuentes confirmó su relación con existencias, movimientos y cierre; la base local limpia contenía 0 registros | Inventario CEDIS y ajustes locales |

## Alias y equivalencias

| Términos o identificadores | Estado: confirmada / candidata / distinta / no resuelta | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| `catalogo_id` + `sku` + `catalogo_nombre` para producto | confirmada | Los tres valores deben corresponder al mismo `PointProduct` activo. Un SKU aislado no prueba identidad; el SKU `0160` tiene colisión conocida. | Ninguna equivalencia automática |
| `catalogo_id` + `codigo_point` + `catalogo_nombre` para insumo | confirmada | Los tres valores deben corresponder al mismo `Insumo` activo. No se resuelve por nombre ni código aislado. | Ninguna equivalencia automática |
| `ConteoSucursal` y `ConteoFisicoMensual` | distinta | El primero conserva evidencia de sucursal independiente de stock; el segundo es un cierre CEDIS capaz de producir ajustes. | No reutilizar `ConteoFisicoMensual` para esta carga |
| Nombre de hoja y nombre de catálogo | distinta | Hoja/celda son procedencia documental; nunca identifican artículos. | Conservar ambos como evidencia sin homologarlos |

## Decisión de diseño

Reutilizar sin extender el esquema los modelos actuales `ConteoSucursal`,
`LineaConteoSucursal`, `LecturaConteoSucursal` y `EventoConteoSucursal`. El
comando congela código, nombre, unidad y fuente de unidad mediante el resolvedor
vigente, deja el conteo en `ENVIADO` y no genera inicio, hora de conteo,
aceptación, conciliación ni ajuste Point. La identidad documental usa la digest
SHA-256 **declarada** del workbook, los metadatos de su verificación externa, UUID
determinista y hash canónico del payload para reintentos idempotentes y conflictos
explícitos. El comando valida el formato y conserva esa declaración, pero no abre
el workbook ni afirma haber comprobado su hash. No se crea una segunda tabla
maestra ni se usa `ConteoFisicoMensual`.

Consultas o procedimiento reproducible: `python3 manage.py
inventario_fuentes_datos --term conteo`; `python3 manage.py
inventario_fuentes_datos --term inventario`; consultas ORM acotadas de conteos y
catálogos en la base local; `python3 manage.py test
inventario.tests_importar_conteos_historicos --keepdb`. El inventario léxico solo
identifica candidatos y no demuestra equivalencia semántica.

Riesgos y pendientes: no se inspeccionó el Excel real ni producción. El payload
corregido debe excluir celdas en blanco y aportar la identidad compuesta exacta;
los errores documentales conocidos `0017`, `0018` y `0019` deben corregirse antes
de ejecutar. El modo predeterminado es `DRY-RUN`; aplicar requiere `--aplicar` y
un actor con permiso vigente para coordinar conteos. La fecha debe ser histórica,
anterior al día local de ejecución. La entrada `-` lee JSON desde stdin para no
copiar el payload al servidor. La digest debe comprobarse fuera del comando (por
ejemplo con `shasum -a 256`) y registrar método y responsable en
`fuente.verificacion_externa`.
