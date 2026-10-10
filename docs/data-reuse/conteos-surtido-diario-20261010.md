# Ficha de fuentes — conteos físicos de sucursal

Fecha y ambiente consultado: 10 de octubre de 2026; producción de solo lectura y PostgreSQL 16 local aislado.

## Necesidad y unidad de análisis

Un renglón representa un artículo físico identificable por código Point, sucursal y unidad oficial. Separar la propuesta diaria de productos e insumos del catálogo mensual más amplio. No mostrar cantidades esperadas durante captura. No se maneja es una incidencia con cantidad vacía, no stock cero.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Artículos | PointProduct / pos_bridge_products; Insumo / maestros_insumo | Sincronización Point y maestros existentes | SKU o codigo_point, PK y unidad | Catálogo leído en producción; PMH028 insumo 635 activo, unidad pza | Preparación y búsqueda de conteos |
| Surtido reciente | PointTransferLine / pos_bridge_transfer_lines | Sincronización de transferencias Point | Código, destino, recepción, snapshot vigente, cancelación | PMH028 recibido recientemente en las diez sucursales consultadas | Propuesta diaria |
| Conversión a rebanadas | PointConversionLine / pos_bridge_conversion_lines | Sincronización de conversiones Point | Código, sucursal y hora del movimiento | Fuente existente para entradas por conversión | Propuesta diaria de rebanadas aun sin venta |
| Ventas recientes | sold_point_skus_for_range | Servicio canónico de ventas | Código y sucursal, fecha de venta | Lectura de tres días por sucursal, sin exportar cantidades | Propuesta diaria |
| Saldo de referencia | PointInventorySnapshot y PointInsumoInventorySnapshot | Ciclo de inventario Point | Código, sucursal, ciclo, captura | Corte de octubre 9; un saldo antiguo puede conservar artículos de temporada | Catálogo mensual y referencia del revisor |
| Producto e ingrediente | Receta y LineaReceta; maestros_insumo | Recetas y matching existentes | FK insumo_id y receta_id, código, cantidad y unidad | Receta 371/0124 consume 1 pza de insumo 635/PMH028; las cuatro variantes también | Selección física, sin modificar la receta |
| Clasificación | Receta.modo_costeo; PointProductCategory; inventory_consumption_filter | Catálogo y reglas existentes | Código exacto | Reventa, servicios y complementos ya clasificados | Evitar saturar la lista diaria |
| Captura | ConteoSucursal, LineaConteoSucursal, LecturaConteoSucursal, EventoConteoSucursal | Servicio transaccional existente | Conteo, ronda, línea, versión y request_id | Envío existente acepta incidencia sin cantidad; diferencia queda vacía | Colaborador, revisor, historial y Excel |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| PMH028 y 0124 / variantes | Distintos, relación confirmada | LineaReceta apunta a insumo 635, 1 pza. Mauricio confirmó que reciben y cuentan el insumo; la presentación se prepara al vender | Confirmada por Mauricio en este hilo; no fusionar registros |
| 50181900 y PMH028 | Distintos | Hay otra preparación y un insumo activo con código 50181900; no se usan como alias de PMH028 | Ninguna equivalencia aplicada |
| Pay de queso base y 0317/0318 | Distintos | Mauricio indicó contar pay base, sin combinación comercial de toppings | Excluir variantes de la propuesta diaria; no convertir cantidades ni ventas |
| No se maneja y cero | Distintos | Cantidad nula con incidencia frente a cantidad física 0 | Mantener historia y no producir diferencia numérica para incidencia sin cantidad |

## Decisión de diseño

Reutilizar modelos, permisos, unidades, operaciones idempotentes, borradores y envío existentes. Sin nuevas tablas, migraciones, ajustes de inventario o cambios de credenciales. La app propone la lista diaria combinando productos e insumos: recepciones y conversiones de siete días y ventas de tres días, sin usar un saldo positivo antiguo como único criterio. Son ventanas de propuesta, no vencimientos ni bloqueo del catálogo; buscar y desmarcar siguen disponibles. Reventa, servicios, toppings, bebidas clasificadas y variantes comerciales 0317/0318 quedan fuera de la propuesta diaria. Crema queda disponible mediante búsqueda, sin exigirla todos los días. La búsqueda de sucursal excluye presentaciones que consumen PMH028 para evitar que se cuenten dos veces.

La opción mensual mantiene el criterio amplio existente. No se introduce programación automática ni un cierre financiero nuevo. No se modifica la comparación temporal de Point ni se ajustan saldos al aceptar.

Consultas o procedimiento reproducible: manage.py inventario_fuentes_datos --term conteo --term insumo --term receta --presence --limit 12 en PostgreSQL 16; producción bajo SET TRANSACTION READ ONLY, Insumo por nombre muerto/código PMH028 y Receta por nombre muerto con líneas e insumos relacionados. Evidencia acotada del catálogo y movimientos de octubre 4–10 conservada fuera del repositorio.

Riesgos y pendientes: la actividad reciente propone surtido pero no prueba existencias físicas. Un artículo sin movimiento reciente se agrega mediante búsqueda. La receta prueba consumo esperado, no ejecución del descuento en Point. No se cambian permisos de suplentes ni registros operativos. Validar navegación, búsqueda, borradores, envío, consola y producción antes del cierre.

Validación del catálogo real: Point conserva velas y pirotecnia bajo categorías de proveedor (Alegría, Granmark), tarjetas en Otros postres e incluso recetas FABRICADO; no basta modo_costeo. La propuesta diaria excluye también las familias explícitas de accesorios, velas, tarjetas, pirotecnia y Coca-cola por categoría o nombre, sin reclasificar maestros. Se revisaron las categorías de la propuesta de todas las sucursales; también quedan fuera servicios, extras, recetarios, empaques y proveedores de artículos para fiestas. Litro crema sigue disponible al buscar, sin exigirlo a diario. El catálogo mensual y la búsqueda mantienen esos artículos disponibles.

Incidencia de fuente pendiente: la réplica activa de Point tiene dos nombres distintos para SKU 0318 (Pay de Queso 3 Sabores C y Vela Luminosa Blister Num 0-9). No son equivalentes ni se fusionan maestros. Ambos quedan fuera de la propuesta diaria; revisar la identidad canónica en Point antes de usarlos en el cierre mensual.

La búsqueda de preparación conserva los artículos ya seleccionados en el formulario, pero oculta los que no coinciden mientras se busca. Siguen incluidos en el envío; al limpiar la búsqueda vuelven a mostrarse. Así no se pierde la selección ni se obliga a desplazar toda la lista para encontrar una coincidencia.
