# Ficha de fuentes — identidad y vigencia del inventario Point

Fecha y ambiente: 2026-11-05; PostgreSQL de producción (solo lectura), captura
Point de 07:09 America/Mazatlan; pruebas en PostgreSQL 16 aislado.

## Necesidad y unidad de análisis

Una existencia representa un insumo identificado por código Point, ubicación
(ALMACEN o CEDIS) y ciclo de captura. Mostrar el catálogo completo y rechazar
identidades ambiguas sin inventar existencias ni modificar documentos históricos.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Maestro | maestros.Insumo | Catálogo ERP / importadores Point | PK y código Point; activo | 555 activos, 75 inactivos; 188 activos sin código | Compras, recetas, inventario, reportes |
| Alias | maestros.InsumoAlias | Resoluciones de catálogo | Nombre normalizado → FK | No se crean equivalencias nuevas | Identificación de insumos |
| Captura | pos_bridge.PointInsumoInventorySnapshot | CanonicalInsumoInventoryCaptureService | Ciclo + ubicación + insumo | Ciclo 130676; 354/356 capturas, incluidas dos de inactivos por ubicación | CanonicalPointInventoryService |
| Estado de ciclo | pos_bridge.PointSyncJob | Tarea Celery existente | PK, estado, parámetros y resumen | Último terminado 05/11 04:21; renglones rechazados aun con SUCCESS | Vigencia y diagnóstico |
| Unidad | maestros.UnidadMedida | Maestro existente | Código, dimensión y factor | Point incluye M sin equivalencia validada; no convertir longitud en piezas | Conversión compartida |
| Documento fuente | Existencias Point | Point | ID interno, código, ubicación, unidad | 371 renglones por ubicación en lectura directa protegida | Captura canónica existente |

## Alias y equivalencias

| Registros | Estado | Evidencia y caso contrario | Revisión |
| --- | --- | --- | --- |
| Caja CH #433 / Rollos Térmicos #595 | Distintos | Códigos 075/RTERMICOS, nombres y cantidades distintos; #433 conserva nombre_normalizado incorrecto | Separación autorizada por Mauricio 05/11; reparar solo normalización y conservar referencias |
| Panes 01PLOTUSR, 08DCDBB, 01PPB01 | Código exacto activo identificado | Registros inactivos anteriores en kg eclipsan activos en unidad | Priorizar único activo; no reactivar históricos |
| Pan Dawn Chocolate Rosca #508/#560 | Ambiguo | Código duplicado con diferencia de mayúsculas | Bloquear selección arbitraria; no fusionar datos |
| Sorbato 0099/0102 | No resuelto | Mismo nombre, KG/U; maestro usa 0102 con kg | No cambiar código ni unidad sin confirmar presentación |
| Etiquetas 311026 | Distintos con código duplicado en Point | IDs/nombres diferentes; en CEDIS ambos tienen cero | Rechazar aunque saldos coincidan; corregir origen con evidencia |
| 188 sin código | No resuelto | 93 internos, 75 materia prima, 20 empaque | Clasificar; parecido nominal no autoriza vincular ni desactivar |

## Decisión de diseño

Reutilizar maestros, capturas, ciclo, conversión y cachés existentes. No crear
tablas, importadores ni equivalencias. Separar grupos nominales cuando contienen
códigos Point diferentes; los miembros sin código no se asignan a uno de varios
códigos. Mantener el costo dentro de cada grupo separado. La lectura valida el
código de la captura contra el maestro y muestra el rechazo del ciclo.

El resolver compartido selecciona solo una identidad activa por código
normalizado. No cae a nombre si ese código existe exclusivamente inactivo o es
ambiguo. Las capturas además exigen coincidencia de código y rechazan artículos
distintos con la misma identidad aun con saldo igual. Se conservan todas las FK
de recetas, compras, movimientos e históricos.

La pantalla Existencias consulta el catálogo sin límite; la búsqueda filtra
después de recuperar todos los insumos y declara el alcance del contador. Los
estados de vigencia no acreditan exactitud física ni ausencia de negativos.

## Consultas y validación

`manage.py inventario_fuentes_datos --term insumo --term inventario --term point`
ejecutado con PostgreSQL local conectado, todas las migraciones aplicadas y
check limpio. Los 92 candidatos léxicos no se usan como prueba de equivalencia.
Producción consultada en transacciones READ ONLY. Point leído bajo
point_account_session_lock sin persistir cantidades. Evidencia y desglose por
insumo en expediente local de la tarea, fuera del repositorio.

Pruebas de separación de miembros/costos, persistencia de nombre normalizado al
guardar parcialmente, preferencia activa, ambigüedad, captura cruzada, códigos
duplicados con saldo cero, rechazo explícito y búsqueda después del registro 200.

## Riesgos, despliegue y reversión

Cambian consumidores compartidos de catálogo/recetas/costos: validar sus pruebas
y CI completo. No hay migraciones ni cambios de configuración. Publicar por PR,
deploy_web_safe, luego verificar UI autenticada y captura real. Reparación de
#433 con respaldo previo, comparación de valores esperados y AuditLog. No
reescribir snapshots históricos. Una nueva captura reemplaza el ciclo operativo.
Rollback de código mediante revert y despliegue oficial; la corrección del nombre
normalizado es independiente y no requiere deshacer referencias ni existencias.
Los negativos se entregan a revisión humana, no se regularizan automáticamente.


## Verificación de extracción completa por categoría

La captura visual posterior al despliegue reportó SUCCESS pero obtuvo solo
278/333 insumos utilizables frente a 352/352 en la lectura oficial por categoría.
Faltaban categorías completas y había filas repetidas entre selecciones. La
captura canónica reutiliza `PointHttpSessionClient.get_insumo_categories` y
`get_branch_insumos` (`/Stock/GetInsumosPA`, parámetros `almacen` y `categoria`),
ya existentes en la consulta oficial de stock. Cada categoría se consulta por
ID y cada respuesta se analiza antes de seguir. No se cambia la captura visual
de productos/costos ni se introduce otra fuente o tabla. Una cantidad ausente,
inválida o no finita rechaza la captura; cero explícito se conserva como dato.
