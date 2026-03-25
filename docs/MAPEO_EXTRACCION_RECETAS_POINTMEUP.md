# Mapeo de Extracción de Recetas desde Pointmeup

## Objetivo

Definir con precisión cómo se encuentra una receta dentro de Pointmeup y en qué orden debe extraerse para materializarla en el ERP sin depender de inferencias de IA.

Este documento fija:

- la ruta funcional dentro de Point
- los endpoints autenticados ya identificados en este repo
- el orden correcto de extracción
- las reglas de decisión para `receta base`, `presentación derivada`, `componente directo` e `histórico`

## Fuente de verdad

La fuente de verdad de la receta es `Pointmeup`.

El ERP no debe inventar recetas. El agente no debe definir cantidades, rendimientos ni BOM por sí solo.

El flujo correcto es:

```text
Pointmeup
-> extracción cruda autenticada
-> staging
-> parser canónico
-> matching ERP
-> reglas de negocio
-> materialización aprobada
```

## Hallazgo clave

En Pointmeup la receta no vive en un solo lugar. Hoy hay dos rutas operativas:

1. `Configuración -> Productos -> Editar -> Siguiente -> Siguiente -> Receta`
2. `Catálogos -> Insumos -> Editar -> Receta`

Eso significa que una receta puede aparecer como:

- BOM directo del producto final
- preparación interna asociada a un artículo/insumo
- presentación derivada que no trae BOM propio de producto

## Ruta 1: Receta de producto final

### Flujo funcional en Point

```text
Configuración
-> Productos
-> seleccionar producto
-> Editar
-> Siguiente
-> Siguiente
-> Receta
```

### Endpoints autenticados ya identificados

Extraídos de [point_http_client.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/pos_bridge/services/point_http_client.py):

- `POST /Account/SignIn_click`
- `POST /Account/get_workSpaces`
- `POST /Account/get_acctok`
- `GET /Catalogos/get_productos`
- `GET /Catalogos/get_producto_byID`
- `GET /Catalogos/getBomsByProducts`

### Orden correcto de extracción

1. Autenticar sesión Point.
2. Seleccionar cuenta/workspace.
3. Descargar catálogo de productos con `get_productos`.
4. Por cada producto seleccionado:
   - leer detalle con `get_producto_byID`
   - leer BOM con `getBomsByProducts`
5. Si el BOM existe:
   - guardar raw
   - normalizar
   - materializar como `Receta` tipo producto final
6. Si el BOM viene vacío:
   - no crear receta vacía
   - mandar el producto a auditoría de faltantes

### Campos clave esperados

Del catálogo/listado:

- `PK_Producto`
- `Codigo`
- `Nombre`
- `Familia`
- `Categoria`
- `hasReceta`

Del BOM:

- `Codigo_Articulo` o `CodigoInsumo`
- `Articulo` o `Nombre`
- `Cantidad`
- `Unidad_corto` o `Unidad`

### Estado de implementación actual

Esto ya está operativo en [product_recipe_sync_service.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/pos_bridge/services/product_recipe_sync_service.py).

## Ruta 2: Receta corroborada desde Catálogos / Insumos

### Flujo funcional en Point

```text
Catálogos
-> Insumos
-> buscar artículo interno
-> Editar
-> Receta
```

Esta ruta se usa cuando el producto final no trae BOM en `Configuración -> Productos`, pero internamente la preparación sí existe como artículo/insumo.

### Endpoints autenticados ya identificados

También desde [point_http_client.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/pos_bridge/services/point_http_client.py):

- `GET /Catalogos/get_articulos`
- `GET /Catalogos/ArticuloGetbyid`

### Orden correcto de extracción

1. Detectar producto sin BOM propio.
2. Generar términos de búsqueda a partir de:
   - código de producto
   - nombre de producto
   - nombre normalizado
   - tokens de nombre sin ruido comercial
3. Buscar candidatos internos en `get_articulos`.
4. Para cada candidato razonable:
   - abrir `ArticuloGetbyid`
   - revisar si trae `BOM`
5. Si un solo candidato trae BOM fuerte:
   - clasificar como `CORROBORATED_FROM_INSUMO_CATALOG`
6. Si hay varios candidatos:
   - `POSSIBLE_MATCH_REQUIRES_REVIEW`
7. Si hay candidato pero sin BOM:
   - `INTERNAL_CANDIDATE_WITHOUT_BOM`
8. Si no hay evidencia:
   - `MISSING_IN_POINT`

### Estado de implementación actual

Esto ya está operativo en [recipe_gap_audit_service.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/pos_bridge/services/recipe_gap_audit_service.py).

## Ruta 3: Presentaciones derivadas

### Casos típicos

- rebanadas de pastel
- rebanadas de pay
- SKUs con empaque/etiqueta directa pero sin receta base propia

### Regla de negocio

No deben tratarse como receta base nueva.

Se clasifican como:

- `DERIVED_PRESENTATION`

Y deben resolverse como:

- receta padre
- rendimiento por presentación
- componentes directos de salida

### Estado actual

La inferencia de derivadas ya existe en [recipe_gap_audit_service.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/pos_bridge/services/recipe_gap_audit_service.py) y la persistencia en [derived_presentation_sync_service.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/pos_bridge/services/derived_presentation_sync_service.py).

## Orden maestro de extracción recomendado

Este debe ser el orden oficial del sistema:

### Fase A. Extracción cruda

1. Login Point
2. Selección de workspace
3. Catálogo de productos
4. Detalle de producto
5. BOM de producto
6. Si BOM vacío:
   - búsqueda en catálogo de artículos
   - detalle de artículo
   - BOM del artículo

### Fase B. Clasificación

1. `PRODUCT_BOM_PRESENT`
2. `DERIVED_PRESENTATION`
3. `CORROBORATED_FROM_INSUMO_CATALOG`
4. `POSSIBLE_MATCH_REQUIRES_REVIEW`
5. `INTERNAL_CANDIDATE_WITHOUT_BOM`
6. `MISSING_IN_POINT`

### Fase C. Materialización

Solo se materializa directo al ERP cuando el caso queda en:

- `PRODUCT_BOM_PRESENT`
- `DERIVED_PRESENTATION`
- `CORROBORATED_FROM_INSUMO_CATALOG` con decisión aprobada

No materializar automático cuando el caso quede en:

- `POSSIBLE_MATCH_REQUIRES_REVIEW`
- `INTERNAL_CANDIDATE_WITHOUT_BOM`
- `MISSING_IN_POINT`

## Arquitectura objetivo recomendada

Para que el agente “sepa extraer recetas”, la capa correcta no es LLM-first, sino esta:

### 1. Extracción

- `PointRecipeExtractionRun`
- `PointRecipeRawHeader`
- `PointRecipeRawLine`

### 2. Canonicalización

- `PointRecipeCanonical`
- `PointRecipeCanonicalLine`

### 3. Matching

- `PointRecipeMatchDecision`

### 4. Materialización

- `RecipeMaterializationDecision`

### 5. Agent layer

El agente solo debe:

- lanzar el run
- explicar qué encontró
- sugerir clasificación
- preparar colas de revisión

El agente no debe:

- inventar cantidades
- definir rendimiento
- crear BOM sin evidencia Point

## Regla financiera y operativa para Dirección General

Desde punto de vista ejecutivo, la extracción de recetas tiene esta prioridad:

1. proteger costo y merma
2. proteger producción y transferencias
3. proteger cuadre de inventario
4. proteger reporting comercial y forecast

Eso exige que la extracción sea:

- determinística
- auditable
- idempotente
- separada entre actual e histórico

## Riesgos identificados

1. `hasReceta=true` no garantiza que el BOM venga completo.
2. Un producto puede no tener BOM propio pero sí existir como preparación en `Catálogos -> Insumos`.
3. Un SKU derivado puede parecer “falta de receta” cuando en realidad es `presentación derivada`.
4. Un artículo de temporada puede existir solo para histórico y no debe ensuciar la operación actual.
5. Si se materializa desde un match dudoso, se contamina costeo, producción y dashboard.

## Checklist de implementación segura

- Confirmar fuente Point para cada tipo de receta.
- Guardar raw payload antes de cualquier transformación.
- No crear recetas vacías.
- No materializar casos ambiguos.
- Mantener clasificación explícita entre base, derivada, directa e histórico.
- Registrar `source_hash` e idempotencia por extracción.
- Separar dashboards de:
  - pendientes operativos
  - históricos/temporada

## Criterio de aceptación

La extracción de recetas quedará correctamente entendida cuando el sistema pueda responder, para cada SKU Point:

1. ¿La receta vive en `Productos`?
2. ¿La receta vive en `Catálogos / Insumos`?
3. ¿Es una presentación derivada?
4. ¿Es un histórico/temporada?
5. ¿Debe materializarse ya o quedar en revisión?

## Siguiente paso recomendado

Implementar la siguiente capa formal del modelo:

- `PointRecipeExtractionRun`
- `PointRecipeRawHeader`
- `PointRecipeRawLine`
- `PointRecipeCanonical`
- `PointRecipeMatchDecision`

Con eso, el agente dejará de ser “el que trata de entender recetas” y pasará a ser “el orquestador que explica y audita una extracción ya bien estructurada”.
