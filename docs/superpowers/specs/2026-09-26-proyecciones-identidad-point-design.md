# Diseño: identidad exacta de Point en Pronósticos y Proyecciones

## Objetivo

Corregir el selector y los motores de Pronósticos y Proyecciones para que cada
producto conserve su identidad, nombre y categoría vigentes en Point. La
selección de un artículo nunca debe incluir otro producto por compartir SKU.

La implementación se preparará y validará localmente. Quedan fuera de esta fase
el despliegue, los cambios de catálogo en Point y la modificación de pronósticos
históricos guardados. Mauricio revisará el resultado visible antes de autorizar
cualquier envío a producción.

## Diagnóstico confirmado

- `PointProduct.id` identifica de manera inequívoca cada producto replicado de
  Point; el SKU no es único.
- El catálogo activo contiene 169 grupos de SKU repetidos y 30 grupos cruzan
  categorías distintas.
- El selector actual transmite SKU. Por eso artículos como Bollo Lotus y Glow 2
  son indistinguibles para el formulario.
- En la pantalla actual hay 141 filas, pero solo 129 SKU distintos; 12 productos
  aparecen sin venta propia reciente por compartir código.
- El selector mezcla la categoría exacta de Point con agrupaciones de interfaz
  como Accesorios y Bebidas.
- Los motores nuevos ya producen categorías correctas cuando reciben el producto
  correcto: las 107 relaciones Point-receta con venta reciente no presentan
  discrepancias de nombre o categoría.
- Las dos últimas sincronizaciones `recipes` fallaron dentro de una transacción.
  Su diagnóstico es independiente y no autoriza ejecutar sincronizaciones en
  producción.

## Alternativas consideradas

### A. Identidad por `point_product_id` — seleccionada

El formulario transmite el identificador del producto de Point. Los servicios
filtran ese producto exacto y resuelven su receta mediante la relación ya
observada en ventas. Es estable ante SKU duplicado y cambios de nombre.

### B. Identidad por SKU y nombre

Reduce algunas colisiones, pero se rompe cuando Point renombra un producto y
mantiene lógica de identidad textual en varios consumidores. Se descarta.

### C. Corregir únicamente los SKU conflictivos actuales

Resolvería los ejemplos conocidos, pero una nueva colisión volvería a introducir
el defecto. Se descarta por no corregir la causa estructural.

## Diseño funcional

### 1. Catálogo del selector

- Cada opción tendrá como valor `PointProduct.id`.
- El SKU seguirá visible como dato informativo, pero no participará en la
  identidad de la selección.
- Solo se mostrarán productos con historial utilizable por el motor
  correspondiente. Un producto sin relación pronosticable no se ofrecerá como
  si pudiera generar resultados.
- La etiqueta de categoría conservará exactamente la categoría vigente de
  `PointProduct.category`.
- Las agrupaciones operativas podrán conservarse como navegación visual, pero
  se expondrán explícitamente como `grupo_operativo`; nunca sustituirán
  `categoria_point`.

### 2. Contrato de formulario y servicios

- La vista recibirá `point_product_ids` como una colección de enteros.
- Se validará que cada ID exista, esté activo y pertenezca al catálogo permitido.
- `calcular_pronostico` filtrará `PointSalesDailyProductFact.point_product_id`.
- `calcular_proyeccion_operativa` resolverá recetas desde las relaciones del
  producto exacto seleccionado. No consultará otros productos con el mismo SKU.
- La receta podrá complementar la proyección, pero no cambiar el nombre o la
  categoría que Point asigna al producto cuando la salida sea por producto.
- Una selección inválida conservará la pantalla, mostrará un mensaje claro y no
  generará un resultado parcial engañoso.

No se mantendrá un fallback silencioso por SKU en el flujo nuevo. Si se requiere
compatibilidad con una solicitud antigua, se rechazará de forma explícita o se
aislará en un adaptador probado que nunca amplíe una selección ambigua.

### 3. Categorías y grupos

La salida distinguirá estos conceptos:

- `categoria_point`: categoría vigente y literal de Point.
- `grupo_operativo`: agrupación opcional para facilitar navegación, por ejemplo
  Accesorios o Bebidas.
- `familia_receta`: clasificación productiva de la receta.

Las tablas y el detalle mostrarán como categoría principal `categoria_point`.
Los grupos se usarán únicamente como encabezados o filtros cuando aporten valor.
La única receta con venta reciente y familia vacía, Pay de Plátano Rebanada,
usará la categoría como fallback visible sin alterar la fuente.

### 4. Orden y exportación

- El orden preferido existente se conservará para categorías conocidas.
- Toda categoría realmente presente y no incluida en el orden controlado se
  agregará al final de manera determinista.
- Las hojas de Excel recorrerán las categorías del resultado, no una lista que
  pueda omitir categorías nuevas.
- `Cake Topper` tendrá cobertura aunque no forme parte del pronóstico productivo
  actual.
- Las variantes ortográficas detectadas, como `PLÁSTICOS` y `Plásticos`, no se
  fusionarán en datos del ERP. Se reportarán para corregirse en Point; el código
  solo garantizará orden y cobertura sin perder filas.

### 5. Históricos

- Los ocho pronósticos guardados permanecerán inmutables.
- El detalle seguirá indicando que es una fotografía del cálculo generado.
- Cuando una categoría guardada difiera del catálogo actual, podrá mostrarse una
  nota de contexto, sin reescribir el JSON histórico.
- La regeneración será una operación nueva y explícita si posteriormente se
  autoriza; nunca una migración silenciosa.

### 6. Sincronización de recetas

Se realizará diagnóstico de solo lectura de los jobs fallidos, sus trazas y el
flujo transaccional. Si la causa requiere código, se propondrá un cambio acotado
y probado dentro de esta rama o se separará en otra tarea si afecta un contrato
distinto. No se reintentará el job ni se escribirán datos en producción sin una
autorización posterior.

## Interfaz y accesibilidad

El cambio conservará la estructura visual de la pantalla. Se mejorarán las
etiquetas para distinguir categoría Point y grupo, sin rediseñar el módulo.
Checkboxes, foco, selección por teclado y estados de error deben seguir siendo
accesibles. La corrección no agregará animaciones ni dependencias frontend.

## Pruebas de aceptación

1. Seleccionar Bollo Lotus (`point_product_id=110`) no incluye Glow 2 aunque
   ambos tengan SKU `0160`.
2. Seleccionar Vaso Fresas con Crema Mediano o Grande no incluye Viva Party 1 o
   2 para los SKU `0147` y `0148`.
3. Seleccionar un producto accesorio con un SKU compartido no genera la receta
   de otro producto.
4. Un ID inexistente, inactivo o fuera del catálogo se rechaza sin resultado
   parcial.
5. La categoría mostrada y exportada coincide con `PointProduct.category`.
6. Una categoría no presente en el orden preferido aparece en pantalla y Excel.
7. El selector no contiene duplicados producidos por usar el SKU como llave.
8. Los pronósticos guardados existentes no cambian.
9. Las pruebas del módulo Ventas, `manage.py check` y `migrate --check` terminan
   sin errores en PostgreSQL.
10. La pantalla se valida en un navegador local con selección, generación,
    errores de consola y solicitudes de red revisados.

## Archivos y contratos previstos

- `ventas/views.py`: catálogo, lectura y validación de la selección, exportación.
- `ventas/services/pronostico_engine.py`: filtro exacto por producto.
- `ventas/services/proyecciones_engine.py`: resolución exacta producto-receta.
- `ventas/templates/ventas/pronostico.html`: valores y etiquetas del selector.
- `ventas/tests.py`: regresiones de identidad, categorías y exportación.
- Archivos adicionales del sincronizador de recetas solo si el diagnóstico
  confirma una causa de código y el cambio permanece acotado.

El contrato compartido que cambia es la selección interna de SKU a ID de producto
Point. No se modifica una API pública. No se requieren migraciones de base de
datos para el diseño principal.

## Límites de entrega

La fase local termina con una demostración verificable, diff revisado y pruebas.
No se abrirá PR, no se fusionará y no se desplegará hasta que Mauricio vea las
correcciones y dé una autorización explícita para continuar.
