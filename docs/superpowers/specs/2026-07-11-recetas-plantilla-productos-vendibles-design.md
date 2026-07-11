# Plantilla de cálculo de insumos con productos vendibles

## Objetivo

La descarga **Plantilla XLSX** de Cálculo de insumos debe incluir por defecto el
catálogo de productos finales que se venden y que están vinculados con una
receta ERP. La persona usuaria solo tendrá que capturar la cantidad que desea
producir.

## Alcance

- Modificar exclusivamente la generación de la plantilla XLSX de Cálculo de
  insumos y sus pruebas automatizadas.
- Conservar la importación de plantillas anteriores.
- No cambiar modelos, migraciones, recetas, catálogo Point ni datos operativos.
- No modificar la plantilla CSV en esta tarea.

## Fuente de verdad

Los renglones se obtendrán del catálogo activo de productos Point enlazados con
una receta ERP. No se mantendrá una lista manual de productos permitidos.

Cada renglón incluirá:

1. Familia.
2. Código Point.
3. Producto ERP.
4. Cantidad plan, inicialmente vacía.
5. Notas, inicialmente vacías.

Los renglones se ordenarán primero por familia y después por nombre de producto,
con un orden estable para que la plantilla sea fácil de revisar y comparar.

## Regla de exclusión

La plantilla excluirá familias clasificadas como accesorios o artículos que no
forman parte del plan de producción, incluyendo velas, letreros, desechables y
familias equivalentes presentes en el catálogo.

La decisión se aplicará sobre la familia o clasificación canónica del producto,
no únicamente sobre palabras contenidas en el nombre comercial. La regla estará
centralizada y cubierta por pruebas para que sea auditable y ampliable sin tocar
la construcción del XLSX.

Si un producto activo no tiene receta ERP vinculada, no aparecerá en la
plantilla porque no puede generar una explosión de insumos válida.

## Compatibilidad de importación

La columna `Familia` será informativa. El importador seguirá resolviendo los
productos mediante los identificadores vigentes y continuará aceptando archivos
anteriores que no tengan esa columna.

Las cantidades vacías de la plantilla se tratarán como renglones no capturados;
no generarán producción ni errores de importación. Una cantidad positiva seguirá
siendo necesaria para incluir el producto en el cálculo.

## Flujo técnico

1. La vista de descarga consulta los productos activos enlazados con recetas.
2. Un selector enfocado filtra las familias no productivas y ordena el resultado.
3. La construcción del workbook escribe esos productos en la hoja de captura.
4. Las demás hojas y reglas del workbook conservan su comportamiento actual.
5. La respuesta mantiene el mismo tipo de archivo y nombre de descarga.

## Pruebas y validación

Las pruebas automatizadas demostrarán que:

- un producto activo de una familia vendible y con receta aparece;
- un producto de accesorios, como velas o letreros, no aparece;
- un producto sin receta no aparece;
- la cantidad y las notas quedan vacías;
- el orden es familia y producto;
- una plantilla antigua sin `Familia` continúa siendo importable.

Antes del PR se ejecutarán los tests enfocados de recetas, `manage.py check` y
`manage.py migrate --check` con una conexión PostgreSQL local válida. Después del
merge se desplegará mediante `scripts/deploy_web_safe.sh` y se descargará la
plantilla desde la pantalla real de producción para revisar contenido, consola y
respuesta de red.

## Criterio de cierre

La tarea termina únicamente cuando la plantilla descargada desde
`https://erp.pollyanasdolce.com/recetas/plan-produccion/?seccion=calculo_insumos`
contenga los productos vendibles con receta, no contenga accesorios y pueda
importarse con cantidades capturadas sin romper la compatibilidad existente.
