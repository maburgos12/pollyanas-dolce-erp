# Ficha de fuentes — Mantenimiento en el costo de personal

Fecha y ambiente consultado: 2026-11-05; producción en solo lectura y PostgreSQL16 aislado para pruebas.

## Necesidad y unidad de análisis

Clasificar el costo mensual de personal que ya tiene departamento y función MANTENIMIENTO. El lector omite ese departamento y muestra incorrectamente «función pendiente de clasificar en RRHH». No se necesita cambiar la ficha, el puesto ni los importes de nómina.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Departamento y función | rrhh.Empleado | Capital Humano | ID maestro del empleado | Registro inactivo con departamento, puesto operativo y descriptivo Mantenimiento; nivel COLABORADOR | RRHH y lectores de nómina |
| Catálogo de funciones | rrhh.CatalogoFuncionOperativa | Catálogo nativo del ERP | Código MANTENIMIENTO | Registro 15 activo y del sistema; departamento original/actual MANTENIMIENTO, función MANTENIMIENTO | Organización y ficha maestra |
| Adscripción mensual | rrhh.NominaLinea | Importación de lista de raya | Línea y periodo; snapshot del departamento | Una línea de septiembre tiene departamento_snapshot=MANTENIMIENTO y origen LISTA_RAYA; sus importes y sucursal ya están capturados | Planeación, resumen por departamento y CSV |

## Alias y equivalencias

MANTENIMIENTO es el código estructurado existente en las tres fuentes. No se añade una equivalencia, función, categoría, tabla maestra o captura. La ubicación física Matriz no convierte el costo en personal de tienda.

## Decisión de diseño

Extender el mapa de lectura y su etiqueta, conservando el grupo Mantenimiento por separado. Los consumidores de `leer_personal_registrado` y el consolidado reciben el mismo dato nativo. No se modifica el catálogo, ningún empleado, snapshot, salario, bono, finiquito ni estado de baja.

Consultas reproducibles: `inventario_fuentes_datos --term mantenimiento --term personal`; consulta acotada del catálogo por código y de la línea con el aviso, comprobando los campos estructurados del empleado y del snapshot. El inventario lexical se conserva fuera del repositorio en la evidencia de la tarea.

## Validación y riesgos

La regresión reproduce el aviso con dos quincenas pagadas de una persona inactiva cuyo departamento y puesto ya están completos. Verifica grupo, etiqueta, conteo único, total mensual, exclusión de tienda y conservación exacta de líneas y estado del empleado. No se deducen funciones para personas cuya fuente sí esté incompleta. Verificar septiembre y el CSV en producción; octubre debe conservar sus 71 personas y costo 978858.04.
