# Investigación transversal: primera conexión de recetas

La propuesta transversal fue autorizada por Mauricio. El inventario de producción identificó 382 modelos, 304 con registros, sin extraer valores operativos. Se detectaron compras clásicas vacías frente a departamentales con datos y servicios de auditoría que escriben; no deben ofrecerse automáticamente como READ.

## Cambio de este corte

El modelo puede descubrir dos contratos adicionales y encadenar búsqueda → identificación → ingredientes/costos guardados. No hay clasificador de intenciones ni nuevas frases obligatorias. Se reutilizan Responses, descubrimiento nativo, revalidación de permisos, historial, evidencia y presupuesto. Los ingredientes permiten enlazar al lector existente de costos de insumos; cada fuente conserva su fecha y unidad. La selección de una receta no prueba correspondencia con otra presentación comercial.

El historial guarda el catálogo con el que se obtuvo su huella de acceso. La compatibilidad con el catálogo previo conserva sus respuestas al añadir lectores, pero sigue bloqueando permisos revocados, claves desconocidas y evidencia fuera del catálogo original.

Los lectores viven en `api/ai_gateway_recipe_reads.py`, integrados al puente en `api/ai_gateway_business_reads.py`. La interfaz presenta tablas existentes, con escape de texto y unidades/fechas, sin acciones de escritura. Contratos aditivos; sin nuevas rutas, migraciones, variables de entorno ni dependencias. Se actualiza la caché del shell por el cambio de JS.

## Aceptación

- Búsqueda normalizada y alias activo devuelven candidatos acotados, nunca una selección arbitraria.
- Contexto muestra ingredientes actuales, estado de vinculación, rendimientos y costos versionados exactos, con fuente y fecha.
- Ausencia de costo permanece desconocida; versión guardada no se presenta como costo vigente completo ni como margen.
- Sin permiso de recetas, no se descubre ni ejecuta; revocación durante lectura o historial impide proyectar evidencia.
- No cambia recetas, costos, roles, flags, presupuesto ni participantes.
- La consulta nativa puede descubrir, buscar y leer en rondas sucesivas; contenido recuperado sigue tratado como datos.

## Continuación del alcance aprobado

1. Separar elegibilidad general del piloto del acceso a activos en TODOS los consumidores, con matriz de permisos por dominio e historial. No basta quitar `can_read_assets` en una función.
2. Conectar fuentes empresariales verificadas y sus relaciones mediante descriptores y contratos acotados. Nunca autorizar todos los modelos por introspección.
3. Consultas agregadas y métricas canónicas; productos, precios, mermas, margen y pronósticos con cobertura temporal comprobada.
4. Lecturas de administración, Capital Humano y logística con controles de campos y registros; ningún servicio con efectos secundarios se clasificará READ.
5. Documentos privados y tableros con fuentes, plantillas empresariales y trabajos recuperables.

Este documento no declara completados esos pasos ni activada cobertura total del ERP.

## Validación y reversión

Pruebas PostgreSQL y JS, revisión de diff, CI del PR, deploy oficial y validación de catálogo/resultado servido. Rollback: revertir el commit de este corte y desplegar por el flujo oficial. Conservar conversaciones, costos, evidencia y auditoría; no hay migraciones que revertir. Recursos locales pertenecen exclusivamente a `erp_ai_transversal_20261013`; su registro y cierre se conservan en artefactos de la tarea.

## Separación de acceso por dominio — 2026-10-16

La admisión READ requiere usuario activo, participante configurado y flag READ exacto.
El catálogo añade únicamente capacidades autorizadas por el módulo; activos deja
de ser prerrequisito de recetas y compras. La navegación y API de conversación
aceptan al participante sin conceder permisos de Orquestación.

| Superficie | Control conservado |
| --- | --- |
| Recetas y demás lectores empresariales | Permiso de cada lector, alcance y huella fresca |
| Compras departamentales | Responsable de área o gestor; confirmación e idempotencia existentes |
| Activos, incidencias y workflows de mantenimiento | Flag y permiso de activos; sucursal y controles propios |
| Audio, imágenes y PDF | Admisión común; validación de archivos y propiedad existentes |
| Historial y referencias | Revalidación de módulos y recursos; también lecturas vacías de activos |
| Consumo | Participante y reserva acumulativa existentes; sin cambios de límites |

Sin migraciones, altas de participantes, ampliación de presupuesto ni nuevas capacidades
empresariales. Pruebas con PostgreSQL sintético y proveedor simulado; conexiones externas
bloqueadas. Esto valida integración y controles, no calidad del modelo real.
