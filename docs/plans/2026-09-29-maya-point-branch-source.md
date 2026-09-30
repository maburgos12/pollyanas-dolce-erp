# Corrección del origen de sucursal Point para Maya

## Resultado requerido

Una fila histórica no puede ganar por nombre antes de la fila del ID Point solicitado. La disponibilidad debe conservar la identidad real del resultado vivo sin reemplazar el código ERP existente. No se modifican sucursales, históricos, reservas, precios ni configuración productiva.

## Implementación mínima

1. En `PointLiveInventoryLookupService._find_branch_row`, buscar todos los ID exactos antes de evaluar nombres. Exigir una única coincidencia; un ID numérico configurado ausente o contradictorio debe fallar. Con un alias no numérico, aceptar solo una fila del nombre Point identificado. Sin PointBranch, conservar el respaldo ERP unívoco.
2. Cambiar la versión de la clave de caché del stock vivo para no reutilizar resultados del selector anterior.
3. Añadir campos opcionales de producto/sucursal Point a `PickupAvailability` y `to_dict`, provenientes exclusivamente del resultado vivo real. Mantener `branch_code`, nombres y demás campos existentes para compatibilidad. No presentar una identidad de snapshot como evidencia viva.
4. Probar primero el fallo: Crucero aparece antes que Bamoa PK 2; duplicados y conflictos de ID; alias Bamoa; sucursales normales; respuesta con ERP `CRUCERO` e identidad viva Point 2/Bamoa. Ejecutar las suites relevantes de CRM y del lector Point con PostgreSQL 16 aislado.
5. Revisión de especificación, después calidad, y comprobación independiente. Registrar límites antes de cualquier despliegue.

## Entorno y alcance

Base `b9b6c2321f6afd56ce32f6ab44fc0538aeff4803`; worktree `maya-point-branch-source`. PostgreSQL 16 aislado en puerto 55593, Compose `erp_maya_point_source`. Preflight aprobado, migraciones completas y `manage.py check` sin errores antes de editar.

La ficha `docs/data-reuse/maya-point-branch-price-2026-09-29.md` registra fuentes reales. Precio por sucursal y catálogo Maya Bamoa siguen siendo decisiones/validaciones posteriores. Ninguna prueba local implica activación del piloto 6264 o cambio en la línea 6756.

## Segunda etapa: disponibilidad sin escrituras de reservas

El rastreo comprobó que `get_availability` llama a `_reserved_qty`, que barre reservas vencidas. Esa lectura pública puede actualizar estados comerciales. `create_reservation` ya ejecuta explícitamente `expire_stale_reservations` antes de comprobar disponibilidad, por lo que el barrido no necesita estar en la lectura.

Después de aprobar la primera etapa, hacer `_reserved_qty` una consulta pura: sumar reservas confirmadas y activas sin vencimiento o con vencimiento igual/posterior al instante de consulta. Excluir activas vencidas sin modificar sus estados. Mantener la limpieza explícita de los flujos de escritura. Eliminar exclusivamente la lógica de debounce que quede sin consumidores, sin cambiar configuración productiva.

Probar RED/GREEN que una lectura no invoca barrido ni modifica reservas, que el stock prometible excluye vencidas y conserva confirmadas/activas vigentes, y que la creación explícita conserva la limpieza. Repetir revisiones de especificación y calidad antes de integrar la etapa.

## Evidencia local completada

- Etapa 1: `ce14cb6` y `98c82cf`. Revisión de especificación y calidad aprobadas. Se corrigió también la construcción del resultado: un código o identificador ausente permanece desconocido, no se rellena desde la petición. 47 pruebas PostgreSQL 16 aprobadas por el implementador y la revisión independiente.
- Etapa 2: `d550657`. La regresión RED comprobó que el GET anterior cambiaba una reserva ACTIVE a EXPIRED. La corrección GREEN excluye reservas activas vencidas sin escribir y mantiene la limpieza explícita de creación/confirmación. Especificación y calidad aprobadas, sin hallazgos.
- Comprobación del coordinador: 49 pruebas de `api.tests_public_api`, `pos_bridge.tests.test_live_inventory_lookup_service` e `inventario.tests_point_reconciliation`, aprobadas en PostgreSQL 16 (1.541 s). Revisión de calidad independiente: las mismas 49 aprobadas (1.465 s). `migrate --check` sin migraciones pendientes.

Sin push, despliegue, cambios comerciales, configuración de sucursal, mensajes ni activación del piloto. Falta adaptar el consumidor Maya y aprobar la configuración Bamoa; el alcance comercial de `Precio_default` sigue pendiente.
