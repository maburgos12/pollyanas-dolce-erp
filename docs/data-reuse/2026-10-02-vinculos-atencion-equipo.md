# Ficha de fuentes — vínculos de atención de equipos

Fecha y ambiente consultado: 02/10/2026, main 2b483901 y PostgreSQL16.12 de producción, sólo lectura. Propuesta concreta aprobada por Mauricio en este chat mediante «Sí». La implementación usa un worktree y una base PostgreSQL16 exclusivos.

## Necesidad y unidad de análisis

Relacionar un reporte de falla con las órdenes existentes que lo atienden. Una relación documenta atención; no equivale a un trabajo nuevo ni confirma que dos importes sean el mismo gasto. Se admite una falla con varias órdenes y una orden con varios reportes del mismo equipo. Cada documento conserva su identidad, estados, fechas, evidencias, costos y autoría.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Equipo | activos.Activo / activos_activo | Activos; servicios de instalaciones en determinados caminos | PK, código y QR; sucursal | 196 | Pasaporte, órdenes, fallas, mantenimiento |
| Trabajo | activos.OrdenMantenimiento / activos_ordenmantenimiento | Activos y Mantenimiento web/móvil | PK/folio; activo_ref obligatorio | 63, todas cerradas; sin FK a ReporteFalla | Historial, expediente, detalle y presupuesto |
| Reporte | fallas.ReporteFalla / fallas_reportefalla | Fallas, Operación y Mantenimiento | PK; sucursal, activo opcional, duplicado_de | 107; 33 con activo; 2 duplicadas | Seguimiento, higiene, historial y presupuesto |
| Solicitud histórica | activos.SolicitudFalla / activos_solicitudfalla | Activos | PK y orden_atencion | 0 | Clasificación de órdenes/directos; no representa ReporteFalla |
| Evento | activos.BitacoraMantenimiento / activos_bitacoramantenimiento | Creación y seguimiento de orden | PK y orden_id | 63 | Trazabilidad; no otro trabajo ni gasto |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Servicio directo de equipo / orden | Confirmada como fuente | Los creadores utilizan OrdenMantenimiento | Reutilizar sin copia |
| ReporteFalla / SolicitudFalla | Distintas | Diferentes modelos y flujos | Conservar contratos |
| Reporte / orden del mismo trabajo | No resuelta sin relación | Equipo, fecha, nombre o monto no acreditan identidad | Acción explícita de atención; sin deduplicación financiera |
| Bitácora / orden | Relación confirmada | FK orden_id | Conservar como evento |
| Instalación sin activo / activo denominado instalación | No resuelta | _get_installation_asset crea/reutiliza Activo; ReporteFalla permite instalación sin equipo | Fuera del vínculo de equipos; tratamiento posterior |

## Decisión de diseño

Reutilizar OrdenMantenimiento, ReporteFalla, permisos por objeto y core.audit. Añadir únicamente una relación con par único orden/reporte, creador, fecha y motivo; ningún importe ni maestro paralelo. Sin backfill ni fusión histórica. Primera cobertura: mismo activo registrado y sucursal coherente; sin reportes marcados duplicados. Las consultas autorizan ambos extremos y las acciones reutilizan el permiso de escritura de Mantenimiento.

Mostrar relaciones en el detalle existente sin cambiar UID, tipos, filtros, totales ni paginación del historial. Relacionar documentos no cambia estados ni reconocimiento de gastos. El presupuesto vigente se conserva.

Proteger ambos documentos mediante PROTECT y manejar el error en todas las rutas de eliminación/cancelación identificadas. Una solicitud cuyo documento está vinculado permanece pendiente y los archivos se conservan. Retirar un vínculo exige motivo y auditoría, sin borrar los documentos.

Consultas reproducibles: `python manage.py inventario_fuentes_datos --term servicio --term mantenimiento --term falla --term orden` en producción (38 modelos candidatos, 15 mostrados); consulta Django dentro de transaction.atomic y SET TRANSACTION READ ONLY para count, agrupación de estados, FK y duplicado_de de los cinco modelos del dominio. El inventario es léxico; no prueba equivalencia. Grafo de código consultado: índice existente devolvió cero resultados para estos símbolos; se contrastaron definiciones, rutas y llamadores mediante rg y lectura acotada.

Riesgos y límites: no sumar componentes conectados como una intervención; no excluir automáticamente fallas vinculadas del gasto; no crear equipos para enlazar instalaciones; preservar períodos/centros históricos. Revisar concurrencia entre vínculo y eliminación, solicitudes pendientes, errores de auditoría, paginación y acceso al segundo documento. No se modifica Compras, Logística, RRHH, Point, inventario, finanzas, configuración ni datos históricos en este paquete.

## Validación de implementación

Suite PostgreSQL16: `manage.py test activos mantenimiento fallas operacion reportes.tests_presupuesto_real --keepdb --noinput`: 764 pruebas, OK. `check`, `migrate --check` y `makemigrations --check --dry-run`: sin errores ni cambios faltantes; sintaxis del JS inline y del service worker verificada con Node. Revisiones independientes de especificación y calidad: sin hallazgos pendientes.

Navegador local autenticado: creación de dos vínculos y retiro de uno, verificación de fuentes intactas, lector sin controles de edición ni importes restringidos, regreso al historial filtrado y conservación de borradores en los detalles operativos; ancho móvil 390px y consola sin errores. La prueba histórica de migración de Activos restaura ahora todas las migraciones dependientes antes del flush, incluso si falla la prueba. No cambia código operativo de Activos por este ajuste.

La migración 0007 sólo crea la relación y sus restricciones; sin operaciones de datos. La comprobación final de producción se registra en el informe de cierre externo, incluyendo conteos y huellas de los documentos antes y después del despliegue.

Validación adicional de frontera: una prueba con 21 casos de JSON no objeto en ambas direcciones de alta y en retiro confirma respuesta 400 y conservación exacta de documentos, vínculos y auditoría. Las 20 pruebas de vínculos pasaron después de este ajuste; los formularios QueryDict y objetos JSON siguen funcionando. La primera ejecución CI se sustituyó por una ejecución de la versión final, sin omitir sus checks obligatorios.
