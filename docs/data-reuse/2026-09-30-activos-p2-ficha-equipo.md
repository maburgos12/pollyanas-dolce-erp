# Ficha de fuentes — Activos P2: ficha y datos del equipo

Fecha y ambiente consultado: 2026-09-30. Diseño aprobado basado en lectura PostgreSQL de producción; implementación y pruebas en PostgreSQL 16 aislado, worktree `activos-ficha-p2-20260930`. Las altas/ediciones de prueba son ficticias y locales.

## Necesidad y unidad de análisis

Una fila de `Activo` representa un equipo identificado por PK/código/QR. Consultar su pasaporte vigente, corregir nombre/categoría/criticidad/notas y asignar sucursal por FK sólo al crear. Sucursal y área/ubicación interna son dimensiones diferentes. Órdenes, fallas y planes son documentos relacionados.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Equipo | activos.Activo / activos_activo | Catálogo e importador vigentes | PK, código y qr_token; sucursal nullable | Diseño aprobado: producción main 73d9ea2a, 185 equipos, 6 sin sucursal, 185 sin serie y 185 sin costo de adquisición; snapshot, no inventario físico | Pasaporte, fallas, órdenes, planes, importación y reportes |
| Sucursal | core.Sucursal / core_sucursal | Catálogo corporativo | PK explícita; no inferida de ubicación | Diseño aprobado confirmó FK con inventario y consultas acotadas; tests locales verifican FK/null/inexistente | Permisos, operación y reportes |
| Pasaporte | activos.services_pasaporte.construir_pasaporte | Lectura de fuentes existentes | Equipo y documentos por FK | Servicio existente; diez eventos por lista, no historial completo | operacion:activo_pasaporte |
| Acceso | activos_autorizados y mantenimiento.services_access.can_view_costs | Políticas vigentes | Scope por usuario/sucursal y costos por rol existente | Tests existentes de alcance/HTML; catálogo calcula enlace desde el mismo queryset | Pasaporte y QR |
| Auditoría | core.AuditLog / core_auditlog | core.audit.log_event | Objeto y usuario; payload antes/después de campos cambiados | Tests locales de cambios, reenvío sin cambios y rollback conjunto | Trazabilidad |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Equipo / activo | Confirmada como concepto | Misma fuente Activo; nombre repetido no prueba identidad física | No se fusionan registros |
| Sucursal / ubicación | Distinta | FK canónica frente a texto interno; NULL se muestra Sin sucursal asignada | Traslados/correcciones existentes fuera de P2 |
| Orden / falla / plan / equipo | Distinta | Documentos referencian un equipo, no son una segunda identidad | Se conserva cada fuente |
| Nombre similar del importador | Candidata | Su heurística nominal puede cambiar al renombrar; P2 no modifica importador | Revisión humana en futura importación |

## Decisión de diseño

Extender campos y ruta POST existentes, sin nuevos modelos, captura financiera ni reglas de acceso. `Ver ficha` reutiliza token y endpoint existente, condicionado por `activos_autorizados`. Alta selecciona una FK existente o NULL explícito; no reasigna equipos anteriores.

El parcial `activos/_datos_equipo.html` contiene sólo datos descriptivos, indicador operativo recalculado y formulario. Se reutiliza para el catálogo y para el envelope `target/html` de `data-async-action`, sin devolver ficha técnica, importes ni facturas. Edición usa whitelist de cuatro campos, validadores del modelo sin truncar, token `actualizado_en`, row lock y auditoría en una misma transacción; un reenvío igual no vuelve a auditar. Las acciones técnicas existentes mantienen su política.

Consultas o procedimiento reproducible: diseño aprobado ejecutó `pg_isready`, comprobó `connection.vendor == 'postgresql'`, `transaction.atomic` + `SET TRANSACTION READ ONLY`; `inventario_fuentes_datos` con equipo/activo/sucursal/mantenimiento y `limit=10` (113 candidatos léxicos, mostró 10). Counts acotados de Activo, sucursal NULL, serie vacía y costo NULL. No establece equivalencias semánticas ni autoriza fusionar identidades. El plan y evidencia original se conservan externamente en `Downloads/analisis-administracion-activos-2026-09-30/diseno-p2-ficha-equipo.md`.

Riesgos y pendientes: la ficha es historia reciente, no historia contable completa; la importación requiere revisión tras renombrar. No se altera clasificación histórica del gasto, importes ni referencias. CI, navegador y validación productiva se verifican en el flujo de entrega; pruebas de escritura sólo en local. Los conteos citados son la lectura fechada del diseño, no una lectura nueva tras implementar.
