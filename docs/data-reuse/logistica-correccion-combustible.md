# Ficha de fuentes — Corrección de combustible y aviso al capturista

Fecha y ambiente consultado: 2026-11-04; código main y PostgreSQL de producción, sólo lectura.

## Necesidad y unidad de análisis

Una corrección autorizada de litros/importe de una carga en ruta, con motivo, valores anteriores/nuevos, actor y destinatario. El aviso se confirma una vez por corrección; no una vez por sesión.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Carga en ruta | logistica.CargaCombustibleUnidad / logistica_cargacombustibleunidad | API de captura; editor autorizado | PK, bitácora y repartidor | #168: bitácora 619, repartidor 12, importe 1200.00, litros 52.04 | Bitácoras, auditoría OCR, reportes de combustible |
| Autor | logistica.Repartidor / logistica_repartidor; auth.User | Identidad de la sesión al capturar | repartidor.user_id | #168 enlaza usuario 69, activo; AuditLog CREATE de #168 confirma usuario 69 | PWA, permisos, avisos |
| Historial | core.AuditLog / core_auditlog | core.audit.log_event | modelo + object_id + PK del evento | Existe CREATE para #168 | Auditoría; revisión concurrente del formulario |
| Aviso personal | core.Notificacion / core_notificacion | core.notificaciones.crear_notificacion | usuario y evento de corrección | Tabla existente, cero eventos de tipo logistica.correccion_combustible antes de implementar | Bandeja ERP y aviso al entrar |
| Combustible al cierre | logistica.BitacoraSalidaLlegada | Flujo de cierre de turno | PK bitácora | Inventario de fuentes identifica litros_cargados/costo_combustible | Totales de bitácoras |

## Alias y equivalencias

La carga en ruta y el combustible del cierre son fuentes distintas; no se fusionan ni se duplican. El editor de esta entrega corresponde a CargaCombustibleUnidad, la fuente del incidente. Se confirma identidad del autor con FK y evento CREATE; no por similitud de nombre. No cambia autor, foto, fecha ni unidad al corregir.

## Decisión de diseño

Reutilizar la carga existente, AuditLog y Notificacion. Restringir a superusuario o rol DG/ADMIN con gestión de Bitácoras; is_staff por sí solo no autoriza. Guardar modificación e historial/aviso en una transacción. Exigir motivo y revisión firmada contra sobrescritura. Reiniciar auditoría y encolarla después del commit; nunca conservar un OK de los valores antiguos. Avisos privados en ERP/App Operativa/PWA, confirmación idempotente en servidor y sin consumir desde preview. Ninguna tabla ni migración nueva.

Procedimiento reproducible: pg_isready; manage.py inventario_fuentes_datos --term combustible --term notificacion; SELECT acotado de carga #168 con repartidor/auth_user, AuditLog filtrado por modelo/object_id y conteo de Notificacion filtrado por objeto_tipo.

Riesgos: una lectura OCR no autoriza corrección automática. Validar la foto antes de guardar. Un aviso pendiente sigue disponible si falla la confirmación; sólo desaparece tras éxito del servidor. La reauditoría depende de la cola y del proveedor, por lo que una captura corregida puede quedar pendiente/error, nunca un OK heredado. No cambiar importes productivos para probar la funcionalidad.
