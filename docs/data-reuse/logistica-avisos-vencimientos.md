# Ficha de fuentes — avisos de licencias y documentos

Fecha: 26 de noviembre de 2026. Producción consultada en sólo lectura; PostgreSQL 16 local exclusivo para pruebas sintéticas.

## Necesidad y unidad de análisis

Avisar sobre una licencia o documento vigente por persona/unidad, fecha registrada y umbral alcanzado. Cada intento de transporte tiene destinatario institucional; aceptación del correo no prueba entrega ni renovación.

## Fuentes y consumidores

| Concepto | Fuente oficial / identificador | Evidencia | Consumidores |
| --- | --- | --- | --- |
| Licencia | `logistica.Repartidor`, PK y `licencia_expiracion` | 15 registros de repartidores; incluye una licencia vencida, otra próxima y fechas ausentes. No se modifican fechas ni expedientes. | RRHH, administración, API de repartidores y bloqueo de inicio de turno existente |
| Documento vehicular | `logistica.DocumentoUnidad`, PK y `fecha_vencimiento`; unidad activa y documento vigente | Seis documentos en producción. `vigente=False` puede representar un documento retirado; no se reactiva ni se interpreta como licencia. | Flota, tablero ejecutivo y tarea diaria |
| Destinatarios | `ConfigAlertaFlota.destinatarios` y usuarios activos; respaldo institucional en `LOGISTICA_VENCIMIENTOS_DESTINATARIOS` | No había configuraciones de flota. Sin usuarios de supervisión logística asignados. Respaldo explícito: correo institucional de Dirección. | Servicio de avisos y tablero ejecutivo |
| Programación | Celery Beat, tarea `logistica.tasks.alertar_documentos_por_vencer` | Habilitada diariamente a las 08:00 America/Mazatlan. Logs 24–26/nov: cero avisos por configuración ausente. | Trabajador Celery existente |
| Notificación en ERP | `core.Notificacion` | Es una bandeja por usuario y lectura, sin unicidad por fecha/umbral/correo ni estado del transporte. | Bandeja general; no se altera su contrato |

## Alias y equivalencias

Licencia de persona y documento de unidad son entidades distintas. No se fusionan registros por nombre, teléfono ni archivo. Se reutilizan PK existentes. Correos se recortan, validan, normalizan y deduplican; sólo dominio exacto `pollyanasdolce.com`. La selección explícita sin destinos válidos bloquea el envío y genera advertencia; no se reemplaza silenciosamente por Dirección. Una desactivación explícita se respeta.

## Decisión

Reutilizar las fuentes actuales. Extender configuración con último intento y resultado; crear únicamente bitácora de transporte `AvisoVencimientoFlota`, sin otra captura de personas o documentos. Clave única por fuente/PK/fecha/umbral/correo; permite nueva serie cuando cambia el vencimiento o destinatario. Se recupera el umbral más urgente alcanzado, sin ráfaga de los anteriores; incluye vencidos y licencias sin fecha. No cambia `vigente` por el mero vencimiento.

Se excluyen cuentas técnicas y usuarios/unidades inactivos. Los errores de rechazo explícito o cero aceptaciones se reintentan en la siguiente revisión diaria. Una interrupción o excepción de resultado desconocido requiere cotejo con el proveedor antes de decidir un reenvío. Sin prueba de rechazo no se reenvía automáticamente. La bitácora es de sólo lectura en administración.

El tablero conserva sus permisos existentes. Muestra destinatarios efectivos, licencias/documentos críticos, ausencia de primera ejecución, revisión atrasada (>26 h), desactivación, configuración inválida y avisos pendientes/inciertos. No se amplían APIs ni permisos.

## Procedimiento y límites

Inventario local: `manage.py inventario_fuentes_datos --term licencia --term vencimiento --term 'alerta flota'`, después de aplicar migraciones y comprobar PostgreSQL. Consulta productiva: transacción `READ ONLY`, límite de 10 segundos; modelos anteriores, configuración, destinatarios y tarea Beat exacta. Logs acotados del trabajador confirman el retorno `config_no_activa`.

No se presume entrega por aceptación del proveedor, existencia de un archivo por su campo ni vigencia física por los registros. El incidente concreto sigue sin persona/fecha confirmadas. No se cambiaron expedientes operativos ni se enviaron mensajes manuales durante la implementación.
