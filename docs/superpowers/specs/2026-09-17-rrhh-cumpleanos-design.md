# Cumpleaños de empleados activos

Diseño aprobado por Mauricio en esta conversación: calendario RRHH y avisos en ERP y correo para Capital Humano, jefes de área y Dirección. Resumen los lunes, anticipo tres días antes y aviso del día a las 08:00 America/Mazatlan.

RRHH es la fuente de personas y activo; auth.User es la fuente del correo operativo. Agregar fecha_nacimiento nullable sin backfill, sin inferir CURP/RFC y sin modificar datos existentes. Capital Humano con permiso de gestión puede registrar/corregir fecha verificada con motivo y AuditLog. Jefaturas/Dirección ven únicamente día/mes, no edad ni año. La vista global corresponde a Capital Humano y Dirección; jefaturas activas ven su departamento y equipo directo. Las demás cuentas no tienen acceso.

Calendario mensual, vista lista, hoy/próximos siete días/mes, filtros por departamento y sucursal, lista de activos sin fecha y historial de avisos para responsables globales. Fecha 29/02 se observa el 28/02 en años no bisiestos; la fecha original permanece intacta. Inactivos nunca aparecen ni generan avisos; no es necesario usuario ERP para el cumpleañero.

Reutilizar Notificacion y resolución de correo de trabajo existentes. Enviar un aviso agrupado por destinatario/tipo/fecha de referencia, con restricción única en una bitácora persistente. ERP y correo son estados independientes. Bloqueo de fila serializa concurrencia; marca en proceso antes de llamar al proveedor. Aceptado por proveedor no equivale a recibido. Fallos explícitos se reintentan; incidentes inciertos quedan visibles sin reenvío automático. Antes de cada canal se recalculan activos y alcance del destinatario. Sin fechas no se generan mensajes vacíos. No recuperar cumpleaños de días anteriores.

Job diario a las 08:00 en cola notificaciones, aislado de Point. Reintentos explícitos del job para fallos confirmados, limitados al mismo día. Navegación central NAV_GROUPS, mismos permisos en enlace y vista, bump del SW general. Formulario progresivo data-async-action con toast global, preservación de inputs y retorno a fragmento estable.

Validar fechas, cambio de año y 29/02; exclusión de bajas; alcance de jefaturas; privacidad HTML/correo; deduplicación, recuperación de fallo e incertidumbre; correo faltante; job y cola. PostgreSQL 16 aislado, checks/migraciones, pruebas RRHH/navegación, navegador escritorio/móvil, CI, merge, deploy oficial y lectura nueva en producción. Todas las fechas están pendientes de captura porque la fuente inspeccionada no almacena fecha de nacimiento.
