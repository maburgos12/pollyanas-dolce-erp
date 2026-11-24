# Permiso empresarial por contingencia

Autorización de Mauricio: implementar y probar; presentar simulación del sábado 21 antes de aplicar datos productivos. No autoriza ajustar jornadas, horarios BOT, checadas, pagos ni enviar avisos al personal.

## Resultado aceptable

Una autorización empresarial por clima, seguridad, crisis sanitaria u otro motivo identifica días completos no penalizables de personal expresamente cubierto. La misma fuente gobierna asistencia, acumulados disciplinarios, avisos derivados, bonos y prenómina; no se inventa asistencia ni se exporta una falta por un permiso justificado.

## Fuentes verificadas

Base: `76b361120be19026f39cae825ff711d373acc08e`. El inventario de fuentes PostgreSQL se ejecutó en producción mediante transacción READ ONLY para horario, permiso, incidencia y venta: 76 candidatos léxicos, salida acotada a 15. Los nombres son candidatos, no equivalencias.

| Concepto | Fuente existente | Decisión |
| --- | --- | --- |
| Persona | `rrhh.Empleado`, historial `EmpleadoBaja` | Reutilizar IDs y vigencias; no emparejar sucursales por nombre. |
| Permiso personal | `rrhh.PermisoSalida` | Conservar contrato; distinguir su justificación de una falta confirmada. |
| Incidencia y auditoría | `IncidenciaAsistencia`, `IncidenciaAsistenciaBitacora` | Conservar historial; resolver generadas cubiertas con referencia y auditoría. |
| Horario comercial | `horarios_especiales.SolicitudHorarioEspecial` y reglas aprobadas | Contexto distinto de permiso laboral; no modificar ni republicar. |
| Ventas | Indicadores y ventas canónicas `pos_bridge` | No cambiar importes ni convertir faltantes en cero. |
| Permiso empresarial | No se identificó fuente equivalente | Crear fuente explícita con beneficiarios congelados y autorización trazable. |

Producción observada en lectura: sábado 21 tiene 58 faltas pendientes, 4 avisos y 6 incidencias de baja por faltas, todos automáticos. No se encontró permiso personal aprobado que cubriera ese sábado. Las incidencias de baja son alertas, no evidencia de baja efectiva. La vigencia BOT consultada es 23–29/nov.; El Túnel conserva cierre 25/nov. por regla semanal aprobada. No se copiaron registros personales a este documento.

## Trabajo y validación

- [x] Fijar fuente empresarial separada de permiso personal, estados borrador/aprobado/revocado, categoría, motivo, rango de hasta 31 días completos y tratamiento salarial explícito independiente.
- [x] Congelar IDs de beneficiarios al autorizar, validando vigencia laboral y alcance; autorización por Dirección mediante predicado existente, revisión estable y auditoría.
- [x] Crear servicio de simulación sin escrituras y comando operativo que por defecto simule. Aplicación exige actor autorizado y digest vigente del contenido y de los datos afectados. Repetir no duplica; modificaciones invalidan simulaciones anteriores.
- [x] Introducir resolución común de permiso empresarial en asistencia y reportes. La ausencia cubierta no suma faltas, retardos derivados ni avisos; conservar marcas reales y trabajo registrado. No transformar cierre comercial en permiso.
- [x] Corregir prenómina: permiso justificado no genera movimiento FALTA, pero una falta realmente validada conserva su tratamiento existente. No reabrir cortes cerrados/exportados.
- [x] Integrar bonos de ventas y producción con contexto neutral: el permiso no castiga asistencia ni cancela bono por esa ausencia; tampoco crea días trabajados, producción, uniforme ni puntualidad ficticios. Conservar ajustes manuales y pagos existentes.
- [x] Resolver solamente incidencias automáticas cubiertas y reconstruir avisos dependientes cronológicamente. Los conteos directos abarcan 29 días posteriores; si el desplazamiento de una emisión puede afectar otras después de ese límite, continuar hasta estabilizar o bloquear explícitamente ese histórico. Nunca ejecutar el motor completo de jornada sobre la cola disciplinaria ni eliminar faltas por retardos originados fuera de la cobertura. Conflictos manuales y artefactos cerrados se muestran y bloquean aplicación automática según corresponda.
- [x] Probar ámbito, vigencias, borrador/revocación, empresa/selección, actor, concurrencia, digest obsoleto, repetición, reingesta, permisos parciales personales, faltas reales vecinas, ausencia de checadas ficticias y protección financiera.
- [x] Ejecutar `check`, migraciones, pruebas afectadas y revisión funcional/de calidad: 436 pruebas PostgreSQL aprobadas, 59.477 s; revisores de especificación y calidad sin bloqueos pendientes.
- [ ] Crear PR borrador y conservar checks CI del SHA vigente. No aplicar sábado ni desplegar cambios operativos antes de presentar simulación al usuario.

Pruebas usan PostgreSQL16 exclusivo de la tarea, proyecto `erp-contingencia-empresa-20261124`, puerto 55686. Recursos y comandos están registrados en el directorio de artefactos de la tarea. No se reutilizan bases ajenas. Al entregar se respalda/verifica y retira únicamente este entorno desechable, conservando rama/worktree con handoff y revisión pendiente.

## Controles de alcance

Viernes 20: cierre aproximado a las 19:00 informado por Mauricio; sucursales y horas exactas desconocidas; hubo personal resguardado sin poder salir. No se aplica un permiso colectivo completo al viernes ni se deduce salida de la última venta. Sábado 21: candidato a permiso por cancelación empresarial, pendiente de simulación y autorización de aplicación. Los horarios de 23–29 son solo contexto de análisis.

No se cambian permisos de acceso generales, stock, ventas, salarios, horarios, vacaciones, fuentes Point ni BOT. La clasificación no penalizable y la política salarial son decisiones distintas. Una autorización de contingencia no elimina faltas ajenas a su alcance.

## Evidencia de validación

La simulación local anonimizada conserva 69 beneficiarios candidatos, resuelve 58 faltas del sábado y las 10 alertas fechadas ese día, mantiene tres conciliaciones por vacaciones y no crea checadas. En la ventana posterior, 12 alertas originales se resuelven y dos emisiones quedan sustentadas por faltas de otras fechas: el total no resuelto cambia de 14 a 4. Esto no es un digest productivo ni una simulación monetaria del personal real. El replay no contiene bonos ni nómina operativos. Las proyecciones financieras se probaron con fixtures sintéticos, incluidas diferencias entre encabezados y registros diarios.

Chromium validó reportes de escritorio/móvil, folios, anexo y descarga PDF sin errores de consola. WebKit móvil renderizó el contexto; la activación nativa del anexo no quedó validada. No se probó iPhone físico. Se conservó evidencia externa en el directorio de artefactos de la tarea.
