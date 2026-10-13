# Ficha de fuentes — fechas de bonos de ventas

Fecha y ambiente: 2026-10-13; PostgreSQL 16 local aislado y lecturas acotadas de producción.

## Necesidad y unidad de análisis

Un registro corresponde a empleado + corte + fecha completa. El calendario anterior identificaba días por posición en el corte, mientras RRHH usaba el número del día del mes. En el corte 28-mayo–27-junio, una asistencia de junio-1 se mostraba en mayo-28.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Asistencia | rrhh.AsistenciaEmpleado | Checador / RRHH | empleado + fecha | 18 registros automáticos del día 1 en el corte junio coinciden con asistencia de junio-1; 1532 asistencias RRHH en el rango completo | Servicios de sincronización de ambos bonos |
| Registro de ventas | bonos_ventas.RegistroDiarioVentas | RRHH automático / captura manual autenticada | antes bono + dia; ahora bono + fecha | 4691 filas, 165 bonos; junio: 892 filas de 32 empleados, 696 sin capturista de la sincronización mensual y 196 manuales | Calendario, API, admin y recálculo |
| Corte | bonos_ventas.ConfigBonoVentasPeriodo | Configuración existente | mes + año y rango opcional | 5 periodos; junio cruza meses; otros periodos observados corresponden a un mes completo | Calendario y sincronización |
| Ajustes y categorías | BonoVentasEmpleado / VentaCategoriaSucursal | Captura autorizada / servicios existentes | bono y periodo + sucursal + categoría | Huellas completas de 165 bonos y 8 categorías; respaldo de las tres tablas afectadas | Resumen, nómina y repartidores |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Día de RRHH vs posición de calendario | Distintos | día 1 de junio y primera posición (28-mayo) no son la misma fecha | No mezclar identificadores |
| Fecha automática legada | Confirmada por contrato de origen | get_or_create(bono, dia=fecha.day) en el mes del periodo; capturado_por NULL; lecturas y fechas de creación coherentes con sincronización | Mantener el alcance mensual existente |
| Captura manual en corte desplazado | No resuelta | Hay capturista, pero no se guardó fecha; posición y día del mes difieren | Conservar NULL, valores y notas; revisión humana antes de asignar fecha |
| Captura manual en mes completo | Confirmada | Posición desde el día 1 coincide con fecha del mes | Completar metadata sin cambiar la captura |

## Decisión de diseño

Extender la tabla existente con fecha nullable y unicidad bono + fecha; conservar unicidad bono + dia solo en legados NULL. La migración completa 4495 fechas seguras y mantiene 196 manuales pendientes. No actualiza asistencias, uniformes, puntualidad, notas, capturistas, timestamps ni valores de bonos. No se crea otra fuente ni se cambia la cobertura mensual del checador o las fórmulas monetarias.

La API exige fecha completa en cortes desplazados al crear y en toda edición de una fila fechada (rechaza calendarios anteriores), valida rango/día, rechaza futuro, duplicados y cambios de identidad. Los bonos con históricos sin fecha no admiten nuevas filas ni sincronización que pudiera duplicarlas. Las notas legadas siguen editables por ID. Cambios manuales de asistencia/puntualidad registran capturista y quedan protegidos frente a la sincronización automática. Calendario y detalles identifican filas por fecha / ID, no por posición. SW y CSS actualizan versión. La carga descarta respuestas de otro bono; selección y registros se limpian juntos y la edición permanece bloqueada durante la carga.

Procedimiento: inventario_fuentes_datos --term asistencia --term bono --term registro; inspección de creadores y consumidores; BEGIN READ ONLY con agregados por periodo, capturista y fechas de creación; huellas JSON de filas/campos para comparar antes/después, sin exponer datos personales. Respaldo pg_dump de registros, bonos y periodos verificado contra las secciones COPY de una lectura nueva de las tablas fuente.

Riesgos y pendientes: no reinterpretar los 196 manuales de junio. Abrir el resumen de mayo/junio ya guarda indicadores recalculados (22 diferencias previas de cancelación, sin diferencias monetarias); evitar ese recálculo durante la auditoría de fechas. Usar consultas de solo lectura para esos históricos y prueba aislada de navegador. La auditoría del contrato mensual de junio detectó 138 diferencias previas entre estado guardado y estado actual RRHH; conservar los valores sin conciliarlos. Hay 177 registros sin capturista con actualizaciones posteriores: la fecha conserva la clave original de creación mensual, sin afirmar que los booleanos actuales siguen siendo automáticos. Validar API, activos servidos, producción, todas las filas y limpieza del entorno después del despliegue. Una reversión al esquema por número de día puede fallar cuando existan dos meses con el mismo día; conservar esquema y corregir hacia adelante.
