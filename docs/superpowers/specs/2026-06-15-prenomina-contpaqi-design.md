# Diseno: RRHH Prenomina y Export CONTPAQi Nominas

Fecha: 2026-06-15
Estado: aprobado para plan de implementacion
Alcance: nueva pantalla `RRHH -> Prenomina`, corte de periodo, reporte interno imprimible por persona, ajustes auditados de asistencia y export de movimientos para CONTPAQi Nominas Desktop.

## Objetivo

Crear una mesa de cierre de prenomina dentro del ERP que consolide asistencia, incidencias, permisos, vacaciones, suspensiones, horas extra y ajustes manuales antes de entregar movimientos a CONTPAQi Nominas Desktop.

El sistema debe permitir revisar el periodo por persona, imprimir evidencia interna y exportar solo movimientos definitivos. No debe duplicar captura que ya existe ni inventar datos fuera de la base actual del ERP.

## Decisiones Aprobadas

- La funcionalidad vivira en una pantalla nueva: `RRHH -> Prenomina`.
- El primer alcance exportara movimientos de incidencias, dias y horas. Bonos e importes quedan visibles en el reporte interno, pero no se exportan a CONTPAQi hasta confirmar claves de concepto.
- La prenomina se construira desde la base de datos actual del ERP.
- Los ajustes de checada aprobados actualizaran `AsistenciaEmpleado`, guardando antes/despues en una bitacora propia para auditoria.
- El diseno puede tener una composicion propia de mesa de cierre, pero debe respetar navegacion, controles, colores, tablas, botones y estilo operativo del ERP.

## Contexto Existente

Ya existen estas piezas y deben reutilizarse:

- `rrhh.Empleado`: fuente de verdad de personas, codigo, nombre, fecha de ingreso, sucursal, area, puesto y salario.
- `rrhh.AsistenciaEmpleado`: checadas del dia: entrada, salida comida, regreso comida, salida, minutos comida y minutos trabajados.
- `rrhh.IncidenciaAsistencia`: resultado de reglas: falta, retardo, falta por retardos, jornada incompleta, comida excedida, suspension, hora extra pendiente, avisos.
- `rrhh.IncidenciaAsistenciaBitacora`: historial actual de cambios manuales sobre incidencias.
- `rrhh.PermisoSalida` y `rrhh.PermisoSalidaCambio`: permisos y cambios auditados.
- `rrhh.SolicitudVacaciones`: vacaciones aprobadas.
- `rrhh.HoraExtra`: solicitud, autorizacion, rechazo, pago o cancelacion de horas extra.
- `rrhh.NominaPeriodo` y `rrhh.NominaLinea`: periodo y lineas de nomina ya existentes.
- `bonos_produccion` y `bonos_ventas`: bonos y ajustes operativos que pueden mostrarse como referencia interna.

No existe aun:

- Corte de prenomina guardado y reproducible.
- Export ERP -> CONTPAQi Nominas.
- Ajuste auditado sobre la checada base (`AsistenciaEmpleado`).
- Bandeja central de pendientes antes del cierre de prenomina.

## Arquitectura

### Modelos Nuevos

`PrenominaCorte`

- folio
- fecha_inicio
- fecha_fin
- fecha_corte
- tipo_periodo: semanal, quincenal, mensual o rango manual
- sucursal o area opcional
- estado: borrador, en_revision, listo, exportado, cerrado
- creado_por
- creado_en
- actualizado_en
- notas
- resumen JSON para totales del corte

`PrenominaEmpleadoResumen`

- corte
- empleado
- dias_periodo
- dias_laborables
- dias_no_laborados_pre_ingreso
- dias_asistencia
- faltas
- retardos
- suspensiones
- permisos
- vacaciones
- horas_extra_autorizadas
- ajustes_pendientes
- alertas_bloqueantes
- estado: listo, revisar, bloqueado
- observaciones
- snapshot JSON con detalle calculado

`PrenominaMovimiento`

- corte
- empleado
- fecha
- tipo_movimiento_erp
- clave_contpaqi
- valor
- horas
- importe
- fuente_modelo
- fuente_id
- estado: pendiente_configuracion, listo, bloqueado, exportado
- referencia_erp
- observaciones

`AjusteAsistencia`

- empleado
- fecha
- asistencia
- tipo_ajuste: entrada, salida, salida_comida, regreso_comida, turno, observacion
- estado: pendiente, aprobado, rechazado, aplicado
- valores_anteriores JSON
- valores_propuestos JSON
- valores_aplicados JSON
- motivo
- solicitado_por
- autorizado_por
- aplicado_por
- creado_en
- autorizado_en
- aplicado_en
- comentario_autorizacion
- evidencia opcional

### Servicios

`rrhh/services_prenomina.py`

- Crear corte desde un rango.
- Recalcular corte.
- Construir resumen por empleado.
- Construir movimientos exportables.
- Detectar alertas y bloqueos.
- Aplicar ajustes aprobados y recalcular reglas del dia.

`rrhh/exporters/contpaqi_prenomina.py`

- Generar archivo de revision XLSX.
- Generar archivo de movimientos para CONTPAQi Nominas.
- Validar claves de equivalencia antes de exportar.

`rrhh/services_ajustes_asistencia.py`

- Crear solicitud de ajuste.
- Aprobar o rechazar ajuste.
- Aplicar ajuste a `AsistenciaEmpleado`.
- Registrar historial antes/despues.
- Recalcular incidencias del dia.

## Reglas Del Corte

El corte usa la misma base del ERP. El orden de lectura es:

1. Empleados activos o empleados con actividad dentro del periodo.
2. `fecha_ingreso` para excluir dias previos como no laborados.
3. Asistencias existentes.
4. Permisos autorizados.
5. Vacaciones aprobadas.
6. Suspensiones activas.
7. Horas extra autorizadas.
8. Incidencias no resueltas.
9. Ajustes de asistencia aplicados.

Reglas obligatorias:

- Dias antes de `Empleado.fecha_ingreso` son `No laborado (previo ingreso)` y no se exportan como falta.
- Incidencias `resuelto` no impactan prenomina ni export.
- Incidencias `pendiente` generan revision o bloqueo segun severidad.
- Incidencias `conciliado` pueden impactar movimientos.
- Horas extra solo exportan si estan autorizadas.
- Suspensiones solo exportan si estan activas/conciliadas.
- Permisos y vacaciones autorizadas justifican dias y evitan faltas cuando corresponda.
- Checadas incompletas generan alerta hasta que se resuelvan o ajusten.
- Ajustes pendientes bloquean export CONTPAQi, pero no bloquean reporte de revision.

## Ajustes De Asistencia

La mesa de ajustes debe cubrir casos operativos frecuentes:

- Falto checar salida.
- Falto checar entrada.
- Checo entrada o salida incorrecta.
- Falto checar salida comida.
- Falto checar regreso comida.
- Cambio de turno aplicado.
- Correccion de observacion para nomina.

Flujo:

1. Jefe directo o RRHH solicita el ajuste.
2. Se captura motivo obligatorio.
3. El sistema guarda valores anteriores y valores propuestos.
4. RRHH aprueba o rechaza.
5. Si aprueba, el sistema actualiza `AsistenciaEmpleado`.
6. El sistema recalcula reglas del dia.
7. El ajuste queda como aplicado con historial completo.

La checada original no se pierde: queda en `valores_anteriores` del ajuste. El reporte de prenomina y asistencia leen la asistencia corregida para no complicar cada consumidor del dato.

## Interfaz

### Pantalla Principal: `RRHH -> Prenomina`

Estructura:

- Navegacion estandar de RRHH con `module-tabs`.
- Filtros de corte:
  - fecha inicio
  - fecha fin
  - fecha corte
  - tipo de periodo
  - sucursal o area opcional
  - boton `Generar corte`
- Resumen del corte:
  - colaboradores incluidos
  - faltas reales
  - retardos
  - suspensiones
  - horas extra autorizadas
  - ajustes pendientes
  - alertas bloqueantes
  - movimientos listos para CONTPAQi
- Tabla por empleado:
  - codigo
  - empleado
  - area/sucursal
  - fecha ingreso
  - dias periodo
  - dias laborables
  - asistencias
  - faltas
  - retardos
  - suspensiones
  - horas extra
  - ajustes pendientes
  - estado
  - acciones: `Ver`, `Imprimir`, `Ajustar`

### Vista Por Persona

La ficha individual muestra:

- Encabezado de empleado y periodo.
- Resumen del periodo.
- Tabla dia por dia.
- Checadas originales/corregidas cuando exista ajuste.
- Incidencias.
- Permisos, vacaciones y suspensiones.
- Horas extra autorizadas.
- Ajustes aplicados y pendientes.
- Observaciones para nomina.
- Botones:
  - `Imprimir / PDF`
  - `Solicitar ajuste`
  - `Exportar persona`

### Bandeja De Ajustes

Dentro del corte:

- Pendientes
- Aprobados
- Rechazados
- Aplicados
- Acciones:
  - `Revisar`
  - `Aprobar`
  - `Rechazar`

## Reglas UI

La pantalla puede tener composicion propia de mesa de cierre, pero debe respetar el lenguaje del ERP:

- Usar navegacion estandar de RRHH.
- Usar componentes base: `card`, `card-header`, `kpi-grid`, `kpi-card`, `table-responsive`, `table table-striped`.
- Usar botones estandar: `btn btn-primary`, `btn btn-secondary`, `btn-sm`.
- No usar landing page.
- No usar tarjetas dentro de tarjetas.
- Mantener tablas densas, legibles y operativas.
- Usar colores institucionales y estados consistentes.
- Acciones visibles y claras.
- Imprimibles sobrios en tamano carta.

Se permite creatividad en:

- Mesa de cierre del periodo.
- Semaforo de exportacion CONTPAQi.
- Bloques compactos por estado: listo, revisar, bloqueado.
- Ficha imprimible por empleado.
- Linea de trazabilidad: checador -> reglas -> ajustes -> prenomina -> export.
- Panel de validaciones del corte.

## Export CONTPAQi Nominas

El primer export sera un layout de movimientos. No es calculo total de nomina ni lista de raya.

Columnas base:

```text
CodigoEmpleado
Fecha
Dia
TipoMovimiento
ClaveCONTPAQi
Valor
Horas
Importe
ReferenciaERP
Observaciones
```

Movimientos considerados en primera version:

- Falta real.
- Suspension activa.
- Hora extra autorizada.
- Incapacidad si existe fuente confiable.
- Otros movimientos aprobados y configurados.

No se exporta:

- Dias pre-ingreso.
- Incidencias resueltas.
- Alertas pendientes.
- Checadas incompletas sin ajuste aprobado.
- Horas extra pendientes o rechazadas.
- Bonos o importes sin claves CONTPAQi confirmadas.

## Equivalencias CONTPAQi

Se necesita configuracion de equivalencias:

- tipo_movimiento_erp
- clave_contpaqi
- descripcion
- aplica_valor
- aplica_horas
- aplica_importe
- activo

Sin equivalencias completas, el ERP puede generar reporte interno y layout de revision, pero debe bloquear el export final de movimientos CONTPAQi.

## Validaciones

Bloquean export CONTPAQi:

- Empleado sin codigo.
- Movimiento sin clave CONTPAQi.
- Ajustes pendientes.
- Incidencias criticas pendientes.
- Hora extra pendiente o rechazada cuando se pretende exportar.
- Checada incompleta sin ajuste aprobado.

No bloquean reporte interno:

- Alertas informativas.
- Dias pre-ingreso.
- Incidencias resueltas.
- Bonos sin clave CONTPAQi en esta primera version.

## Reportes

### Reporte Interno De Revision

Formato XLSX:

- Resumen
- Empleados
- Detalle_Diario
- Incidencias
- Ajustes
- Horas_Extra
- Movimientos_CONTPAQi
- Validaciones

### Reporte Imprimible Por Persona

Formato de pantalla imprimible:

- Datos del empleado.
- Periodo.
- Resumen de dias.
- Incidencias del periodo.
- Ajustes aplicados.
- Horas extra autorizadas.
- Observaciones.
- Firmas si se decide en implementacion.

## Permisos

- RRHH puede generar cortes, aprobar ajustes, exportar y cerrar.
- Jefe directo puede solicitar ajustes de sus colaboradores y consultar fichas de su equipo.
- Direccion o admin puede ver todo.
- Usuarios sin permiso RRHH no pueden exportar movimientos.

## Estados Del Corte

- `borrador`: corte creado, editable.
- `en_revision`: tiene alertas o ajustes pendientes.
- `listo`: sin bloqueos para export.
- `exportado`: archivo CONTPAQi generado.
- `cerrado`: corte bloqueado para cambios ordinarios.

## Pruebas

Pruebas unitarias:

- Pre-ingreso no genera falta ni movimiento.
- Incidencia resuelta no impacta prenomina.
- Falta conciliada genera movimiento si tiene clave.
- Hora extra autorizada genera movimiento.
- Hora extra pendiente bloquea export.
- Ajuste aprobado actualiza `AsistenciaEmpleado` y conserva historial.
- Ajuste pendiente bloquea export.
- Empleado sin codigo bloquea export.

Pruebas de vista:

- `RRHH -> Prenomina` renderiza filtros y resumen.
- Se puede generar corte.
- Vista por persona muestra detalle diario.
- Imprimir persona renderiza layout.
- Export de revision se descarga aunque existan alertas.
- Export CONTPAQi se bloquea con validaciones pendientes.

Pruebas de regresion:

- `Reporte asistencia` sigue funcionando.
- Edicion de incidencia con bitacora sigue funcionando.
- Permisos y horas extra conservan sus flujos existentes.

## Fuera De Alcance De La Primera Version

- Calculo completo de nomina.
- Timbrado.
- Conexion directa a CONTPAQi por API.
- Export de bonos/importes sin claves confirmadas.
- Sustituir `NominaPeriodo`.
- Borrar o reescribir checadas originales sin historial.

## Riesgos

- Las claves reales de CONTPAQi dependen de la empresa y deben confirmarse antes del export final.
- Si se mezclan ajustes con edicion directa de incidencias puede duplicarse el efecto; por eso el ajuste de checada debe actualizar asistencia y recalcular reglas.
- Si se permite exportar con alertas pendientes se puede mandar prenomina incorrecta; por eso el export CONTPAQi debe bloquearse, pero el reporte interno debe seguir disponible.

## Criterios De Aceptacion

- Existe pantalla nueva `RRHH -> Prenomina`.
- Se puede generar un corte de rango.
- Se ve resumen por persona.
- Se puede abrir e imprimir ficha individual.
- Se pueden solicitar, aprobar, rechazar y aplicar ajustes de asistencia.
- Ajustes aprobados actualizan `AsistenciaEmpleado` y guardan historial antes/despues.
- El corte bloquea export CONTPAQi cuando hay pendientes.
- El reporte interno XLSX se puede descargar para revision.
- El export CONTPAQi solo incluye movimientos definitivos y configurados.
- Los dias pre-ingreso no aparecen como faltas.
- Las incidencias resueltas no castigan ni exportan.

