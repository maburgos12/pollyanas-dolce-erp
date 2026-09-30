# Ficha de fuentes — horas extra y corte de prenómina

Fecha y ambiente consultado: 2026-09-30; PostgreSQL local aislado y producción en modo de solo lectura.

## Necesidad y unidad de análisis

Asignar cada solicitud de hora extra autorizada a un solo corte de prenómina. La unidad de análisis es una `HoraExtra`; la evidencia de su inclusión en nómina es un `PrenominaMovimiento` de tipo `HORA_EXTRA` ligado a un `PrenominaCorte`.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Solicitud y autorización | `rrhh.HoraExtra` / `rrhh_horaextra` | Detección de asistencia, captura manual y `resolver_hora_extra` | `id`; empleado y fecha trabajada; `fecha_autorizacion_jefe` | Producción: 281–284 siguen pendientes; total por estado: 204 autorizadas, 8 pendientes, 44 rechazadas y 25 canceladas | Bandeja de horas extra, conciliación de asistencia, prenómina |
| Corte operativo | `rrhh.PrenominaCorte` / `rrhh_prenominacorte` | Flujo de prenómina | `id` y `folio`; periodo y `creado_en` | Producción: 1 corte histórico, `PRE-202606-001`, 2026-06-01 a 2026-06-15 | Detalle y exportación de prenómina |
| Asignación al corte | `rrhh.PrenominaMovimiento` / `rrhh_prenominamovimiento` | `recalcular_corte_prenomina` | corte + fuente + tipo; `fuente_modelo=rrhh.HoraExtra` y `fuente_id=<id>` | Producción: 6 movimientos; uno es `HORA_EXTRA` y apunta a HoraExtra 4 | Resumen, detalle y exportación de prenómina |
| Nómina heredada | `rrhh.NominaPeriodo` y `rrhh.NominaLinea` | Flujo de nómina legado | periodo + empleado | Producción: 0 periodos; `aplicar_horas_extra_a_nomina` no tiene llamadores en el grafo actual | Sin consumidor activo comprobado |

## Alias y equivalencias

| Términos o identificadores | Estado: confirmada / candidata / distinta / no resuelta | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Hora extra autorizada / movimiento `HORA_EXTRA` | confirmada | El servicio actual crea el movimiento con `fuente_modelo=rrhh.HoraExtra` y `fuente_id` igual al id de la solicitud | No |
| Quincena pagable / `PrenominaCorte` | confirmada | Es el corte que concentra movimientos y exportación; conserva folio y rango | No |
| `NominaPeriodo` / `PrenominaCorte` | distinta | Son flujos separados; producción no tiene `NominaPeriodo` y el helper correspondiente no tiene llamadores | No fusionar en esta tarea |
| Estado `AUTORIZADO` / pendiente real de pago | no resuelta para históricos | Hay 204 autorizadas y solo un movimiento de prenómina; el estado no demuestra si ya se pagaron fuera del ERP | Sí: no reactivar históricos automáticamente |

## Decisión de diseño

Reutilizar `HoraExtra`, `PrenominaCorte` y `PrenominaMovimiento`. No crear una tabla paralela ni duplicar la captura. Extender `HoraExtra` con una bandera de seguimiento de prenómina cuyo valor inicial en la migración sea verdadero solo para solicitudes pendientes; las autorizadas históricas quedan fuera hasta una regularización explícita. Usar `PrenominaMovimiento` como asignación única y auditable al corte.

Consultas o procedimiento reproducible: `inventario_fuentes_datos --term "hora extra"`, `--term prenomina`, `--term "corte nomina"` y `--term quincena`; conteos acotados en producción dentro de `BEGIN READ ONLY`; grafo de llamadas sobre `resolver_hora_extra`, `_horas_extra_por_empleado`, `recalcular_corte_prenomina` y `aplicar_horas_extra_a_nomina`.

Riesgos y pendientes: el histórico autorizado no permite distinguir lo pagado fuera del ERP. Nunca debe incorporarse en bloque. La inclusión retroactiva requerirá una conciliación separada y aprobación expresa.
