# Reglamento de Asistencia y Automatización — Pollyana's Dolce

**Versión 1.2 · Ratificado por Dirección General el 11-jun-2026 · Actualizado el 27-jul-2026**

Aplica a todo el personal operativo de las 9 sucursales. Fuente técnica: motor de
reglas del ERP (`rrhh/services_asistencia_reglas.py`). Este documento es la
referencia oficial: cualquier cambio al motor de asistencia debe partir de aquí y
actualizarse aquí en el mismo PR.

---

## I. Definiciones

- **Checada**: registro de entrada o salida capturado por el checador (Hikvision o Point).
- **Turno**: horario asignado al empleado, con hora de entrada, hora de salida y tolerancia en minutos.
- **Tolerancia**: margen de **10 minutos** posteriores a la hora de entrada. Es un
  escudo disciplinario: evita la sanción formal, **no** otorga puntualidad.
- **Incidencia**: registro automático que el motor genera por cada evento. Estados:
  *pendiente* (cuenta para sanción), *conciliada* (justificada) o *resuelta* (sin efecto).
- **Ventana móvil**: todo acumulado se analiza desde el día evaluado hacia atrás
  (7 días para tolerancia, 15 para retardos, 30 para faltas). No se reinicia por
  quincena ni por mes.
- **Episodio**: cada cruce de umbral dentro de una ventana genera **una sola**
  incidencia de escalamiento, no una por día.

## II. Puntualidad y tolerancia

- **R-01.** Un día es *puntual* únicamente si el empleado checa a su hora exacta o
  antes. Un minuto tarde ya rompe la puntualidad del día, aunque quede dentro de tolerancia.
- **R-02.** Llegar entre 1 y 10 minutos tarde registra un **uso de tolerancia**:
  sin sanción formal, pero el día pierde puntualidad para el bono.
- **R-03.** Tres usos de tolerancia en una ventana móvil de **7 días** equivalen a
  **un retardo**. Cada tres usos adicionales dentro de la ventana generan otro
  (6 usos = 2 retardos). *(Actualizado el 27-jul-2026: la ventana original de 15
  días se redujo a 7 por instrucción de Dirección General.)*

## III. Retardos y llegadas tarde

- **R-04.** Llegar después de la tolerancia (más de 10 minutos) **sin permiso
  aprobado** se registra como **falta**, aunque el empleado haya trabajado el día.
- **R-05.** Llegar tarde **con permiso de ingreso aprobado** se registra como
  retardo conciliado: sin sanción, pero el día no es puntual.
- **R-06.** Tres retardos en ventana móvil de **15 días** equivalen a **una falta
  por retardos**. Los retardos derivados de tolerancia (R-03) sí cuentan para esta
  escala. *(Ventana ratificada en 15 días por Dirección General el 27-jul-2026:
  la reducción a 7 días aplica únicamente a la acumulación de tolerancias, R-03.)*

## IV. Faltas

- **R-07.** Todo día laborable sin checada genera una **falta**, salvo causa
  justificada (sección VII).
- **R-08.** Día laborable es todo día excepto domingo y los descansos oficiales de
  ley. *(Limitación vigente: el sistema aún no reconoce el día de descanso
  individual de cada empleado; ver sección XII.)*

## V. Escalamiento disciplinario

- **R-09.** Tres faltas reales en ventana de 30 días generan un **aviso de riesgo
  de baja** (uno por episodio).
- **R-10.** Cuatro faltas reales en 30 días generan la **marca de baja por
  política interna** (una por episodio).
- **R-11.** Para efectos de despido justificado **solo cuentan faltas reales
  injustificadas**. Las faltas por retardos (castigo interno) y las suspensiones
  quedan excluidas, conforme al Art. 47 LFT.

## VI. Comida, jornada y horas extra

- **R-12.** El tiempo de comida es de **35 minutos exactos**. El minuto 36 genera
  incidencia de comida excedida con los minutos de exceso.
- **R-13.** Trabajar menos minutos que la jornada del turno genera incidencia de
  **jornada incompleta**; se concilia automáticamente si existe permiso que cubra
  la ausencia.
- **R-14.** La hora extra detectada por checador queda **pendiente de autorización
  del jefe directo**; sin autorización no procede. Pendientes de más de 24 horas
  generan alerta por correo.

## VII. Ausencias justificadas

- **R-15.** No generan falta: vacaciones aprobadas **o en trámite con días
  reservados**, permisos de salida aprobados, incapacidades registradas,
  suspensiones activas y empleados marcados **exentos de checador** (trabajo
  remoto u oficina sin checador, con motivo registrado).
- **R-16.** Si la justificación se cancela (vacación rechazada, permiso revocado),
  el motor re-evalúa el rango y la falta reaparece automáticamente.

## VIII. Suspensiones disciplinarias

- **R-17.** El día de suspensión se registra como incidencia conciliada: **no**
  cuenta para despido, pero para el bono el empleado pierde asistencia y
  puntualidad de ese día.
- **R-18.** La suspensión puede ser con o sin goce de sueldo, y solo la aplican el
  jefe directo o Capital Humano.

## IX. Efecto en bonos

Todas las reglas de este documento aplican a los bonos (ratificado el 27-jul-2026).

- **R-19.** El checador alimenta automáticamente los bonos: falta pendiente = día
  sin asistencia; cualquier llegada tarde (uso de tolerancia, retardo o falta) =
  día sin puntualidad; suspensión = pierde ambos.
- **R-20.** La sincronización solo reconstruye bonos en estatus *borrador*. Los
  campos capturados por las jefas (`bono_extra`, ajustes) son **intocables** para
  todo proceso automático.
- **R-21.** La falta por retardos (R-06) es una salida disciplinaria de RRHH; el
  bono no la descuenta como día sin asistencia porque ya penaliza los retardos con
  sus propios límites por área (`limite_puntualidad`, `limite_retardos_cancelacion`).

## X. Ediciones y auditoría

- **R-22.** El jefe directo y Capital Humano pueden editar cualquier incidencia.
  Toda edición exige **comentario obligatorio** y queda en bitácora (quién, qué
  campo, valor anterior y nuevo).
- **R-23.** Una incidencia editada a mano **nunca** es modificada ni resuelta por
  el motor.

## XI. Automatizaciones vigentes

| # | Proceso | Momento |
|---|---|---|
| A-1 | Evaluación de reglas al llegar cada checada (Hik y Point) | Tiempo real |
| A-2 | Barrido diario: faltas de quien no checó el día anterior | 7:00 am |
| A-3 | Re-evaluación al aprobar/rechazar permisos, vacaciones, suspensiones, incapacidades | Al momento |
| A-4 | Sincronización de asistencia/puntualidad a bonos | Tiempo real + reconciliación 2:40 pm |
| A-5 | Auditoría de saldos de vacaciones con correo si hay hallazgos | 7:30 am |
| A-6 | Alerta de horas extra sin autorizar | Diaria |
| A-7 | Reporte de asistencia con filtros, KPIs por empleado, detalle diario, edición inline y export CSV/XLSX | A demanda de Capital Humano |

## XII. Estado de activación

### Reglas operando hoy
Faltas por día sin checada (R-07), escalamiento disciplinario (R-09 a R-11),
comida excedida (R-12), ausencias justificadas (R-15, R-16), suspensiones
(R-17, R-18), efecto en bonos por faltas (R-19, R-20), ediciones y bitácora
(R-22, R-23) y todas las automatizaciones de la sección XI.

### Reglas que nunca han operado
Puntualidad, tolerancia, retardos, jornada incompleta y hora extra automática
(R-01 a R-06, R-13, R-14). **No fueron desactivadas: nunca llegaron a
encenderse**, porque dependen de una configuración que quedó pendiente desde que
el módulo se construyó en junio de 2026.

**Cadena de dependencia (diagnóstico del 27-jul-2026):**

1. El motor solo puede juzgar una llegada si la checada tiene **turno asignado**;
   sin turno no existe hora de referencia y las reglas de entrada se omiten.
2. El turno se asigna automáticamente solo si el catálogo tiene **turnos activos**,
   eligiendo el turno cuya hora de inicio esté más cerca de la hora en que la
   persona checó (tolerancia de búsqueda: ±90 minutos).
3. El catálogo nunca se pobló con los turnos reales por área: solo existen dos
   turnos genéricos (General 8:00–16:00, Logística 9:00–17:00), ambos inactivos.
4. Resultado medible: de 3,803 checadas registradas entre mayo y julio de 2026,
   solo 9 tienen turno asignado (0.2%). Las incidencias que dependen del turno
   —uso de tolerancia, retardo por tolerancia, jornada incompleta y hora extra
   pendiente— no existieron en la base hasta el 27-jul-2026. Las que no dependen
   del turno (faltas, comida, avisos, suspensiones) operan sin interrupción desde
   el 11-jun-2026.

**Defecto de diseño a corregir antes de encender:** hoy el turno se deduce de la
hora en que la persona checó, no del horario que le corresponde. Con dos turnos
separados por una hora, quien entra a las 8:00 y llega a las 9:00 sería asignado
al turno de las 9:00 y quedaría registrado como puntual. El modelo `Empleado` no
tiene campo de turno ni horario asignado; el turno vive únicamente en cada
registro de asistencia.

### Pendientes de decisión de Dirección
1. **Turno por empleado**: crear el horario asignado por persona (o por
   puesto/área) para que el turno deje de deducirse de la hora de llegada.
2. **Catálogo real de turnos** por área, con horas y tolerancia correctas.
3. **Día de descanso individual** por empleado y tratamiento del domingo (hoy
   el sistema solo excluye domingos y descansos oficiales, lo que genera faltas
   falsas a quien descansa entre semana).
4. **R-04**: ratificar si llegar más de 10 minutos tarde sigue siendo falta
   completa o debe bajar a retardo.
