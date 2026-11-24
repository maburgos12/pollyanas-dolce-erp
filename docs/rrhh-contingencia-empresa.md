# Permiso empresarial de contingencia

Operación inicial por comando, con actor de Dirección General activo y simulación
sin escrituras por defecto. No existe autorización implícita para aplicar el caso
21/nov/2026: se presenta primero su simulación y se espera la decisión de Mauricio.

## Fuente y alcance

`PermisoContingencia` registra días completos inclusivos (1–31 días), motivo
`seguridad`, `clima`, `salud` u `otro`, descripción y referencias de evidencia.
`BeneficiarioContingencia` congela IDs y el intervalo laboral acreditado al aprobar.
`EventoContingencia` conserva actor, fecha, revisión, digest y simulación, incluidos
los movimientos automáticos previos que eventualmente se retiran. La fuente y la
bitácora no se editan ni eliminan mediante los servicios; un cambio requiere otra
clave. Una revocación conserva el padrón y la historia. Borrador no exime asistencia.

`empresa` selecciona personal Pollyana con ingreso en el rango y actividad vigente
o baja documentada posterior al inicio. Inactivos sin fecha de baja se enumeran
como excluidos sin inferir su situación. `empleados` exige IDs explícitos válidos y
vigencia acreditada para todos; no resuelve sucursales/nombres ambiguos. Para un
alcance por sucursal, preparar y revisar fuera del comando la lista histórica de
IDs, y enviarla como `empleados`. La selección se congela en la autorización, no
incluye contrataciones posteriores a su instantánea.

El tratamiento salarial `conservar`, `con_goce` o `sin_goce` registra intención por
separado de la clasificación sin sanción. Ninguno cambia salarios, días trabajados,
marcas, horas extra, uniformes o producción. `conservar` no declara goce ni descuento.
Las implicaciones salariales que requieran movimientos deben tramitarse aparte.

## Preparar y revisar

Usar exclusivamente una conexión PostgreSQL autorizada, con la migración
`rrhh.0065_permiso_contingencia_empresa` aplicada. Ejemplo de archivo revisable:

```json
{
  "clave": "CONTINGENCIA-20261121",
  "fecha_inicio": "2026-11-21",
  "fecha_fin": "2026-11-21",
  "motivo": "seguridad",
  "descripcion": "Suspensión empresarial de todas las actividades; evidencia revisada por Dirección.",
  "evidencia": ["referencia-al-acuerdo-de-direccion"],
  "alcance": "empresa",
  "tratamiento_salarial": "conservar"
}
```

```sh
python manage.py permiso_contingencia --archivo /ruta/revisada/contingencia.json --actor ID_DG
```

Revisar beneficiarios, excluidos, conflictos, IDs de faltas a resolver, alertas antes
y después, plan de alertas, movimientos previos, cambios de elegibilidad e importes
de bonos (`bonos_plan`) y digest. La proyección de bonos usa sus registros diarios
y fórmulas, sin guardar modelos ni fabricar cantidades ganadas. La salida no autoriza
aplicación. Las referencias de evidencia no se descargan ni se envían a terceros.

Después de la aprobación expresa del resultado concreto:

```sh
python manage.py permiso_contingencia --archivo /ruta/revisada/contingencia.json --actor ID_DG --aplicar --digest DIGEST_REVISADO
```

`--accion borrador` registra sin eximir. Aprobar esa misma clave congela el padrón
vigente tras otra simulación. Repetir la misma acción y contenido es idempotente.
Revocar requiere una nueva simulación `--accion revocar` y su digest; no reabre una
clave revocada. Una corrección posterior requiere nueva autorización.

## Protección y límites

La aplicación toma bloqueos PostgreSQL por empleado/día antes de bloquear filas,
relee las fuentes y compara el digest. Cambios entre simulación y aplicación
obligan a simular nuevamente. La población concurrente tiene el corte de esa
relectura; no se bloquean todas las tablas de RRHH. Las fuentes incluyen historial
de incidencias de los 29 días anteriores, marcas, permisos, estado financiero y
registros de bonos. No ejecutar operaciones SQL externas para saltar el servicio.

Identidades ambiguas de registros diarios de producción, conflictos manuales de
falta/alertas, nómina cerrada/pagada, prenómina cerrada o
exportada, movimientos exportados y bonos cerrados/pagados bloquean aplicación.
No se reabren ni modifican automáticamente. Los importes manuales de bonos se
conservan bajo bloqueo; solo se sincronizan borradores. Una falta por retardos de
días anteriores conserva su causalidad aunque su fecha agregada coincida con el
permiso. Las marcas existentes mantienen sus revisiones de integridad y extra.

Se resuelven y auditan únicamente faltas automáticas cubiertas; vacaciones y
permisos personales conservan sus propias fuentes. La reparación cronológica
modifica solo avisos/bajas automáticos por faltas hasta `min(hoy, fin+29 días)`.
No se emiten alertas en fechas posteriores a la última evidencia de evaluación
de cada beneficiario. Si hay alertas no resueltas posteriores a esa cota, se bloquea el caso: un cambio en
la fecha de emisión podría alterar su deduplicación y requiere revisión ampliada.

Prenómina retira solo movimientos automáticos no exportados ligados a faltas
justificadas y refresca los resúmenes de los beneficiarios ya presentes en cortes
abiertos, que quedan en revisión. No reconstruye todo el corte ni toca movimientos
manuales. Bonos usan días neutrales para elegibilidad sin registrar trabajo; un
día de descanso o con trabajo real no se suma como neutral. Los reportes conservan
evidencia original y muestran la fuente empresarial.

No incluir el viernes 20/nov por aproximaciones de cierre o refugio del personal.
El horario contextual de 23–29/nov tampoco acredita marcas o trabajo individual.
