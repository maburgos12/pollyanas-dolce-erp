# Bitácora de contexto comercial

`ventas.ContextoComercial` conserva declaraciones documentales de empresa por
fecha. No es una fuente de importes ni acredita cobertura Point, ceros, causa
cuantificada de variaciones, cierre individual de sucursal o salida del personal.
RRHH conserva sus propios permisos; una clave RRHH en evidencia es solo referencia.

El alta exige usuario activo autorizado por el predicado existente de Dirección
General. No hay edición ni borrado por modelo, queryset, comando o admin. Una
aclaración requiere otra clave documental; la misma clave con otro contenido se
rechaza. SQL administrativo directo queda fuera de estas interfaces.

El comando recibe una lista JSON con exactamente estos campos por entrada:

```json
[
  {
    "clave": "EJEMPLO-CONTEXTO-20261120",
    "fecha": "2026-11-20",
    "tipo": "afectacion_parcial",
    "titulo": "Afectación comercial declarada",
    "descripcion": "Descripción sustentada; sin atribuir horas o sucursales no confirmadas.",
    "evidencia": ["Declaración de Dirección General, fecha y referencia verificable"]
  }
]
```

Tipos: `afectacion_parcial` o `cierre_empresa`. Evidencia debe ser una lista no
vacía de referencias textuales. No se descargan enlaces ni se envían mensajes.
El JSON anterior ilustra el contrato; no es un registro operativo aprobado.

```sh
python manage.py registrar_contexto_comercial --archivo /ruta/contextos.json --actor ID_DG
python manage.py registrar_contexto_comercial --archivo /ruta/contextos.json --actor ID_DG --aplicar --digest DIGEST_DE_LA_VISTA_PREVIA
```

El primer paso es solo lectura, compatible con transacciones PostgreSQL
REPEATABLE READ/READ ONLY. El digest enlaza actor, contenido y estado persistido
de las claves del lote. Aplicar vuelve a validar todo dentro de una transacción,
serializa altas entre actores con un advisory lock transaccional y exige digest
fresco. Un cambio de estado exige otra vista previa. Repetir una clave idéntica
tras una nueva vista previa es idempotente. Un conflicto invalida el lote completo.

En pruebas locales de esta tarea, ejecutar exclusivamente el wrapper registrado
`/Users/mauricioburgos/.codex-task-artifacts/ventas-contexto-contingencia-20261124/manage.sh`.
El despliegue y la captura productiva siguen el procedimiento del proyecto.

Los cuatro lectores públicos de ventas agregan `operational_contexts` después de
seleccionar la fuente. Las notas no intervienen en selección, cálculos,
pronósticos ni cobertura. Se consulta una vez por rango/día; bulk y grouped
propagan la documentación sin consultas por producto o sucursal.

Ventas muestra el mes del corte visible, o el actual si falta corte. El filtro
`contexto_mes=YYYY-MM` afecta exclusivamente el período de la bitácora y conserva
los demás parámetros. Un valor inválido muestra explicación y el mes por defecto.
BI usa `sales_year`/`sales_month` (0: todo el año), aun al filtrar sucursal; las
notas se leen fuera del caché numérico de 300 segundos y son generales de empresa.

Fuentes revisadas: declaración de DG para el viernes 20 y sábado 21 de noviembre
de 2026; autorización RRHH `CONTINGENCIA-SEGURIDAD-20261121` solo como referencia.
No existe equivalencia con `CorteOficialDiario` (importes/interpretación de corte)
ni `AnalyticAuditLog` (auditoría técnica). El inventario y el cotejo productivo de
solo lectura están en los artefactos de la tarea; no se copian datos personales.
