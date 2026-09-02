# Entrega local: gastos recurrentes y personal

Fecha: 2026-09-02. Estado: implementado y verificado localmente, pendiente de revisión final, commit/PR, despliegue oficial y validación con fuentes reales completas. No equivale a Rentabilidad resuelta en producción.

## Versión vigente para revisión de Codex

- Worktree: `/Users/mauricioburgos/Downloads/codex_worktrees/rentabilidad_gastos_publicacion`.
- Rama: `codex/rentabilidad-gastos-publicacion`.
- Base: `1bb263b579566ce228cdca79e6a467df61b569b4`.
- Codex asume revisión y eventual publicación conforme al protocolo de PR #1247. Claude no es requisito.
- Traslado mecánico de 9 archivos modificados y 12 nuevos; origen intacto. Se conserva `reportes/tests_operating_finance.py` de la nueva base y no se copian instrucciones antiguas de AGENTS.md/CLAUDE.md.
- Esta preparación no carga datos operativos y no incluye commit, PR de Rentabilidad ni despliegue. La evidencia de navegador y fuentes reales que sigue es histórica, no una validación nueva de producción.

## Versión histórica conservada como respaldo

- Worktree anterior: `/Users/mauricioburgos/Downloads/codex_worktrees/rentabilidad_gastos_recurrentes_actualizada`.
- Rama: `codex/rentabilidad-gastos-recurrentes-actualizada`.
- Base: `2199ba2f690f303c26e920f8394163d736a98485` (incluye el cierre Point incorporado durante esta implementación).
- Todo el cambio está sin confirmar. No se creó PR ni se publicó código.
- Respaldo previo conservado: worktree `rentabilidad_gastos_recurrentes` y stash `a3d7daad946357e1703bc2e984d6bf8fc5b9b2e6`, titulado `resguardo-rentabilidad-recurrentes-base-20260902`. No integrar ambas copias ni aplicar el stash otra vez en la rama vigente.
- El traslado se hizo con el script de inicio y preflight sobre rama limpia. No hubo archivos en conflicto con los 84 commits incorporados en main. Se repitieron migraciones, pruebas y navegador en la base nueva.

## Cambios funcionales

1. Lectura mensual compartida por tarea y pantalla: gastos/obligaciones, reglas de reparto, nómina cerrada/pagada y cargas patronales. No escribe datos desde GET.
2. Deduplicación de obligación/gasto enlazados, espejos, versiones y total/componentes de nómina. Los pagos son evidencia, no un segundo costo.
3. Cobertura mensual opcional y periodicidad mensual/bimestral en Reportes, migración aditiva 0047. Conserva el importe original, fechas de pago y centavos. Impide inicio bimestral a mitad de mes, cambio que corte un ciclo y solapamientos.
4. Matriz conserva el proporcional de renta existente. La factura correcta del complejo es $66,210.12; no se fija otro importe ni un porcentaje general en código.
5. Fuentes incompletas producen advertencias y PE no calculable. Se conservan importes anteriores cuando no puede reconstruirse de forma segura un componente; no se presentan como confirmados.
6. Detalle por concepto con monto original, cobertura, costo del mes, regla/vigencia, soporte y pendientes. No se exponen nombres ni salarios individuales. Tabla desplazable accesible en móvil con estilos existentes.

Archivos principales: tres servicios `reportes/services_rentabilidad_*.py`; modelos/servicio de gastos y formularios existentes de Reportes; modelos, tarea, vistas y plantillas de Rentabilidad; migración 0047 y cuatro módulos de pruebas nuevos.

## Revalidación local sobre la base de publicación

Entorno PostgreSQL 16 aislado: proyecto Docker `erp_rentabilidad_publicacion`, compose temporal `/tmp/erp-rentabilidad-publicacion-QEaa2A/compose.yml`. Base local vacía previamente migrada; no se incorporaron datos de producción ni se cargaron fixtures operativos para esta comprobación. Los datos de los tests permanecen exclusivamente en la base de pruebas.

```bash
docker compose -p erp_rentabilidad_publicacion -f /tmp/erp-rentabilidad-publicacion-QEaa2A/compose.yml exec -T db pg_isready -U postgres -d pastelerias_erp
docker compose -p erp_rentabilidad_publicacion -f /tmp/erp-rentabilidad-publicacion-QEaa2A/compose.yml run --rm check manage.py migrate reportes 0047
docker compose -p erp_rentabilidad_publicacion -f /tmp/erp-rentabilidad-publicacion-QEaa2A/compose.yml run --rm check manage.py check
docker compose -p erp_rentabilidad_publicacion -f /tmp/erp-rentabilidad-publicacion-QEaa2A/compose.yml run --rm check manage.py migrate --check
docker compose -p erp_rentabilidad_publicacion -f /tmp/erp-rentabilidad-publicacion-QEaa2A/compose.yml run --rm check manage.py makemigrations --check --dry-run
docker compose -p erp_rentabilidad_publicacion -f /tmp/erp-rentabilidad-publicacion-QEaa2A/compose.yml run --rm check manage.py test reportes.tests_rentabilidad_gastos reportes.tests_rentabilidad_personal reportes.tests_cobertura_gastos reportes.tests_gastos_compromisos reportes.tests_presupuesto_real rentabilidad --keepdb
```

- `pg_isready`: accepting connections. `reportes.0047_cobertura_gastos_recurrentes`: OK, aplicada solo localmente.
- `check`: 0 incidencias. `migrate --check`: salida 0. `makemigrations --check --dry-run`: No changes detected.
- 232 pruebas: OK en 10.713 s, salida 0; base de pruebas conservada por `--keepdb`.
- Advertencias: catálogo local vacío sin algunas recetas/equivalencias durante preparación de la base; directorio `/app/staticfiles/` ausente en el entorno de tests. Las respuestas 400/403/409 y trazas de `PermissionDenied` corresponden a los casos negativos de la suite, sin fallos de pruebas.
- Comparación `cmp`: los 18 archivos de código, plantillas, migración y pruebas trasladados son idénticos al origen. Solo se actualizaron los tres documentos de especificación/plan/evidencia. `reportes/tests_operating_finance.py`, `AGENTS.md` y `CLAUDE.md` no tienen cambios frente a la nueva base.
- `git diff --check`: limpio. `git worktree prune --dry-run`: sin elementos reportados. No hubo correcciones de lógica en este traslado.
- Revisión independiente del diff y validación de navegador de la nueva base quedan a cargo del responsable principal. No se revalida ni declara producción desde esta evidencia local.

## Revisión final y navegador de publicación

Codex repitió independientemente las 232 pruebas: OK en 10.808 s, salida 0. Revisiones independientes de especificación y calidad aprobadas, sin nuevos P1/P2 al integrar sobre `1bb263b5`.

En navegador real integrado, con usuario y datos DEMO exclusivamente locales en `127.0.0.1:8895`, se verificaron resumen, pestaña Gastos y detalle: renta $13,242.00, nómina $1,200.00, cargas patronales $100.00 y CFE original $100.01 junio–julio, con $50.00 en junio. Se mostró PE «No calculable» y asignación histórica de nómina provisional. Inspección visual del detalle realizada. En ancho móvil 390px, documento 390px y región de tabla 362px con contenido desplazable 940px; viewport restaurado. Se observaron 49 eventos de red del reporte, sin fallos ni HTTP >=400, y consola sin errores/advertencias.

También se envió el formulario de gasto fijo con un concepto ficticio CFE bimestral de $100.01 y se confirmó su aparición y frecuencia Bimestral en la lista. No se generaron pagos ni se usaron datos operativos reales. Estos fixtures se agregaron después de preparar el entorno de pruebas aislado.

La sesión de producción permanece en login a la espera del acceso del usuario; esta evidencia no certifica la pantalla productiva. No hay cambios de datos de producción por la validación local.

## Evidencia técnica histórica

Entorno PostgreSQL 16 aislado: proyecto Docker `erp_rentabilidad_actualizada`, puerto local 55493, volumen propio. Compose temporal: `/tmp/erp-rentabilidad-recurrentes-t9vwXc/actualizada.yml`. No se modificó el compose ni variables del ERP real. La base de pruebas se creó a partir de la base local recién migrada, antes de incorporar los datos DEMO.

Comandos reproducibles (verificar `pg_isready` antes de cada sesión Django):

```bash
docker compose -p erp_rentabilidad_actualizada -f /tmp/erp-rentabilidad-recurrentes-t9vwXc/actualizada.yml exec -T db pg_isready -U postgres -d pastelerias_erp
docker compose -p erp_rentabilidad_actualizada -f /tmp/erp-rentabilidad-recurrentes-t9vwXc/actualizada.yml run --rm check manage.py check
docker compose -p erp_rentabilidad_actualizada -f /tmp/erp-rentabilidad-recurrentes-t9vwXc/actualizada.yml run --rm check manage.py migrate --check
docker compose -p erp_rentabilidad_actualizada -f /tmp/erp-rentabilidad-recurrentes-t9vwXc/actualizada.yml run --rm check manage.py makemigrations --check --dry-run
docker compose -p erp_rentabilidad_actualizada -f /tmp/erp-rentabilidad-recurrentes-t9vwXc/actualizada.yml run --rm check manage.py test reportes.tests_rentabilidad_gastos reportes.tests_rentabilidad_personal reportes.tests_cobertura_gastos reportes.tests_gastos_compromisos reportes.tests_presupuesto_real rentabilidad --keepdb
```

- 232 pruebas: OK en 12.173 s sobre base actualizada. Incluyen SIPARE, presupuesto, permisos, GET sin escrituras, centavos, duplicados, vigencias y conservación de importes parciales.
- `check`: sin incidencias; `migrate --check`: 0 pendientes; `makemigrations --check --dry-run`: sin cambios detectados; `git diff --check`: sin errores.
- RED/GREEN observado para los dos defectos de ciclo bimestral señalados en revisión; 13 pruebas de cobertura pasan.
- Revisión independiente de especificación: conforme. Revisión independiente de calidad: dos P2 corregidos y aprobados; sin bloqueantes restantes por inspección.
- Advertencia local de directorio staticfiles ausente durante tests; los recursos estáticos sí se comprobaron en navegador con el servidor de desarrollo. Respuestas 403/400/409 durante tests son casos negativos esperados.

## Navegador local

Autenticación con cuenta DEMO, dashboard de junio, pestaña Gastos, detalle y formulario existente. En la base anterior se probó alta bimestral mediante el formulario y se confirmó su aparición. En la base actualizada se repitieron dashboard/detalle: renta DEMO $13,242.00, nómina $1,200.00, cargas $100.00 y CFE original $100.01 junio–julio con costo de junio $50.00. Son fixtures de prueba, no cifras certificadas del negocio.

Se comprobó PE «No calculable» y advertencia de asignación histórica de RRHH. En móvil de 390px, la región mide 362px, desplaza su tabla de 940px y el documento no desborda. El tamaño del navegador se restauró. En la navegación final: 17 respuestas de recursos, ningún fallo de carga/HTTP >=400 y consola sin errores/advertencias. Rentabilidad no tiene SW propio y el SW global inspeccionado no cachea estas páginas HTML; no se modificó el contrato global de caché.

## Fuentes reales: consulta de producción de solo lectura

Se consultó PostgreSQL mediante `BEGIN READ ONLY`; no se ejecutó la migración ni se copiaron archivos al VPS.

| Mes 2026 | Registros GastoOperativoMensual REAL | Centros |
|---|---:|---:|
| Enero | 134 | 10 |
| Febrero | 155 | 10 |
| Marzo | 136 | 10 |
| Abril | 133 | 9 |
| Mayo en adelante | 0 | 0 |

Los nueve centros de sucursal con gastos tienen último mes abril: Matriz 70 registros enero–abril, Payán 54, Colosio 57, Crucero 58, El Túnel 59, Guamúchil 33, Las Glorias 59, Leyva 60 y Plaza Nío 59. Existen centros alternos `SUC_*` sin gastos; no deben mezclarse por nombre ni copiarse importes automáticamente. Contratos recurrentes: 0; obligaciones de gasto: 0.

La ausencia anterior describe esas tablas, no afirma que el gasto no exista en facturas, bancos, capturas manuales u otros documentos. La integración puede leer líneas manuales trazables, pero no inventa enlaces factura–sucursal. RRHH conserva la sucursal actual del empleado, no una asignación histórica inequívoca en cada línea de nómina: se muestra explícitamente provisional. Deben verificarse también cédulas patronales y cobertura de recibos antes de certificar cada mes.

## Siguiente responsable y límites

La decisión final y ejecución de commit/PR/merge/deploy corresponde a Codex dentro de la autorización de Mauricio, conforme al protocolo actualizado por PR #1247. La entrega histórica a `claude_revision_final` no es un requisito vigente. Antes de publicar: revisar diff completo (incluye archivos sin seguimiento), revalidar la base vigente, revisar/aplicar 0047 mediante el flujo oficial, desplegar con `scripts/deploy_web_safe.sh` y comprobar el ERP autenticado. El preflight inicial del nuevo worktree se completó limpio antes del traslado; no debe confundirse el cambio propio pendiente de revisión con suciedad ajena.

Queda pendiente la conciliación histórica detallada por registro, sucursal, periodo e importe para los gastos faltantes. Esta entrega contiene diagnóstico de cobertura, no una propuesta aprobada de carga masiva. No altera nóminas, bonos, facturas, pagos, OneDrive ni datos operativos de producción. El criterio correcto sigue siendo $66,210.12 para el complejo y el proporcional documentado para Matriz–Ventas.

Se preservan las dos bases locales aisladas y el respaldo para reproducción. Los servidores de prueba se detienen al entregar; no se eliminan datos ni artefactos ajenos.
