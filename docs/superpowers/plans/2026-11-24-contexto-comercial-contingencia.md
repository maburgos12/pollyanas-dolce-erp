# Contexto comercial de contingencia — implementación

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Registrar y mostrar el contexto comercial del viernes 20 y sábado 21/nov/2026 en ventas y BI, sin cambiar ventas Point, cobertura, métricas, RRHH ni horarios.

**Architecture:** Una fuente documental tipada `ventas.ContextoComercial` con fecha, clave única, tipo (afectación parcial reportada/cierre empresarial), título, descripción, evidencia, actor DG y fecha de registro. No contiene importes ni marcas laborales; registro inmutable por servicio y comando idempotentes, preview sin escrituras y aplicación explícita con digest. Lectores canónicos agregan metadatos documentales sin alterar campos existentes. Ventas y BI muestran las notas del período consultado mediante un parcial compartido y fechas visibles.

**Tech Stack:** Django, PostgreSQL16, plantillas existentes y estilos BI; sin nuevo framework, dependencia, servicio o permisos generales.

## Autorización y aceptación

Mauricio: «Sí, completa lo pendiente en ventas y llévalo a producción: registra el contexto del viernes y el cierre del sábado para explicar la caída, sin modificar Point ni confundir el cierre comercial con la salida del personal.» El diseño acotado fue presentado en este chat; ejecutar sin nueva confirmación pasos ya autorizados.

- Viernes20: interrupciones comerciales por inseguridad en Guasave, alrededor de las 19:00; algunas sucursales cerraron antes, identificación y horas exactas pendientes. Varios colaboradores permanecieron resguardados sin poder salir. La hora aproximada es referencia comercial, jamás salida laboral ni autorización de ausencia del viernes. No afirmar cierre total de todas las sucursales.
- Sábado21: suspensión empresarial total de actividades por seguridad, declarada por Dirección; ausencia de actividad reportada. Referencia documental a `CONTINGENCIA-SEGURIDAD-20261121`, sin derivar RRHH de esta anotación ni inferir cero contable de la falta de datos.
- Anotar contexto explicativo; no cuantificar cuánto de la caída se debe al evento ni certificar integridad de carga. Mantener alertas y cobertura técnica, filas, importes, comparativos, pronósticos y todos los datos de Point. Si existen ventas ese día, conservarlas y no sustituirlas por cero.
- Auditoría: fuente y actor verificados; evidencia como lista no vacía de referencias, nunca descargar enlaces. Repetir una clave con mismo contenido no duplica; con otro contenido bloquea. Dos registros se aplican atómicamente; digest incluye estado existente, actor validado de nuevo. Modelo y servicio rechazan cambios/borrados de fuente mediante interfaces soportadas. No crear formulario de edición o nuevas rutas mutables.

## Ficha de fuentes

Base `a8a9c0134a2ad43e504e8522e03661d0d65d40f0`. Grafo revisado: no se identificó anotación comercial existente. `AnalyticAuditLog` es auditoría técnica sin identidad documental estable; `CorteOficialDiario` altera interpretación del corte/importes y no es adecuado. `rrhh.PermisoContingencia` acredita permiso laboral sábado, no evento comercial viernes. Reutilizar su folio solo como referencia textual.

Inventario de fuentes ejecutado en producción READ ONLY para venta/contexto/contingencia; resultados en artefactos de la tarea. Los candidatos léxicos no son equivalencias. `VentaAutoritativaPoint` y `PointSalesDaily*` conservan autoridad de ventas; `ContextoComercial` es evento documental separado.

## Trabajo

### 1. Fuente documental y registro

Archivos: `ventas/models.py`, migración aditiva posterior a0012, `ventas/services/operational_context.py`, `ventas/management/commands/registrar_contexto_comercial.py`, `ventas/tests_operational_context.py`.

- [x] Tras migrar base aislada, escribir pruebas primero: autorización DG, preview cero escrituras, fecha/tipo/evidencia válidos, orden por fecha, idempotencia, conflicto de contenido, digest obsoleto, atomicidad e inmutabilidad. Verificar RED.
- [x] Implementar modelo y servicio mínimo. Serialización JSON debe indicar ámbito de contexto general/empresa y naturaleza informativa; el viernes no es afirmación de afectación individual de cada sucursal. La descripción conserva incertidumbre. No almacenar hora exacta fabricada.
- [x] Comando `--archivo JSON --actor ID` simula por defecto; `--aplicar --digest` exige contenido idéntico al revisado. Actor institucional DG real se selecciona explícitamente en producción. Repetir sin duplicar. Verificar GREEN y migración.

### 2. Lecturas analíticas y presentación

Archivos: `ventas/services/sales_read_service.py`, `reportes/views.py`, `reportes/commercial_analytics.py`, `reportes/templates/reportes/partials/operational_context.html`, `reportes/templates/reportes/ventas.html`, `reportes/templates/reportes/bi.html`, tests acotados de ventas/reportes.

- [x] Tests RED comparan lecturas antes/después: todos los campos numéricos/de fuente/cobertura idénticos al retirar solamente la nueva clave `operational_contexts`; incluir lectura diaria, rango, agrupada y lote, vacíos y datos existentes en sábado.
- [x] Adjuntar lista documental una vez por rango/día en resultados públicos de lector, sin mezclarla con selección de fuente o validación de ceros. Lotes y agrupados propagan el contexto; evitar consulta por producto/sucursal.
- [x] Ventas muestra las notas del mes visible con filtro de mes documental (`contexto_mes=YYYY-MM`) independiente y etiquetado explícito, preservando los parámetros existentes y sin cambiar fechas/cifras de los otros paneles. Validar valor antes de usarlo; error local legible o fallback documentado. No cambiar respuesta de exportaciones numéricas fuera del alcance.
- [x] BI usa el año/mes de su panel comercial existente para filtrar notas, incluso en vista ligera por defecto y detalle. Consultar notas fuera de caché numérica para que un alta documental se vea inmediatamente. Si hay filtro de sucursal, aclarar ámbito general y que el viernes no acredita cierre de cada sucursal.
- [x] Parcial compartido: fecha, título, tipo, descripción, fuente y clave; texto escapado, contenido largo con ajuste de línea, legible en móvil y sin acciones de escritura. Mostrar contexto sin fabricar causalidad cuantitativa, salida laboral ni cero Point. Verificar GREEN.

### 3. Revisiones y validación

- [ ] `check`, `migrate --check`, `makemigrations --check --dry-run`; suites ventas/reportes/consumidores pertinentes PostgreSQL16. Registrar SHA y logs. Pruebas de no modificación de tablas Point y RRHH, y filtros fuera de período. No reducir descubrimiento ni saltar protecciones.
- [ ] Revisión independiente de especificación y después calidad; reproducir y corregir hallazgos. Validar navegador Chromium escritorio/móvil y WebKit, filtros y etiquetas con fixtures sintéticos; nunca enviar correos/mensajes ni refrescar datos operativos como prueba.
- [ ] Commit/PR una tarea, CI `test` actual aprobado antes de merge sin bypass; autorización productiva ya dada. Registrar fuente/narrativa y orden de trabajo fuera del repo.

### 4. Producción y cierre

- [ ] Respaldo previo y comprobación de integridad, verificar base productiva limpia/migraciones y versión actual. Desplegar script oficial sin pull previo y con SHA/base fijados cuando esté disponible.
- [ ] Registrar dos notas mediante comando/servicio, actor institucional ID2 verificado, preview/digest revisados y transacción. Comparar antes/después hashes de ventas Point/indicadores/datos canónicos de20–21 y autorización RRHH, sin exportar valores sensibles. No atribuir cambios concurrentes a esta tarea sin evidencia.
- [ ] Verificar persistencia, idempotencia y contexto en lectores y reportes HTTPS reales, manteniendo importes/fuente/cobertura. Confirmar sábado no produce cero validado ni Friday hora de salida.
- [ ] Revocar sesión temporal de UI, retirar únicamente entorno Docker local propio con respaldo recuperable verificado y helper oficial; no volúmenes productivos. Cerrar rama/worktree mediante ciclo oficial y dejar comprobante productivo.

## Recursos

Worktree `ventas-contexto-contingencia-20261124`, dueño `codex-contexto-ventas`. Artefactos `/Users/mauricioburgos/.codex-task-artifacts/ventas-contexto-contingencia-20261124`. PostgreSQL16 local exclusivo proyecto `erp-ventas-contexto-20261124`, puerto55687, volumen `erp-ventas-contexto-20261124_db_data`, fixtures descartables. No reutilizar el entorno retirado de RRHH ni bases ajenas.

## Validación local de implementación

Fuente/comando y metadata/presentación implementados; evidencia RED/GREEN bajo artefactos.
`check` sin incidencias, migración0013 aplicada a DB propia, `migrate --check` y
`makemigrations --check --dry-run` satisfactorios. Revisión independiente, navegador,
CI, despliegue y cierre siguen a cargo del coordinador. Ningún dato productivo
fue escrito por el implementador.
