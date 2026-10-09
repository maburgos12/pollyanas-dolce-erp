# Ficha de fuentes — Procedencia técnica de bitácora de equipos

Fecha y ambiente consultado: 2026-10-05; catálogo de código local en base 463a76cb,
PostgreSQL 16.11 aislado, y consultas de producción exclusivamente de lectura
realizadas por el responsable del hilo antes de implementar.

## Necesidad y unidad de análisis

Un origen representa un servicio en un archivo exacto: SHA256 de los bytes,
hoja real (CSV usa `CSV`), primera fila física y slot 1/2. La revisión selecciona
un equipo existente por ID y decide crear un trabajo o relacionar uno existente.
No interpreta nombres como identidad, no captura maestros ni modifica historia.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Equipo maestro | activos.Activo | Catálogo nativo | PK y sucursal | Producción: 196 | Activos, Mantenimiento, QR |
| Trabajo | activos.OrdenMantenimiento | Capturas nativas | PK/folio, activo_ref | Producción: 63 | Activos, Mantenimiento, reportes |
| Evento | activos.BitacoraMantenimiento | Capturas/acciones nativas | PK/orden | Producción: 63 | Historial |
| Reintento directo | mantenimiento.ComprobanteCapturaEquipo | Motor capturar_equipo_autorizado | Usuario/operación/UUID | Producción: 0 | Capturas web/API |
| Fallas | fallas.ReporteFalla / BitacoraFalla | Reportes nativos | PK y sucursal | Producción: 110 / 225 | Mantenimiento |
| Fuentes económicas distintas | Gasto / Obligacion / CFDI / Bank | Finanzas y conciliación | IDs nativos | Producción: 1338 / 172 / 9648 / 13555 | No se escriben desde este importador |

Parcialidad y Pago en las fuentes inspeccionadas: 0; esto no demuestra ausencia
de pagos en otras fuentes. Ningún importe de mantenimiento acredita pago.

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| nombre/marca/modelo/serie del archivo y del catálogo | Candidata | Nombres y series pueden repetirse; no identifican por sí mismos | Operador selecciona PK existente |
| archivo exacto/hoja/fila/slot | Confirmada técnica | Bytes SHA256, coordenada física y slot; archivo corregido cambia identidad | Nueva revisión del archivo corregido |
| ComprobanteCapturaEquipo y origen de archivo | Distinta | UUID por usuario no garantiza identidad global entre web/CLI/actores | Tabla técnica independiente |

## Decisión de diseño

Reutilizar el parser, formatos, plantilla y exclusiones de `bitacora_import`,
`actor_actual`, `validar_equipo`, `authorized_orders`, `can_view_costs` y el motor
`capturar_equipo_autorizado(clave=None)`. Crear sólo una tabla técnica aditiva
`OrigenImportacionBitacora` con UNIQUE de identidad global, huella de la revisión,
autor, equipo y orden originales y FK de orden nullable. La eliminación de una
orden conserva el origen como tombstone. No se alteran las 63 órdenes históricas.
La tabla no es otra captura de equipos, costos, pagos ni proveedores.

Consultas o procedimiento reproducible: `inventario_fuentes_datos --term
importacion --term bitacora --term procedencia --term equipo --term mantenimiento`
(ejecutado por el responsable, evidencia `inventario-fuentes.log`); 12 tablas,
conteo y MD5 de todos los campos en `integridad-produccion-inicial.json`, script
readonly con timeout 15 s. Evidencia externa en
`/Users/mauricioburgos/Downloads/analisis-administracion-activos-2026-09-30/entorno-origen-importacion-20261005`.

Riesgos y pendientes: publicación y validación de producción corresponden al
responsable del hilo; no ejecutar confirmaciones reales en producción. Vista
previa y revisión no persisten ni hacen rollback de escrituras. Un costo vacío,
inválido o no finito queda desconocido; se permite vincular una orden sin alterar
su importe, y no se permite crear trabajo con importe desconocido.

## Operación y límites verificados

- Web: Obtener vista previa → seleccionar PK de equipo y crear/vincular →
  Revisar decisiones → confirmar. Se muestran 100 servicios por página y la
  revisión reúne las selecciones de todas las páginas. Pendientes y descartados
  no generan registros. Volver a las decisiones conserva selección, motivo,
  evidencia y archivo exacto.
- Los bytes/hoja y borradores viajan en tokens firmados, sin sesión ni base de
  datos. Sólo el token emitido por una revisión válida autoriza Confirmar. El
  TTL es 24 horas; una revisión vencida conserva archivo/selecciones y obliga
  otra revisión. Una firma alterada se rechaza.
- Fuente: hasta 2 MB, 1000 filas físicas (CSV) / filas de hoja (XLSX), 32 columnas
  XLSX y 30 MB descomprimidos. La revisión firmada completa tiene un techo de
  900 KiB (anunciado hasta 1 MB), con margen bajo el límite nativo de POST de
  Django. El formulario de revisión también usa multipart en el fallback sin
  JavaScript. Si no cabe, dividir produce archivos distintos: cambia SHA256 y
  requiere revisión nueva; no conservar identidades del archivo anterior.
- Motivo/evidencia: 2000 caracteres tanto web como CLI. Los errores JSON
  mantienen el DOM y el snapshot nativo; los errores HTML mantienen el archivo
  firmado y decisiones válidas disponibles. No se reinicia el catálogo ni los
  parámetros de consulta. Los fingerprints no se muestran al operador.
- Revocación de módulo/ámbito con sesión activa: JSON 403 sin detalles del
  resultado. Sesión anónima o cuenta desactivada: se conserva el 302 al login
  nativo de `@login_required`; `data-capture-snapshot` evita navegar el formulario
  ante una respuesta sin confirmación JSON. Renovar sesión en otra pestaña y
  reintentar; el servicio vuelve a consultar actor/permisos.
- Escritura nativa: `crear_servicio_movil` / `capturar_equipo` ya registran el
  importe de la captura bajo `validar_equipo`. `can_view_costs` filtra lectura,
  especialmente respuestas de reintento; no se añade como permiso de escritura.
  La importación conserva su gate existente de gestión de Inventario/Activos,
  más el permiso y ámbito actuales del mantenimiento al aplicar cada decisión.
- CLI sin `--apply` es sólo lectura. Aplicar exige `--apply --actor ID
  --decisiones revision.json` (y `--sheet` cuando corresponda). El JSON debe ser
  objeto con `archivo_sha256`, `hoja`, `revisado: true` y una lista `decisiones`
  con fila/slot/accion/activo_id/orden_id/motivo/evidencia; la huella/hoja se
  verifican contra el archivo exacto actual. No hay modo heurístico de apply.
- La tabla técnica tiene UNIQUE global; destinos se bloquean por PK en orden
  estable, y permisos se releen después de esperas. Un fallo revierte órdenes,
  bitácoras, procedencias y auditoría de todo el lote. El reintento exacto no
  genera otra auditoría ni actualiza autor, evidencia o trabajo.

QA de navegador y publicación: a cargo del responsable del hilo. No ejecutar
POST de confirmación en producción. El shell SW existente hace siempre fetch
para estas páginas; sus únicos recursos instalados son manifest e iconos. No se
modifica caché ni registro global por esta ampliación del template autenticado.
