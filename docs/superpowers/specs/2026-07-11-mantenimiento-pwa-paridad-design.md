# Paridad operativa entre Mantenimiento ERP y PWA

## Objetivo

La PWA de Mantenimiento debe presentar la información operativa que ya existe en el ERP sin reemplazar sus fuentes de verdad. El usuario podrá reconocer qué está pendiente, qué se atendió, quién intervino, cuándo ocurrió y qué evidencia respalda el trabajo.

## Problemas confirmados

- La PWA calcula `cerrados` sobre una bandeja cuyo backend devuelve exclusivamente registros abiertos; por eso siempre muestra cero.
- El payload de reportes entrega únicamente un resumen y el último comentario. Omite la cronología y evidencias de seguimiento.
- El historial está fragmentado entre órdenes, reparaciones y servicios de unidades, y la PWA muestra solamente unos pocos elementos sin filtros ni detalle.
- Los serializers de servicios de unidad omiten autoría, sucursal, factura y fecha de captura.
- Los trabajos realizados sin reporte previo existen, pero no se distinguen claramente en el historial.

## Fuentes de verdad

- `fallas.ReporteFalla`: reporte inicial, foto, prioridad, sucursal, activo, reportante y fechas del flujo.
- `fallas.BitacoraFalla` y `fallas.EvidenciaSeguimientoFalla`: comentarios, cambios de estado, usuarios y evidencias.
- `activos.OrdenMantenimiento`, `activos.BitacoraMantenimiento` y `activos.EvidenciaOrden`: órdenes, seguimiento, costos, responsables y archivos.
- `logistica.ReparacionUnidad` y `logistica.ServicioRealizadoUnidad`: reparaciones y servicios de flota.
- Autenticación/RRHH: identidad visible de los usuarios. No se creará un registro paralelo de personas.

## Experiencia en la PWA

### Inicio y conteos

La pantalla inicial conservará una bandeja compacta de trabajo abierto. Los indicadores serán calculados por el backend sobre consultas completas, no sobre la lista paginada:

- Abiertos.
- En proceso.
- Críticos.
- Cerrados en el periodo seleccionado.

`Cerrados` usará los últimos 30 días por defecto. Al tocarlo abrirá el historial filtrado. El selector de periodo ofrecerá: esta semana, mes actual, últimos 30 días, últimos 90 días y todo.

Los indicadores de abiertos, en proceso y críticos representan el inventario operativo actual y no se limitan por periodo. Sí respetan origen y sucursal autorizada. Cerrados respeta origen, sucursal autorizada y periodo; no incluye cancelados. Cada listado abierto desde un indicador debe usar exactamente la misma semántica que su conteo.

### Tarjetas

Cada tarjeta mostrará título o folio, origen, estado, prioridad, sucursal, activo o unidad, responsable y fecha relevante. Una miniatura aparecerá solo cuando exista una foto inicial útil. La lista no descargará galerías completas.

### Detalle de reporte

El reporte se abrirá en una pantalla propia dentro de la PWA, no en un modal. Contendrá:

1. Encabezado con estado, prioridad, sucursal y folio.
2. Reporte inicial con foto, descripción, reportante, categoría, área, activo y fecha/hora.
3. Fechas de asignación, resolución y cierre cuando existan.
4. Personas asignada y responsable del cierre.
5. Cronología completa de seguimiento con usuario, fecha, transición de estado y comentario.
6. Evidencias asociadas al seguimiento que las originó.

Las imágenes se podrán ampliar a pantalla completa. El visor tendrá cierre visible, soporte para Escape y texto alternativo. Un archivo faltante mostrará `Evidencia no disponible` sin romper el detalle.

### Historial unificado

Una pantalla reunirá:

- Reportes de sucursal.
- Órdenes de mantenimiento.
- Reparaciones de unidades.
- Servicios realizados a unidades.
- Trabajos realizados sin orden o reporte previo.

Cada elemento tendrá un origen explícito: `Reporte`, `Orden`, `Reparación`, `Servicio de unidad` o `Trabajo sin reporte previo`. Los filtros serán periodo, estado, origen, sucursal, activo/unidad y búsqueda textual.

## Contratos de API

### Bandeja

Los contratos nuevos serán aditivos bajo `/api/mantenimiento/v2/`. Los endpoints actuales permanecerán sin cambios durante la transición.

`GET /api/mantenimiento/v2/bandeja/`

Parámetros:

- `estado=abiertos|cerrados|todos`; valor por defecto: `abiertos`.
- `periodo=semana|mes|30d|90d|todo`; valor por defecto para cerrados: `30d`.
- `origen=sucursales|logistica|todos`.
- `page` y `page_size`; valor por defecto 25 y máximo 100.

Respuesta:

```json
{
  "counts": {
    "abiertos": 0,
    "en_proceso": 0,
    "criticos": 0,
    "cerrados": 0
  },
  "schema_version": 2,
  "results": [],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total": 0,
    "has_next": false
  }
}
```

La PWA cambiará al endpoint v2 en el mismo release. Los endpoints legacy conservarán su shape y `items`; v2 no duplicará `items` y `results`.

### Detalle de bandeja

`GET /api/mantenimiento/v2/items/<tipo>/<id>/`

Tipos permitidos: `falla`, `orden`, `reporte_unidad`, `reparacion` y `servicio_unidad`. Un tipo no permitido o un objeto inexistente responderá 404 sin revelar información adicional.

Para `falla`, el contrato será:

```json
{
  "schema_version": 2,
  "uid": "falla:123",
  "tipo": "falla",
  "estado": {"codigo": "cerrado", "etiqueta": "Cerrado", "grupo": "cerrado"},
  "prioridad": {"codigo": "alta", "etiqueta": "Alta"},
  "reporte_inicial": {
    "titulo": "",
    "descripcion": "",
    "foto": {"id": "falla_inicial:123", "nombre": "foto.jpg", "mime": "image/jpeg", "url": "/api/mantenimiento/v2/evidencias/falla_inicial/123/"},
    "reportado_por": {"id": 1, "nombre": ""},
    "fecha": "2026-07-11T10:00:00-07:00",
    "sucursal": {"id": 1, "nombre": ""},
    "categoria": "",
    "area": "",
    "activo": {"id": 1, "nombre": ""}
  },
  "fechas": {"reporte": null, "asignacion": null, "resolucion": null, "cierre": null},
  "responsables": {"asignado_a": null, "cerrado_por": null},
  "seguimiento": [
    {
      "id": 1,
      "fecha": "2026-07-11T11:00:00-07:00",
      "usuario": {"id": 1, "nombre": ""},
      "estatus_anterior": "abierto",
      "estatus_nuevo": "en_proceso",
      "comentario": "",
      "evidencias": []
    }
  ]
}
```

Los nulos serán explícitos; `seguimiento` se ordenará de más antiguo a más reciente. Los demás tipos conservarán el mismo envoltorio y un bloque específico documentado por serializer.

### Historial

`GET /api/mantenimiento/v2/historial/`

Parámetros:

- `tipo=todo|reporte|orden|reparacion|servicio_unidad|sin_reporte`.
- `estado=todo|abierto|en_proceso|cerrado|cancelado`.
- `periodo`, `sucursal`, `activo`, `unidad`, `q`, `page`, `page_size`.

Respuesta con conteos y resultados normalizados. Los conteos no dependerán del límite de paginación. El orden será por `fecha_evento DESC, uid DESC`; `fecha_evento` será la fecha canónica definida por tipo.

## Semántica temporal y estados

Todas las fronteras se calculan en `America/Mazatlan`, con inicio inclusivo y fin exclusivo.

- Semana: lunes 00:00 hasta el lunes siguiente.
- Mes: primer día del mes 00:00 hasta el primer día del mes siguiente.
- 30d/90d: desde 00:00 de la fecha local correspondiente hasta el inicio del día siguiente al actual.
- Reporte cerrado: `fecha_cierre`, con fallback a `fecha_resolucion`.
- Orden cerrada: `fecha_cierre`.
- Reporte de unidad cerrado: su fecha real de resolución/cierre persistida; no se inferirá de `actualizado_en` si no representa cierre.
- Reparación: `fecha_entrega`, con fallback explícito a `actualizado_en` solo para históricos incompletos y etiqueta de fecha estimada.
- Servicio de unidad: `fecha_servicio`.

Los estados fuente se mapearán a cinco grupos canónicos: `abierto`, `en_proceso`, `cerrado`, `cancelado` y `programado`. `programado` cuenta como abierto, pero conserva su etiqueta propia.

## Normalización de registros

Cada resultado histórico compartirá:

- `uid`, `tipo`, `id`, `folio`.
- `titulo`, `descripcion`.
- `estado` y etiqueta.
- `prioridad` y etiqueta cuando aplique.
- fechas de reporte, inicio, servicio, cierre y registro cuando existan.
- sucursal.
- activo o unidad.
- reportante, creador, responsable y ejecutor cuando existan.
- proveedor y costo cuando apliquen.
- origen y bandera `es_sin_reporte`.
- resumen de evidencias y comentarios.

Las categorías de historial son mutuamente excluyentes. Una orden será `Trabajo sin reporte previo` cuando su origen sea emergencia o iniciativa, no provenga de un plan y no esté vinculada a una solicitud/reporte. Un servicio directo de unidad será únicamente `Servicio de unidad`, con `captura_directa=true`; no se duplicará como `sin_reporte`. Una reparación relacionada con un reporte aparecerá como evento separado con `parent_uid`, no como duplicado del reporte.

## Matriz de acceso

La primera entrega preservará el alcance efectivo actual y no ampliará permisos:

- DG, administradores y usuarios con gestión global de Mantenimiento: todas las sucursales y campos operativos.
- Usuarios con acceso limitado por sucursal: únicamente objetos cuya sucursal esté dentro de su alcance operativo.
- Usuarios de solo lectura: pueden listar, detallar y visualizar evidencia dentro de su alcance, pero no actualizar ni cargar archivos.
- Costos y facturas: solo roles que ya pueden verlos en el ERP o usuarios con gestión de Mantenimiento.
- Notas internas: no forman parte del contrato PWA v2.

Una política/queryset autorizado central se reutilizará en conteos, listas, detalles, descargas y mutaciones. Ningún endpoint resolverá un objeto únicamente por ID.

## Evidencias y seguridad

- Los endpoints usarán las mismas clases de autenticación y `EsMantenimiento` que la PWA actual.
- El detalle podrá consultar cerrados; no reutilizará el filtro de abiertos.
- Los serializers dejarán de entregar `.url` pública para las evidencias incluidas en v2.
- `GET|HEAD /api/mantenimiento/v2/evidencias/<tipo>/<id>/` validará autenticación, alcance y permiso del objeto padre antes de servir el archivo.
- Responderá inline para imágenes/PDF seguros y attachment para otros documentos permitidos; archivo ausente responderá 404.
- Usará `Cache-Control: private, no-store`, nombres seguros y prevención de path traversal. No se cacheará en el service worker.
- No se expondrán rutas físicas, `notas_internas` ni metadatos sensibles.
- Las nuevas cargas usarán un validador compartido: allowlist de MIME/extensión, verificación real de imagen, rechazo de SVG/HTML/ejecutables, máximo 10 MB por imagen, 30 MB por PDF y máximo 5 archivos por avance.
- Las respuestas construirán URLs a partir del request y no de dominios codificados.

## Rendimiento

- Conteos mediante agregaciones separadas y verificables.
- Listas paginadas en backend, 25 por defecto y 100 máximo.
- `select_related` y `prefetch_related` para evitar N+1.
- Detalle y galerías bajo demanda.
- Miniaturas con carga diferida.
- El frontend cancelará o ignorará respuestas obsoletas cuando cambien filtros rápidamente.
- Presupuesto inicial: conteos y página mixta en un número de consultas constante por tipo fuente; las pruebas compararán 1 contra 20 registros para impedir crecimiento N+1.

## Accesibilidad y comportamiento móvil

- Prioridad y estado tendrán texto además de color.
- Imágenes con texto alternativo descriptivo.
- Visor navegable con teclado y foco controlado.
- Controles táctiles de al menos 44px.
- Estados de carga con estructura estable; errores y vacíos explicarán qué falta.
- No se agregarán animaciones decorativas. Las transiciones de detalle serán breves y respetarán `prefers-reduced-motion`.

## Compatibilidad

- No se eliminarán endpoints ni campos actuales durante la primera entrega.
- Los responsables de texto y registros históricos incompletos seguirán visibles con etiquetas honestas como `Sin usuario registrado`.
- La PWA conservará su autenticación y rutas instalables.
- Todo cambio de HTML/JS de la PWA incrementará la versión de su service worker.
- El service worker cacheará solo el shell estático; nunca respuestas API ni evidencias autenticadas.

## Pruebas y validación

### Backend

- Conteos de cerrados por cada origen y periodo.
- Bordes temporales en zona Mazatlán: medianoche, lunes/domingo, cambio de mes y registros a 29/31 días.
- Conteos independientes de `page_size`.
- Filtros de abiertos, cerrados y todos.
- Detalle autorizado con foto inicial, cronología y evidencias.
- Acceso anónimo/sin permiso, alcance de otra sucursal, archivo protegido, HEAD, archivo faltante y path traversal.
- Servicio de unidad con autor, sucursal, factura y fecha.
- Clasificación correcta de trabajo sin reporte previo.
- Clasificación mutuamente excluyente y relación `parent_uid` sin duplicados.
- Consultas sin N+1 en los endpoints principales.

### PWA

- Cerrados deja de mostrar cero cuando existen registros.
- Cambio de periodo actualiza conteo y listado.
- Detalle muestra todos los campos acordados.
- Galería abre, navega y cierra correctamente.
- Historial filtra por origen y periodo.
- Estados vacíos, archivo faltante y error de red.
- Cambio rápido de filtros ignora una respuesta anterior tardía.
- Escape cierra el visor y devuelve el foco al elemento que lo abrió.
- Consola sin errores y requests relevantes correctos.
- Validación en viewport móvil real.

### Producción

- `migrate --check` y `check`.
- Pruebas de módulos afectados.
- `collectstatic` y bump del service worker.
- Verificación autenticada en `/mantenimiento/app/`.
- Confirmación de un reporte real con foto/evidencias, un cerrado real, un servicio de unidad y un trabajo sin reporte previo.
- Comparación de conteos ORM/SQL antes y después sin fabricar ni modificar datos reales.

## Fuera de alcance

- Reescribir los modelos fuente.
- Crear un segundo catálogo de usuarios, activos o unidades.
- Atribuir retroactivamente registros sin evidencia de autoría.
- Editar o eliminar archivos históricos desde el visor inicial.
- Cambiar permisos operativos existentes fuera de Mantenimiento.

## Entrega por etapas

1. Normalizadores de dominio, matriz de estados/fechas y pruebas unitarias.
2. APIs v2 de conteos, detalle, historial y archivos autenticados.
3. Detalle completo de reportes y evidencias en PWA.
4. Historial unificado con órdenes, reparaciones, servicios y trabajos sin reporte.
5. Validación móvil, seguridad y producción.
