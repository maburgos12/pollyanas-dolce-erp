# Logistica - flujo interno de operacion de rutas

Archivo editable principal: `docs/logistica/flujo_operacion_rutas_logistica.drawio`.

Este mapa resume el arbol completo del flujo actual de rutas:

1. Fuentes: solicitudes de sucursal, consolidado CEDIS, Point, catalogos, Google Routes/Roads.
2. Planeacion ERP: `RutaEntrega`, `ParadaRuta`, orden, geocercas, CEDIS como parada de recarga.
3. Carga: `RutaCargaChecklist`, `RutaCargaChecklistLinea`, cantidades solicitadas, enviadas, cargadas y bloqueos de salida.
4. PWA: autenticacion, bitacora/unidad, ruta activa, carga por tramo, validacion de lineas y entrega.
5. GPS: tracking, validaciones, geocercas, permanencia y eventos.
6. Entrega/recepcion: evidencia, estado de entrega, recepcion Point y cierre normal o con diferencia autorizada.
7. Control visual: mapa, timeline, detalle de ruta, service worker/cache y reglas de no mezclar evidencias.

Reglas operativas criticas reflejadas:

- GPS con llegada a geocerca no equivale a entrega.
- Point enviado no equivale a Point recibido.
- CEDIS no es una sucursal entregable; funciona como separador de tramo y recarga.
- La carga esperada, la carga marcada por repartidor y la recepcion real de sucursal son evidencias distintas.
- La PWA debe mostrar solo el tramo operativo actual para no mezclar primera salida con recarga posterior.
- Una ruta en curso debe respetar una sola unidad y un solo repartidor activos.
- Si cambia HTML, CSS o JS de la PWA, se debe hacer bump al service worker.

Archivos revisados como fuente principal:

- `logistica/models.py`
- `logistica/services_carga_ruta.py`
- `logistica/services_rutas_control.py`
- `api/logistica_views.py`
- `api/logistica_serializers.py`
- `api/urls.py`
- `logistica/templates/logistica/pwa.html`
- `logistica/templates/logistica/ruta_detail.html`
- `logistica/templates/logistica/control_rutas.html`
- `pos_bridge/services/open_transfer_sync_service.py`
- `pos_bridge/services/movement_sync_service.py`
- `recetas/models.py`
