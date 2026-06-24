# Paquete Logistica: Trazo Ejecutado GPS

## Objetivo

Mejorar el trazo que realmente ejecuta la unidad en `Control interno de rutas` sin crear otro motor GPS.

Resultado esperado:
- mas puntos utiles desde la PWA;
- linea ejecutada cortada cuando hubo huecos de GPS;
- puntos malos visibles como evidencia, pero fuera del trazo limpio;
- mapa mas legible para auditoria operativa.

## Regla Ponytail

No agregar dependencias, modelos, tablas ni workers nuevos en esta fase.

Usar lo que ya existe:
- `UbicacionRuta` guarda lat/lng, precision, velocidad, timestamps e IP.
- `EventoRuta` ya registra `GPS_PERDIDO`, `GPS_PRECISION_BAJA`, `UBICACION_TARDIA` y `SALTO_IMPOSIBLE`.
- `snap_gps_path_to_roads()` ya ajusta puntos a Google Roads.
- `control_rutas.html` ya dibuja Leaflet.
- `pwa.html` ya tiene tracking automatico, cola offline y service worker.

## Paquete V1

### 1. PWA: capturar mejores puntos

Owner sugerido: Agente PWA.

Archivos:
- `logistica/templates/logistica/pwa.html`
- `logistica/static/logistica/pwa/sw.js`
- `logistica/tests.py`

Cambios minimos:
- Bajar `ROUTE_AUTO_TRACKING_INTERVAL_MS` de 90s a 30s o 45s.
- En `captureRoutePosition()`, enviar `velocidad_kmh` cuando `position.coords.speed` exista:
  - `speed` viene en m/s;
  - guardar `velocidad_kmh = speed * 3.6`.
- Mantener `getCurrentPosition`; no cambiar a `watchPosition` todavia.
- Hacer bump obligatorio:
  - `CACHE_NAME` en `sw.js`;
  - query `sw.js?v=...` en `pwa.html`.

Pruebas minimas:
- Actualizar `test_pwa_tracking_declara_cola_offline_reintento_y_cache_versionado`.
- Agregar assert de `velocidad_kmh` en el payload automatico.

No hacer:
- no cambiar login/token;
- no tocar recepcion Point;
- no cambiar modelos;
- no usar librerias de tracking.

### 2. Backend mapa: segmentar el trazo ejecutado

Owner sugerido: Agente Backend/Mapa.

Archivos:
- `logistica/views.py`
- `logistica/tests.py`

Cambios minimos:
- En `_control_rutas_mapa_payload()`, incluir en cada ubicacion:
  - `timestamp`: `timestamp_servidor.isoformat()`;
  - `precision_metros`;
  - flags derivados por eventos asociados cuando existan: `precision_baja`, `ubicacion_tardia`, `salto_imposible`.
- Construir `ubicaciones_segmentos` para el mapa:
  - cortar segmento si hay gap mayor a 3 minutos;
  - cortar segmento si cambia `fuera_geocerca`;
  - excluir del trazo limpio puntos marcados como baja precision, tardios o salto imposible;
  - preservar esos puntos en `ubicaciones` para evidencia y popup.
- Aplicar `snap_gps_path_to_roads()` por segmento, no a toda la ruta junta.
- Corregir el slice de GPS: usar los ultimos 300 puntos en orden cronologico, no los primeros 300.

Payload propuesto:

```json
{
  "ubicaciones": [{"lat": 0, "lng": 0, "hora": "13:10", "timestamp": "...", "precision_metros": 12}],
  "ubicaciones_segmentos": [
    {"fuente": "GOOGLE_ROADS", "estado": "normal", "coords": [{"lat": 0, "lng": 0}]},
    {"fuente": "RAW", "estado": "fuera_geocerca", "coords": [{"lat": 0, "lng": 0}]}
  ]
}
```

Pruebas minimas:
- ruta con dos ubicaciones separadas por mas de 3 minutos crea dos segmentos;
- punto con evento `GPS_PRECISION_BAJA` queda en `ubicaciones`, pero no en `ubicaciones_segmentos`;
- si Google Roads falla, segmento cae a `RAW`.

No hacer:
- no borrar ubicaciones;
- no ocultar eventos operativos;
- no recalcular rutas planeadas.

### 3. Frontend mapa: dibujar segmentos, no una sola linea

Owner sugerido: Agente Frontend/Mapa.

Archivos:
- `logistica/templates/logistica/control_rutas.html`
- `logistica/tests.py`

Cambios minimos:
- Si `route.ubicaciones_segmentos` existe, dibujar cada segmento con `L.polyline`.
- Mantener fallback actual usando `ubicaciones_snapped` / `ubicaciones`.
- Estilos:
  - normal: azul solido;
  - fuera de geocerca: naranja/rojo;
  - raw fallback: azul punteado;
  - gap sin GPS: no dibujar linea conectando.
- Mantener marcador de ultimo GPS.
- Agregar dots chicos para puntos descartados del trazo limpio con popup de motivo.

Pruebas minimas:
- template contiene loop de segmentos;
- template conserva fallback para payload viejo;
- no se conecta un gap como linea continua.

No hacer:
- no meter clustering;
- no cambiar layout general;
- no redisenar panel de evidencia.

## Orden de trabajo paralelo

1. Agente PWA implementa captura mas frecuente + velocidad + cache bump.
2. Agente Backend/Mapa implementa payload segmentado.
3. Agente Frontend/Mapa implementa render de segmentos usando el payload nuevo.

Los agentes pueden trabajar a la par porque sus archivos principales son distintos.
La integracion final debe resolver `logistica/tests.py` si hay choque de tests.

## Criterios de aceptacion

Local:
- `python3 manage.py test logistica.tests.LogisticaControlRutasTests --verbosity 2`
- `python3 manage.py check`
- `python3 manage.py migrate --check`

Produccion:
- PR mergeado a `main`.
- `git pull origin main` en VPS.
- `python manage.py migrate --check`.
- `python manage.py check`.
- `collectstatic` si cambio de assets estaticos o service worker.
- restart de `web`; si solo PWA/static, confirmar que WhiteNoise sirve el SW nuevo.
- abrir `https://erp.pollyanasdolce.com/logistica/rutas/control/`.
- confirmar visualmente:
  - ruta planeada sigue por carretera;
  - trazo ejecutado ya no une huecos de GPS;
  - ultimo GPS aparece;
  - puntos malos quedan visibles como evidencia.

## Fuera de alcance V1

- ETA.
- optimizacion de rutas.
- `watchPosition`.
- clustering.
- retencion historica.
- dashboards nuevos.
- cambios de privacidad/politica.

Agregar solo cuando V1 no alcance con evidencia real de campo.

