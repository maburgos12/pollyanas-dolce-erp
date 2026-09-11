# Mensaje visible al fallar el inicio de turno

## Objetivo

Evitar que la PWA parezca ignorar el toque en “Iniciar turno” cuando la API rechaza la apertura. El repartidor debe ver el motivo real y poder corregirlo o reintentar sin perder su captura.

## Alcance

- Conservar el mensaje devuelto por la API (`mensaje`, `detail` o detalle de validación).
- Mostrarlo de forma persistente junto al botón de inicio y también mediante el modal operativo existente.
- Mientras la solicitud está en curso, bloquear únicamente el botón presionado y cambiar su etiqueta a “Iniciando…”.
- Ante error de API o red, restaurar el botón, mantener unidad, kilometraje, gasolina, foto y ubicación, y permitir reintento.
- Versionar conjuntamente la plantilla PWA, el registro del service worker y `CACHE_NAME`.

## Fuera de alcance

- Cambios a modelos, API, rutas, reglas de asignación o migraciones.
- Modificaciones adicionales a datos operativos.
- Rediseño general de la bitácora.

## Flujo

`guardarSalida` guarda el estado de envío en el borrador antes de llamar la API. La renderización coloca el estado de error inmediatamente antes del botón con semántica accesible. Toda respuesta no exitosa se normaliza con los helpers actuales, se conserva en el borrador y vuelve a renderizar. Un `catch` cubre fallos de red. Un `finally` lógico deja el formulario reintentable cuando no se creó el turno.

## Criterios de aceptación

1. Un rechazo `unidad_ruta_distinta` muestra literalmente el mensaje del servidor cerca de “Iniciar turno”.
2. El mismo error conserva los datos capturados y deja el botón habilitado.
3. Durante una solicitud no se puede hacer doble envío.
4. Un fallo de red muestra un mensaje accionable y también permite reintentar.
5. Las pruebas de Logística, `check` y `migrate --check` quedan correctos; el cambio llega a producción con una nueva versión de caché.
