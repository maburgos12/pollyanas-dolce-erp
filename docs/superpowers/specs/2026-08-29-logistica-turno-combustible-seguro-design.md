# Diseño: turno y combustible seguros para repartidores

## Problema confirmado

El 29 de agosto de 2026 Luis Peraza abrió correctamente el turno `GS-P1-0106`
en la Peugeot Partner. Después, el navegador siguió consultando el turno activo,
pero no envió ninguna solicitud para registrar combustible ni cerrar el turno.

La PWA deshabilita `Guardar carga` hasta obtener GPS, aunque el contrato del
backend acepta coordenadas opcionales. También deshabilita `Cerrar turno` cuando
falta algún requisito. Al estar deshabilitados, ambos botones impiden ejecutar
las validaciones que ya contienen mensajes explicativos. Para el repartidor el
resultado es un botón inerte, sin una acción que indique cómo continuar.

La misma revisión encontró otro defecto independiente: dos solicitudes
concurrentes crearon los turnos abiertos `GS-P1-0104` y `GS-P1-0105` para José
Luis con el mismo kilometraje y prácticamente la misma hora. La comprobación del
turno abierto ocurre fuera de la transacción y no serializa aperturas del mismo
repartidor.

## Resultado esperado

1. Un repartidor puede registrar una carga aun cuando el dispositivo no entregue
   ubicación, sin perder ticket, litros, importe, unidad, turno, usuario, hora e
   IP.
2. Los botones de guardar combustible y cerrar turno siempre responden. Si falta
   información, muestran exactamente qué debe completar.
3. El cierre conserva como obligatorios kilometraje, nivel de gasolina y foto de
   tablero; no se crea una excepción administrativa desde la PWA.
4. Dos solicitudes concurrentes de inicio producen un solo turno abierto para el
   repartidor. La segunda recibe la respuesta normal `turno_abierto` con el turno
   existente.
5. La corrección no modifica turnos ni cargas históricas y no amplía permisos.

## Alternativas consideradas

### A. Conservar los botones deshabilitados y mejorar el texto

No resuelve el bloqueo: el usuario continúa sin poder ejecutar la acción y un GPS
denegado sigue impidiendo una captura crítica.

### B. Permitir gasolina sin GPS, conservar evidencia principal y validar al pulsar

Es la opción elegida. Coincide con el modelo actual, donde latitud y longitud son
opcionales, preserva la evidencia comprobable del ticket y evita que una capacidad
ambiental del teléfono bloquee toda la operación.

### C. Crear un cierre administrativo desde la PWA

Se descarta. Relajaría kilometraje, nivel o fotografía y permitiría que el propio
repartidor omita evidencia de cierre. Las regularizaciones excepcionales deben
seguir siendo administrativas, explícitas y auditables.

## Diseño de la solución

### Captura de combustible

- `litros`, `importe_total` y `foto_ticket` continúan obligatorios.
- `nivel_gas_despues` continúa opcional, como hoy.
- La PWA intenta obtener ubicación al abrir la captura.
- Si el GPS responde, envía latitud y longitud sin cambiar el flujo actual.
- Si el GPS es denegado, no está disponible o vence el tiempo, muestra `Se
  guardará sin ubicación` y permite continuar.
- `Guardar carga` permanece accionable. Al pulsarlo, enumera los requisitos
  faltantes en lugar de quedar inerte.
- El backend conserva las coordenadas cuando existen y `NULL` cuando no existen;
  no se requiere cambio de modelo ni migración.

### Cierre de turno

- `Cerrar turno` permanece accionable.
- Antes de enviar, muestra los faltantes de kilometraje, nivel y foto.
- Si el nivel final es mayor al inicial y no existe una carga registrada, exige
  registrar la gasolina primero, como hoy.
- No se relajan validaciones del serializer ni evidencia de llegada.

### Apertura concurrente

- El endpoint obtiene un bloqueo de fila sobre el `Repartidor` dentro de una
  transacción.
- Ya dentro del bloqueo vuelve a consultar si existe un turno abierto.
- Si existe, devuelve el mismo contrato `turno_abierto` y no crea otro registro.
- Si no existe, valida y crea el turno en esa misma sección transaccional.
- No se agrega una restricción de base de datos en esta entrega para evitar una
  migración que primero exigiría regularizar los duplicados históricos abiertos.

## Archivos previstos

- `api/logistica_views.py`: serialización transaccional del inicio de turno.
- `api/tests_logistica.py` o la batería logística equivalente: regresión de
  concurrencia/segundo intento.
- `logistica/templates/logistica/pwa.html`: estados accionables, requisitos y GPS
  opcional para combustible.
- Prueba estática o de navegador de la PWA para los contratos visibles.
- `logistica/static/logistica/pwa/sw.js`: incremento obligatorio de caché.

No se modificarán modelos, migraciones, permisos, rutas asignadas, turnos
existentes ni registros de combustible.

## Pruebas y validación

### Automatizadas

- La regresión PWA falla inicialmente porque el guardado exige GPS y los botones
  quedan deshabilitados.
- La prueba de inicio repetido/concurrente falla inicialmente porque puede crear
  más de un turno abierto.
- Las pruebas existentes de combustible confirman que ticket, litros e importe
  siguen obligatorios.
- `python manage.py check` y `python manage.py migrate --check` deben terminar sin
  errores ni migraciones pendientes.

### Navegador

- Con GPS permitido: registra gasolina con coordenadas.
- Con GPS denegado: explica la ausencia y permite guardarla sin coordenadas.
- Sin ticket, litros o importe: el botón responde y enumera faltantes; no envía.
- Cierre incompleto: enumera faltantes; no envía.
- Cierre completo: envía una sola solicitud y refleja el turno cerrado.
- Se revisan consola, Network y versión del Service Worker.

### Producción

- Despliegue únicamente después de CI y revisión del diff.
- Lectura fresca del SHA desplegado y de la versión de caché.
- Validación autenticada del flujo real sin inventar kilometraje, gasolina o
  fotografías para el turno vigente.
- Confirmación de que no se crearon turnos duplicados durante la prueba.

## Riesgos y mitigaciones

- **Menor evidencia geográfica:** solo se degrada GPS; ticket, usuario, unidad,
  turno, hora, IP, litros e importe permanecen. La interfaz declara que se guarda
  sin ubicación.
- **Doble toque o reintento de red:** el bloqueo por repartidor serializa la
  apertura y la segunda solicitud recupera el turno existente.
- **Caché antigua:** el mismo commit incrementa `CACHE_NAME` y la URL de registro
  del Service Worker; el despliegue ejecuta `collectstatic`.
- **Afectación a cierres:** no se cambia el serializer de llegada ni sus campos
  obligatorios.

## Criterios de aceptación

- Ninguna falla de GPS impide registrar una carga válida con ticket.
- Ningún botón crítico queda inerte sin explicar el requisito faltante.
- No se puede cerrar sin KM, nivel de llegada y foto.
- Un repartidor no obtiene dos turnos por solicitudes concurrentes.
- Cero cambios de datos históricos, permisos, modelos o migraciones.
- Pruebas, CI, despliegue seguro y validación de PWA en producción documentados.
