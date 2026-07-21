# Diseño: bandeja de minutas orientada a la acción para empleados

## Objetivo

Reorganizar la pantalla personal de seguimiento para que el empleado identifique en segundos qué acuerdo debe atender, qué puntos le faltan y cuál es su siguiente acción. La vista inicial deja de funcionar como archivo acumulado y se convierte en una bandeja operativa.

Este alcance es exclusivamente frontend. Reutiliza los datos, permisos, rutas y acciones que el módulo `seguimiento` ya entrega. No modifica modelos, migraciones, API, autenticación, asignaciones ni información operativa.

## Problema observado

La pantalla actual mezcla acuerdos activos y completados dentro de una misma sección. Los indicadores acumulados ocupan la zona principal y cada acuerdo despliega simultáneamente metadatos, checklist y varios formularios. Como resultado:

- los completados compiten visualmente con lo pendiente;
- el empleado debe recorrer demasiada información para encontrar qué necesita hacer;
- los estados pendiente, listo para cerrar y en revisión no se distinguen como momentos diferentes del flujo;
- el checklist existe, pero no funciona como el contenido principal del acuerdo;
- las acciones de feedback, evidencia y prórroga aparecen con la misma jerarquía.

## Dirección aprobada

La pantalla responde primero: “¿qué debo hacer ahora para terminar?”.

La vista de Minutas abre en `Por atender`. Los acuerdos se ordenan por urgencia y se muestran como acordeones. Al abrir uno aparecen sus puntos, avance y siguiente acción. Los completados se retiran del flujo principal y permanecen disponibles en `Historial`.

La identidad visual conserva los patrones del ERP móvil: encabezado vino, acento dorado, fondo cálido, Playfair Display para títulos, Nunito para interfaz, navegación horizontal y superficies compactas.

## Arquitectura de información

### Navegación de módulo

Se conserva la navegación existente de `Mi trabajo`: Notificaciones, Minutas, Proyectos, Compromisos y los demás elementos definidos por `core/navigation.py`. No se crea una navegación paralela ni se cambian rutas.

### Segmentos operativos dentro de Minutas

1. **Por atender**: acuerdos abiertos que requieren actividad del empleado. Es la vista inicial.
2. **Para cerrar**: checklist completo o avance suficiente para enviar el acuerdo a revisión.
3. **En revisión**: acuerdos ya enviados que esperan aprobación; no presentan acciones de edición como llamada principal.
4. **Historial**: completados y cancelados. No aparecen en los tres segmentos operativos.

Los conteos de cada segmento deben mostrarse junto a su etiqueta. La clasificación visual se deriva de los campos y propiedades que ya existen en el contexto de la plantilla, sin persistir un nuevo estado.

## Orden de prioridad

Dentro de `Por atender`, el orden visual recomendado es:

1. vencidos;
2. vencen dentro de 24 horas;
3. respuesta nueva del DG o devolución para corrección;
4. resto de acuerdos activos por fecha límite ascendente;
5. acuerdos sin fecha al final.

La prioridad debe expresarse con texto y color, nunca solo con color.

## Componentes

### Encabezado operativo

- Kicker: `Mi trabajo`.
- Título: `Lo que debes atender`.
- Subtítulo contextual: tipo activo y criterio de orden.
- No muestra tarjetas de usuario, puesto, sucursal ni métricas históricas en la primera pantalla.

### Barra de segmentos

- Cuatro controles: Por atender, Para cerrar, En revisión e Historial.
- Objetivo táctil mínimo de 44 px.
- En móvil puede desplazarse horizontalmente si el ancho no permite cuatro etiquetas legibles.
- Mantiene foco visible, `aria-current` o estado equivalente y etiquetas accesibles.

### Aviso de prioridad

Cuando existe un acuerdo vencido o por vencer, aparece un aviso compacto: `Empieza por este acuerdo`. El aviso desplaza el foco al primer acuerdo prioritario; no crea una segunda copia del registro.

### Tarjeta-acordeón de acuerdo

Estado contraído:

- prioridad o fecha límite;
- título completo;
- origen o área cuando exista;
- resumen de avance, por ejemplo `2 puntos pendientes`;
- indicador claro para desplegar.

Estado desplegado:

- descripción y entregable esperado cuando existan;
- barra de avance;
- checklist completo, con estado individual de cada punto;
- última respuesta del DG cuando sea nueva;
- una acción principal contextual;
- acciones secundarias agrupadas bajo `Más acciones`.

Solo un acuerdo permanece abierto a la vez en móvil para evitar una página interminable. En escritorio se conserva el mismo comportamiento por claridad.

### Acción principal contextual

- Checklist incompleto: `Continuar y adjuntar evidencia`.
- Checklist completo y todavía abierto: `Enviar para revisión` o la acción existente equivalente.
- En revisión: mensaje `Esperando aprobación`, sin botón principal de edición.
- Devuelto por DG: `Corregir y responder`.

`Necesito más tiempo`, retroalimentación adicional y carga secundaria de evidencia se muestran como acciones secundarias. La lógica y endpoints actuales no se duplican.

### Historial

- No carga visualmente dentro de `Por atender`.
- Muestra completados y cancelados con búsqueda o filtros existentes cuando estén disponibles.
- Los registros históricos permanecen consultables y conservan toda su trazabilidad.

## Estados visuales

### Vacío

Si no hay acuerdos por atender, la pantalla no muestra métricas en cero. Presenta: `No tienes acuerdos pendientes` y accesos a `En revisión` e `Historial` cuando tengan registros.

### Carga

La renderización actual es del servidor; no se introduce un cargador artificial. Si una acción asíncrona existente actualiza una tarjeta, el botón presionado muestra `Guardando…` y queda bloqueado hasta recibir respuesta.

### Error

Se conserva el contrato global de acciones: toast accesible, inputs preservados y reintento disponible. El error no debe cerrar el acordeón ni desplazar al usuario al inicio.

### Sin checklist

El acuerdo muestra su descripción, entregable y una acción de actualización. No inventa puntos vacíos ni un porcentaje de avance engañoso.

## Responsive y accesibilidad

- Mobile-first para teléfonos usados por empleados.
- Una sola columna hasta escritorio.
- Tipografía mínima de 14 px para contenido operativo y 12 px solo para metadatos secundarios.
- Controles táctiles de al menos 44 × 44 px.
- Acordeones operables con teclado y lectores de pantalla.
- Contraste WCAG AA.
- Prioridades con texto, icono o borde además del color.
- Sin animaciones necesarias para acciones frecuentes; cualquier transición de acordeón respeta `prefers-reduced-motion`.

## Archivos previstos para una implementación posterior

- `seguimiento/templates/seguimiento/mi_seguimiento.html`: nueva jerarquía y componentes de presentación.
- `static/css/template_modules/seguimiento-templates-seguimiento-mi-seguimiento.css`: estilos responsive y estados del acordeón.
- `seguimiento/tests.py`: cobertura de segmentación visible y preservación de acciones/rutas.
- `static/erp-sw.js` o el service worker que controle esta superficie: bump de caché únicamente si el análisis de implementación confirma que cachea los assets modificados.

No se prevén cambios en `seguimiento/models.py`, migraciones, endpoints, permisos ni asignaciones.

## Contratos que deben preservarse

- Cada empleado solo ve acuerdos donde es responsable o participante.
- DG y revisores globales continúan entrando al Panel de acuerdos del equipo.
- Las rutas Minutas, Proyectos y Compromisos conservan su comportamiento y filtros.
- Toggle de checklist, feedback, evidencia y prórroga siguen usando sus acciones actuales.
- Los completados permanecen en el sistema y son consultables; solo cambia su ubicación visual.
- No se duplica lógica de negocio entre HTML y respuestas asíncronas.

## Criterios de aceptación

1. Al abrir Minutas como empleado, la primera vista contiene solo trabajo operativo activo.
2. Ningún acuerdo completado aparece en `Por atender`, `Para cerrar` o `En revisión`.
3. `Historial` conserva completados y cancelados.
4. El primer acuerdo prioritario se identifica sin desplazamiento en un teléfono común.
5. Al desplegar un acuerdo se ven todos sus puntos, avance, fecha y siguiente acción.
6. En móvil solo un acuerdo permanece desplegado a la vez.
7. Un acuerdo en revisión comunica que espera aprobación y no invita a editarlo como si estuviera pendiente.
8. Un acuerdo sin checklist sigue siendo comprensible y accionable.
9. Las acciones existentes conservan URL, método, CSRF, mensajes y contexto después de guardar.
10. La vista de DG, permisos y alcance de usuarios no cambian.
11. No se agregan modelos, migraciones ni contratos de API.
12. La pantalla se valida en navegador real móvil y escritorio, incluyendo consola, red y service worker.

## Pruebas previstas

- Empleado con mezcla de pendientes, en revisión, completados y cancelados.
- Empleado sin pendientes pero con historial.
- Acuerdo vencido, por vencer, sin fecha y devuelto por DG.
- Acuerdo con checklist parcial, completo y sin checklist.
- Acciones de checklist, feedback, evidencia y prórroga conservan contexto.
- Navegación y filtros por Minutas, Proyectos y Compromisos.
- DG sigue redirigido al panel global.
- Viewports móviles de 375 px y 430 px, además de escritorio.
- Consola sin errores y solicitudes sin respuestas fallidas.

## Fuera de alcance

- Cambiar cómo se crean o asignan acuerdos.
- Alterar estados persistidos o reglas de aprobación.
- Modificar app.pollyanasdolce.com.
- Cambiar permisos, usuarios o datos históricos.
- Introducir una SPA, framework JavaScript o biblioteca externa.
- Rediseñar el Panel DG.
