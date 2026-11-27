# Adaptación del ERP a ventanas de iPhone Duo

## Resultado esperado

El shell compartido del ERP y el tablero ejecutivo se distribuyen según el espacio
disponible al abrir/cerrar el dispositivo, girarlo o usar dos ventanas. Se conservan
rutas, permisos, acciones y fuentes de datos. Las PWA independientes no se rediseñan
en esta tarea: la cobertura es el shell que hereda `base.html` y el acceso del ERP.

## Decisiones

- Usar CSS flexible y consultas del contenedor en el tablero; no detectar modelos
  por user agent ni depender de Device Posture/Viewport Segments experimentales.
- Usar `viewport-fit=cover` tanto en el acceso como en el shell del ERP para
  solicitar todo el viewport que el sistema ofrezca a la PWA instalada. La revisión
  inicial conservó el viewport automático y sólo se comprobó en Safari de Mac;
  esa prueba no validaba la app de la pantalla de inicio. No confundir el fondo
  extendido con espacio utilizable para controles: iOS conserva la decisión sobre
  la región del sistema, y Safari 27.1 documenta una columna lateral reservada.
- Dejar que iOS determine el estilo de la barra de estado. Retirar el metadato
  antiguo `apple-mobile-web-app-status-bar-style`, sin cambiar permisos,
  almacenamiento, modo `standalone`, orientación ni contrato de red del SW.
- Tratar cada área segura por separado. La barra, el menú, el contenido, los avisos,
  los diálogos y el acceso respetan las variables `safe-area-inset-*`.
- Usar el menú táctil hasta 1100 px con puntero grueso; conservar la navegación de
  escritorio con ratón. CSS y JavaScript comparten la misma condición. Al pasar a
  escritorio se cierra el menú y se libera el bloqueo del scroll.
- En ventanas estrechas, colocar la variación debajo de los importes mensuales y
  mantener cada año junto a su cantidad. Aprovechar dos/cuatro columnas cuando el
  contenedor lo permita. Conservar el desplazamiento interno de tablas existentes.
- Mantener controles táctiles de 44 px y texto de entrada de 16 px. En los selectores
  nativos de una línea, fijar altura explícita para WebKit.
- Versionar estilos y shell PWA juntos. No modificar el contrato de caché: sigue
  obteniendo los datos de red.

## Validación y límites

Comprobar en WebKit y Chromium los tamaños 382×580, 594×368, 669×867,
951×585 y 385×571 documentados por WebKit; añadir 320×568 y escritorio 1440×900.
Verificar tarjetas sin contenido recortado, abrir/cerrar menú, cambio de tamaño sin
perder selección, desbloqueo al volver a escritorio, filtros y áreas seguras
asimétricas inyectadas. Revisar acceso, compras y ventas como consumidores.

La base PostgreSQL 16 local contiene únicamente migraciones y datos sintéticos.
La emulación no prueba la interfaz de Safari de iOS, el teclado, la suspensión ni
el hardware físico. La revisión de consumidores no equivale a validar cada ruta
del ERP ni las PWA independientes. La publicación requiere CI y autorización
aplicable para integración/despliegue.

Corrección posterior a la captura de Mauricio: usa la PWA instalada en iOS 27.2.1.
Se comprueba el HTML de entrada y tablero, el SW servido y que el layout conserve
los cuatro márgenes seguros al girar o cambiar de tamaño. Las áreas seguras
inyectadas en WebKit/Chrome sólo prueban el CSS: no reproducen la geometría de la
columna del sistema. No afirmar que se aprovechó la zona amarilla sin una nueva
comprobación de la PWA instalada en ese dispositivo. No fijar un ancho por modelo,
ocultar la barra del sistema ni estirar la página con zoom o márgenes negativos.

## Fuentes

- [WebKit: Safari 27.1 e iPhone Duo](https://webkit.org/blog/18486/)
- [Apple: Designing for iPhone Duo](https://developer.apple.com/design/human-interface-guidelines/designing-for-iphone-duo)
- [Apple: Responsive Design Mode](https://developer.apple.com/documentation/safari-developer-tools/responsive-design-mode)
- [WebKit: viewport-fit y áreas seguras](https://webkit.org/blog/7929/designing-websites-for-iphone-x/)
- [WebKit: barra de estado de aplicaciones de la pantalla de inicio](https://bugs.webkit.org/show_bug.cgi?id=317153#c2)
