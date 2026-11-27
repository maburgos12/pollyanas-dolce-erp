# Adaptación del ERP a ventanas de iPhone Duo

## Resultado esperado

El shell compartido del ERP y el tablero ejecutivo se distribuyen según el espacio
disponible al abrir/cerrar el dispositivo, girarlo o usar dos ventanas. Se conservan
rutas, permisos, acciones y fuentes de datos. Las PWA independientes no se rediseñan
en esta tarea: la cobertura es el shell que hereda `base.html` y el acceso del ERP.

## Decisiones

- Usar CSS flexible y consultas del contenedor en el tablero; no detectar modelos
  por user agent ni depender de Device Posture/Viewport Segments experimentales.
- Mantener el viewport existente. Safari reserva la región de controles del sistema;
  no intentar ocuparla con `viewport-fit=cover`.
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

## Fuentes

- [WebKit: Safari 27.1 e iPhone Duo](https://webkit.org/blog/18486/)
- [Apple: Designing for iPhone Duo](https://developer.apple.com/design/human-interface-guidelines/designing-for-iphone-duo)
- [Apple: Responsive Design Mode](https://developer.apple.com/documentation/safari-developer-tools/responsive-design-mode)
