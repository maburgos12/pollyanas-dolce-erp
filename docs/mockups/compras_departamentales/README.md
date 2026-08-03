# Mockup de compras departamentales

Prototipo visual aislado. No usa datos reales, no escribe en el ERP y no representa una integración terminada.

## Vistas incluidas

- solicitud mensual de la responsable de área;
- bandeja compartida de Compras;
- autorización de Dirección General por exceso presupuestal;
- seguimiento con recepción parcial.

## Interacciones demostrables

- cambiar entre los cuatro roles o momentos del flujo;
- agregar artículos;
- recalcular subtotales y total estimado;
- adjuntar una imagen local de referencia;
- simular el envío a Compras;
- validar comentarios para decisiones de Dirección General.

El punto de entrada es `index.html`. Los scripts `render.mjs` y `verify.mjs` permiten generar capturas y verificar el comportamiento de forma headless con un runtime que proporcione Playwright y Chrome.
