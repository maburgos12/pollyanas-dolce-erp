# Design QA - Panel DG compacto

## Evidencia

- Fuente visual: `/Users/mauricioburgos/.codex/generated_images/019f854e-735e-7162-8e75-6bb393bf8ea3/exec-4d5a907d-c772-4880-a193-79e350bbf111.png`
- Implementación renderizada: `/Users/mauricioburgos/.codex/visualizations/2026/07/21/019f854e-735e-7162-8e75-6bb393bf8ea3/seguimiento-empleados/panel-dg-compacto-mobile-final.png`
- Implementación actualizada con iconografía: `/Users/mauricioburgos/.codex/visualizations/2026/07/21/019f854e-735e-7162-8e75-6bb393bf8ea3/seguimiento-empleados/panel-dg-compacto-iconos-viewport.png`
- Comparación conjunta: `/Users/mauricioburgos/.codex/visualizations/2026/07/21/019f854e-735e-7162-8e75-6bb393bf8ea3/seguimiento-empleados/comparacion-panel-dg-final.png`
- Viewport de implementación: 390 x 844 CSS px, deviceScaleFactor 1.
- Dimensiones de fuente: 853 x 1844 px; normalizada a 390 x 843 px para la comparación.
- Dimensiones de implementación: 390 x 1151 px, captura de página completa.
- Estado: Dirección General autenticada, filtro `Vencidos`, tres acuerdos de prueba agrupados por persona.

## Comparación de vista completa

La implementación conserva la composición aprobada: encabezado ejecutivo, CTA Nuevo acuerdo, resumen 2 x 2, selector horizontal de estados y lista compacta agrupada por persona. La mayor altura de la implementación es intencional: mantiene tamaños de texto y objetivos táctiles accesibles a 390 CSS px, mientras la fuente fue generada a 853 px y luego reducida.

## Comparación enfocada

No fue necesaria una segunda composición recortada. La comparación conjunta permite leer con claridad tipografía, CTA, celdas del resumen, estados, avatares, cantidades y affordances de expansión.

## Superficies de fidelidad

- Tipografía: Playfair Display para títulos y Nunito para UI, pesos, jerarquía y line-height coherentes con el ERP y la fuente.
- Espaciado y ritmo: resumen 2 x 2, divisores y agrupación equivalentes. En móvil el CTA ocupa el ancho disponible para evitar truncado.
- Colores y tokens: vino, dorado, verde, rosa cálido y texto marrón usan los tokens existentes del ERP.
- Imágenes y activos: logo real del ERP e iconos Tabler ya disponibles; no hay placeholders, CSS art ni SVG artesanal.
- Copy: etiquetas ejecutivas y descripciones de estado son claras y fieles a la propuesta aprobada.
- Responsividad: validada a 390 x 844 y 853 x 1844; sin solapamientos ni controles ocultos.
- Accesibilidad: controles semánticos nativos, foco visible, objetivos táctiles y reduced-motion.

## Interacciones verificadas

- Cambio de `Vencidos` a `Activos` y actualización del conteo/lista.
- Despliegue de la lista completa.
- Despliegue por persona, manteniendo una sola persona abierta.
- Apertura y cierre del diálogo `Nuevo acuerdo`.
- Consola del navegador sin errores ni advertencias.

## Historial de iteraciones

1. P1: el CTA `Nuevo acuerdo` se truncaba en el viewport móvil. Se cambió el encabezado móvil a columna y el CTA a ancho completo. Evidencia posterior: `panel-dg-compacto-mobile-final.png`, sin truncado.
2. Segunda comparación: no quedan hallazgos P0, P1 ni P2. La diferencia de densidad vertical frente a la fuente reducida es una adaptación responsive deliberada para legibilidad y táctil.
3. Ajuste solicitado durante la implementación: se incorporaron cuatro íconos Tabler con tratamiento cromático por estado en el mini dashboard. Se verificaron los cuatro activos y sin colisiones a 390 x 844.

## Hallazgos

No quedan diferencias accionables P0, P1 o P2.

## Seguimiento opcional

- P3: ajustar microespaciado después de observar datos reales con nombres especialmente largos.

final result: passed
