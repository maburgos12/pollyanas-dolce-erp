# nimiq/qr-scanner 1.4.2 (vendorizado)

Origen: https://cdn.jsdelivr.net/npm/qr-scanner@1.4.2/
Licencia: MIT (ver `LICENSE`).

Se sirve desde el propio dominio, nunca desde un CDN: la cámara del personal de
sucursal no debe depender de un tercero ni exponer la navegación del ERP.

Se usa la compilación UMD (`qr-scanner.umd.min.js`), que define el global
`QrScanner` para un `<script>` normal; el ERP no usa bundlers ni módulos ES en
sus templates.

Se vendorizan **cuatro** archivos, no dos: ambos `.js` y sus dos `.map`. Los
bundles minificados terminan con un comentario `sourceMappingURL`, y el
almacenamiento de estáticos de WhiteNoise resuelve esa referencia al correr
`collectstatic`. Si el `.map` falta, `collectstatic` **aborta por completo** y el
deploy se queda con los estáticos viejos. Se prefiere traerlos a editar el
bundle, para que los archivos queden byte a byte como los publica upstream.

Para actualizar: descargar los cuatro archivos del mismo tag, conservar la
licencia y volver a correr `operacion.tests.OperacionAppTests`.
