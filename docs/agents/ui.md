# Validación de interfaces, acciones y PWA

Aplicar cuando el alcance afecte UI, formularios, navegación, autenticación,
plantillas, estáticos o flujos móviles. Leer primero [PRODUCT](../../PRODUCT.md)
y [DESIGN_STACK](../../DESIGN_STACK.md); elegir el stack mínimo, sin cargar
alternativas completas por defecto. No añadir frameworks JS al ERP principal;
conservar el patrón React existente en bonos cuando corresponda.

## Acciones que cambian estado — contrato obligatorio

- Toda pantalla nueva con acciones como Guardar, Autorizar, Aprobar, Rechazar, Cancelar o Resolver debe mantener la posición y el contexto del usuario.
- Usar el contrato progresivo `data-async-action` y respuesta JSON/HTML compartida; nunca duplicar la lógica de negocio entre ambos formatos.
- Mostrar éxito, error, advertencia o información mediante el toast global accesible. El banner superior no puede ser la única respuesta inmediata.
- Bloquear únicamente el botón presionado, mostrar `Guardando…` o `Procesando…` y prevenir doble envío.
- Ante error, conservar inputs y habilitar reintento. Para POST tradicional, regresar a un identificador estable mediante fragmento.
- Usar confirmación modal solo para acciones destructivas o irreversibles; debe admitir Escape, atrapar foco y devolverlo al disparador.
- Registrar cada migración en [registro de cobertura](../ux/action-context-coverage.md); no declarar cobertura total mientras existan pantallas pendientes.

### Service Worker — caché PWA
Cualquier módulo con `sw.js` (hoy: `bonos_ventas`, `bonos_produccion`, `logistica`,
`fallas`, `mantenimiento`, `operacion` — esta lista crece, no es exclusiva de bonos)
mantiene un caché en el navegador del cliente. Cuando un deploy cambia HTML, CSS o JS
visible en esos módulos, el cliente **no verá el cambio** aunque el servidor ya lo
tenga, porque el SW sigue sirviendo la versión anterior.

Regla: **cualquier commit que modifique templates o estáticos de un módulo con SW debe
incluir en ese mismo commit un bump de la constante `CACHE_NAME` en su `sw.js`.**

- Cmd+Shift+R (hard-refresh) no bypasea el Service Worker — no es suficiente
- El bump de versión es la única garantía de que todos los clientes reciben la versión nueva
- Después del deploy correr `collectstatic` para que el SW nuevo llegue a WhiteNoise

---

## Verificación en navegador
Cuando la tarea afecte UI, flujos web, formularios, navegación o autenticación,
validar en navegador real antes de cerrar con las herramientas de navegador
disponibles en Codex:
- Pantallas principales del módulo modificado
- Formularios: envío, validación, respuesta
- Errores de consola JavaScript
- Network requests relevantes (XHR/Fetch)
- Service worker y caché si algo no aparece

---

## Capturas móviles: iPhone y Safari como plataforma principal

El personal utiliza principalmente iPhone. Todo cambio en formularios y flujos
operativos móviles debe considerar Safari y la app instalada en iOS desde el diseño:

- Verificar WebKit además del navegador de escritorio: teclado decimal, fechas,
  campos ocultos, foco en el error, controles de al menos 44 px y áreas seguras.
- Conservar borradores por usuario, registro y ronda hasta confirmación del servidor.
  Un estado local o HTTP 200 sin recibo válido no significa guardado confirmado.
- Probar conexión interrumpida, respuesta perdida, sesión vencida, cambio de pestaña,
  suspensión/retorno y ediciones durante guardado. No depender solo de beforeunload.
- Reutilizar control de versiones e idempotencia. Ante resultado incierto, conservar
  el intento exacto; no sobrescribir ni reenviar automáticamente datos operativos.
- Indicar el campo concreto que bloquea y hacerlo visible, aunque esté en otra pestaña.
- Mantener diagnóstico acotado de fallos sin cantidades, notas, credenciales ni fotos.
- Distinguir pruebas automatizadas WebKit, Safari de Mac y un iPhone físico. Declarar
  qué se verificó; la emulación no prueba teclado, suspensión ni almacenamiento de iOS.

Estas reglas aplican al código que se cree o modifique; no autorizan cambios masivos
ni alteración de capturas existentes.
