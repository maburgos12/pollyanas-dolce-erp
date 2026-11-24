// Ejecuta los dos scripts reales juntos; DOM, red y reloj son dobles aislados.
// No abre navegador, no consulta Point y no escribe datos de operación.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const path = require('node:path');
const root = path.resolve(__dirname, '../..');
const workerSource = fs.readFileSync(process.argv[2] || path.join(root, 'logistica/static/logistica/pwa/sw.js'), 'utf8');
const consumerSource = fs.readFileSync(path.join(root, 'logistica/static/logistica/pwa/carga_point_erp.js'), 'utf8');

async function scenario({dirty = false, disconnect = false, resume = false} = {}) {
  let worker, tick = 0, network = 0, reads = 0, writes = 0, replaced = 0;
  let complete;
  const timers = [];
  const cache = new Map();
  const url = 'https://erp.example/logistica/rutas/296/';
  // Una respuesta antigua que nunca debe sustituir al estado del servidor.
  const key = `${url}?carga_point_estado=7`;
  cache.set(key, new Response(JSON.stringify({estado: 'RUNNING'}), {status: 202}));
  const result = {estado: 'ACTUALIZADA', mensaje: 'Carga disponible; folio 41863 pendiente no bloquea.'};
  vm.runInNewContext(workerSource, {
    URL, Response,
    self: {addEventListener: (name, fn) => {if (name === 'fetch') worker = fn;}},
    caches: {
      match: async request => {reads++; return cache.get(String(request.url || request));},
      open: async () => ({put: async (request, response) => {writes++; cache.set(request.url, response);}}),
    },
    fetch: async request => {
      if (request.url === url) return new Response('<section id="carga-operacion-point">disponible</section>');
      network++;
      if (disconnect && network === 1) throw new TypeError('offline');
      return new Response(JSON.stringify(network === (disconnect ? 2 : 1) ? {estado: 'RUNNING'} : result),
        {status: network === (disconnect ? 2 : 1) ? 202 : 200});
    },
  });
  const button = {disabled: false};
  const label = {textContent: ''};
  const reload = {hidden: true};
  const widget = {dataset: {solicitudId: resume ? '7' : '0'}, querySelector: selector => selector.includes('mensaje') ? label : reload};
  const operation = {draft: '7.5', replaceWith: () => {replaced++;}};
  const filter = {value: 'pastel'};
  let submitted;
  const form = {getAttribute: () => url, querySelector: () => button,
    addEventListener: (name, fn) => {if (name === 'erp:action-success') submitted = fn;}};
  const nodes = {'actualizar-carga-point': form, 'estado-carga-point': widget,
    'carga-operacion-point': operation, 'route-load-detail-search': filter};
  const toasts = [];
  const sandbox = {
    URL, Date: {now: () => tick},
    document: {getElementById: id => nodes[id], addEventListener: (name, fn) => {
      if (dirty && name === 'input') fn({target: {closest: () => form}});
    }},
    window: {location: {href: url}, ERPActionUI: {showToast: value => toasts.push(value), bind: () => {}}},
    DOMParser: class {parseFromString() {return {getElementById: () => ({fresh: true})};}},
    setTimeout: (fn, ms) => {timers.push(() => {tick += ms; fn();});},
    fetch: (target, options) => {
      const request = {url: String(target), method: 'GET', mode: 'cors', cache: options.cache};
      let response;
      worker({request, respondWith: promise => {response = promise;}});
      return response;
    },
  };
  // Exponer la promesa sólo al ejecutor de pruebas, sin cambiar los fuentes.
  vm.runInNewContext(consumerSource.replace('watch(Number(', 'complete = watch(Number(')
    .replace('watch(event.detail.solicitud_id);', 'complete = watch(event.detail.solicitud_id);'),
    vm.createContext({...sandbox, set complete(value) {complete = value;}}));
  if (!resume) {
    assert.equal(typeof submitted, 'function', 'Debe escuchar el recibo del POST real');
    submitted({detail: {solicitud_id: 7}});
  }
  assert.ok(complete && typeof complete.then === 'function', 'Debe observar el polling real');
  let settled = false, failure;
  complete.then(() => {settled = true;}, error => {settled = true; failure = error;});
  for (let i = 0; i < 150 && button.disabled; i++) {
    await new Promise(resolve => setImmediate(resolve));
    const timer = timers.shift();
    if (timer) timer();
  }
  await new Promise(resolve => setImmediate(resolve));
  assert.ok(settled, 'El polling no puede quedar suspendido sin resultado');
  if (failure) throw failure;
  assert.equal(button.disabled, false, 'El botón debe volver a estar disponible');
  assert.match(label.textContent, /41863 pendiente no bloquea/, 'Debe alcanzar el resultado final, no conservar202');
  assert.equal(network, disconnect ? 3 : 2, 'Debe volver a consultar Point tras202 o desconexión');
  assert.equal(reads, 0, 'no-store debe atravesar el service worker sin leer caché');
  assert.equal(writes, 0, 'El estado privado no debe entrar a caché');
  assert.equal(replaced, dirty ? 0 : 1, 'Una edición abierta impide sustituir el formulario');
  assert.equal(operation.draft, '7.5');
  assert.equal(filter.value, 'pastel');
  assert.equal(reload.hidden, false);
  assert.equal(toasts.at(-1).type, 'success');
  console.log(JSON.stringify({dirty, disconnect, resume, network, cacheReads: reads, cacheWrites: writes,
    formReplaced: replaced, draftPreserved: true, result: 'ACTUALIZADA'}));
}

async function pwaScenario({status, recarga = null, preview = false, owner = 12}) {
  const html = fs.readFileSync(process.argv[3] || path.join(root, 'logistica/templates/logistica/pwa.html'), 'utf8');
  const start = html.indexOf('      async function renderRutaCarga(');
  const end = html.indexOf('      function renderRutaScreen', start);
  assert.ok(start >= 0 && end > start, 'Debe ejecutar el render real de la app');
  const state = {
    previewRepartidor: preview, actualizandoCargaPoint: false, pantallaActual: 'ruta_carga',
    perfil: {repartidor: {id: 12}},
    cargaSucursal: {paradaId: 10, guardando: false, drafts: {9: {cantidad: '7.5', cantidadEditada: true}}},
    rutaActiva: {ruta: {id: 296, estatus: status, repartidor: owner}, recarga_carga_point_id: recarga,
      paradas: [{punto: {sucursal: 5, tipo: 'SUCURSAL'}}]},
  };
  let rendered = '';
  const sandbox = {state, render: value => {rendered = value;}, shell: value => value,
    escapeHtml: value => value, lucideIcon: () => '', renderChecklistCarga: () => 'formulario',
    loadRutaActiva: async () => state.rutaActiva};
  vm.createContext(sandbox);
  vm.runInContext(html.slice(start, end), sandbox);
  await sandbox.renderRutaCarga('', state.rutaActiva);
  const expected = !preview && owner === 12 && ['PLANEADA', 'EN_RUTA'].includes(status);
  assert.equal(rendered.includes('Actualizar carga desde Point'), expected,
    'El titular debe encontrar el botón también mientras captura el tramo actual');
  assert.equal(state.cargaSucursal.paradaId, 10);
  assert.equal(state.cargaSucursal.drafts[9].cantidad, '7.5');
  console.log(JSON.stringify({pwa: true, status, recarga, preview, owner, buttonExpected: expected, draftPreserved: true}));
}

(async () => {
  await scenario();
  await scenario({dirty: true});
  await scenario({disconnect: true});
  await scenario({resume: true});
  for (const input of [{status: 'PLANEADA'}, {status: 'EN_RUTA'}, {status: 'EN_RUTA', recarga: 1408},
    {status: 'COMPLETADA'}, {status: 'EN_RUTA', preview: true}, {status: 'EN_RUTA', owner: 89}]) {
    await pwaScenario(input);
  }
})().catch(error => {console.error(error); process.exitCode = 1;});
