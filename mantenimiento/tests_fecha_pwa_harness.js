"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const template = fs.readFileSync("templates/mantenimiento/pwa.html", "utf8");
function source(from, to) {
  const start = template.indexOf(from);
  const end = template.indexOf(to, start);
  assert.ok(start >= 0 && end > start, `Bloque real no encontrado: ${from}`);
  return template.slice(start, end);
}
const actual = [
  source("      function emptyVehiculoDraft()", "      function setTokens("),
  source("      function fechaCalendarioValida(", "      function setServicioFieldState("),
  source("      function startVehiculo()", "      function openClosedHistory("),
].join("\n");
const tick = () => new Promise(resolve => setImmediate(resolve));
const ok = fecha => ({ok: true, json: async () => ({fecha})});
function setup() {
  const controls = {};
  const names = {
    "servicio-modo": "modo_servicio", "servicio-alcance": "alcance",
    "servicio-sucursal": "sucursal_id", "servicio-activo": "activo_id", "servicio-unidad": "unidad_id",
    "servicio-instalacion": "instalacion_categoria", "servicio-fecha": "fecha_objetivo",
    "servicio-proveedor": "proveedor_servicio", "servicio-descripcion": "descripcion",
    "servicio-costo": "costo_total", "servicio-nota": "nota_trabajo",
  };
  for (const [id, name] of Object.entries(names)) controls[id] = {name, value: "", disabled: false};
  controls["servicio-alcance"].value = "activo";
  controls["servicio-descripcion"].value = "Trabajo de prueba";
  controls["servicio-guardar"] = {};
  controls["servicio-error"] = {};
  const section = {addEventListener(type, callback) { this.input = callback; }, querySelectorAll() { return Object.values(controls).filter(c => c.name); }};
  const state = {
    pantalla: "dashboard", requestGeneration: {capture: 0}, servicioDraft: null,
    resumen: {fecha: "2020-01-01", agenda: ["NO MUTAR"]}, catalogos: {},
    sucursales: [], activos: [], unidades: [], tiposServicio: [], proveedores: [], bandeja: [1],
  };
  const output = {html: [], reads: [], posts: [], responder: () => ok("2026-10-04")};
  const context = {
    state, crypto: require("node:crypto"), FormData,
    Date: class { constructor() { throw new Error("La captura no debe leer el reloj del dispositivo"); } },
    esc: String, shell: html => html, progressDots: () => "", providerOptions: () => "",
    ensureSucursales: async () => {}, ensureActivos: async () => {}, ensureCatalogos: async () => {}, ensureProveedores: async () => {},
    parseListResponse: async () => [], actualizarAlcanceServicio() {},
    render: async html => {
      output.html.push(html);
      if (html.includes('id="servicio-fecha"')) {
        controls["servicio-fecha"].value = /id="servicio-fecha"[^>]*value="([^"]*)"/.exec(html)[1];
        controls["servicio-modo"].value = /id="servicio-modo"[^>]*value="([^"]*)"/.exec(html)[1];
      }
      return true;
    },
    apiFetch: async (path, options = {}) => {
      if (options.method === "POST") {
        output.posts.push({path, options});
        return output.postResponder ? output.postResponder() : {ok: true, json: async () => ({id: 1})};
      }
      if (path === "/resumen/") {
        output.reads.push(options);
        return output.responder();
      }
      return {ok: true, json: async () => []};
    },
    document: {
      getElementById: id => id === "servicio-captura" ? section : controls[id],
      querySelector: selector => Object.values(controls).find(c => c.name === /name="([^"]+)"/.exec(selector)?.[1]),
      querySelectorAll: () => Object.values(controls).filter(c => c.name),
    },
    showScreen(name) {
      state.pantalla = name;
      state.requestGeneration.capture += 1;
      if (name === "vehiculo") output.screen = context.renderVehiculoDraft();
    },
  };
  vm.createContext(context);
  vm.runInContext(actual, context);
  state.vehiculoDraft = context.emptyVehiculoDraft();
  context.ensureVehiculoCatalogos = async () => {};
  return {context, state, output, controls, section};
}
async function checkRead() {
  const {context, state, output} = setup();
  const original = state.resumen;
  for (const valid of ["2026-10-04", "2024-02-29", "2000-02-29", "0001-01-01"]) {
    output.responder = () => ok(valid);
    assert.equal(await context.leerFechaCapturaERP(), valid);
  }
  for (const invalid of [null, undefined, 20261004, "2026-10-04T00:00:00Z", "2026-1-04", "2026-02-29", "1900-02-29", "2026-04-31", "2026-00-01", "2026-13-01", "2026-01-00", "0000-01-01"]) {
    output.responder = () => ok(invalid);
    await assert.rejects(context.leerFechaCapturaERP());
  }
  for (const responder of [() => { throw Error("offline"); }, () => ({ok: false, status: 500}), () => ({ok: true, json: async () => { throw Error("HTML no JSON"); }})]) {
    output.responder = responder;
    await assert.rejects(context.leerFechaCapturaERP());
  }
  assert.strictEqual(state.resumen, original);
  assert.deepEqual(state.resumen.agenda, ["NO MUTAR"]);
  assert.ok(output.reads.every(options => options.cache === "no-store"));
}
async function checkService() {
  const {context, state, output, controls, section} = setup();
  await context.renderServicioPuntual("pendiente");
  const draft = state.servicioDraft;
  const clave = draft.clave;
  assert.equal(controls["servicio-fecha"].value, "2026-10-04");
  controls["servicio-fecha"].value = "2026-10-03"; // La fecha sigue editable.
  section.input();
  output.responder = () => { throw Error("No leer fecha otra vez"); };
  await context.renderServicioPuntual();
  assert.equal(controls["servicio-fecha"].value, "2026-10-03");
  output.postResponder = () => { throw Error("Confirmación perdida"); };
  await context.guardarServicioMovil();
  const intento = draft.intento;
  const original = output.posts[0].options.body;
  assert.equal(JSON.parse(original).fecha_objetivo, "2026-10-03");
  draft.valores = {...draft.valores, fecha_objetivo: "2026-10-09"};
  await context.renderServicioPuntual();
  assert.equal(controls["servicio-fecha"].value, "2026-10-03", "El intento congelado prevalece");
  await context.guardarServicioMovil();
  assert.equal(output.posts[1].options.body, original);
  assert.strictEqual(draft.intento, intento);
  assert.equal(draft.clave, clave);
  assert.equal(output.reads.length, 1);
  assert.equal(state.resumen.fecha, "2020-01-01");
  // El flujo vigente limpia el borrador sólo después de éxito, y el nuevo toma fecha fresca.
  output.postResponder = () => ({ok: true, json: async () => ({id: 1})});
  await context.guardarServicioMovil();
  assert.equal(state.servicioDraft, null);
  output.responder = () => ok("2026-10-05");
  await context.renderServicioPuntual();
  assert.equal(controls["servicio-fecha"].value, "2026-10-05");
  assert.notEqual(state.servicioDraft.clave, clave);
}
async function checkVehicle(modo) {
  const {context, state, output} = setup();
  context.startVehiculo();
  await output.screen;
  const draft = state.vehiculoDraft;
  Object.assign(draft, {step: 3, modo, unidad: 8, unidadLabel: "Camión", tipoServicio: 2, descripcionFalla: "Frenos", costo: "100"});
  await context.renderVehiculoDraft();
  assert.ok(output.html.at(-1).includes(`<span>2026-10-04</span>`));
  output.responder = () => ok("2026-10-05"); // Cruza medianoche: la captura abierta no cambia.
  output.postResponder = () => ({ok: false, json: async () => ({error: "Revisar"})});
  await context.enviarVehiculo();
  await context.enviarVehiculo();
  for (const post of output.posts) assert.equal(post.options.body.get(modo === "servicio" ? "fecha" : "fecha_ingreso"), "2026-10-04");
  assert.strictEqual(state.vehiculoDraft, draft);
  assert.equal(output.reads.length, 1);
  assert.ok(output.html.at(-1).includes(`<span>2026-10-04</span>`));
  output.postResponder = () => { throw Error("Red desconectada"); };
  await assert.rejects(context.enviarVehiculo()); // P7 conserva la fecha; no introduce idempotencia/recovery de flota.
  assert.equal(draft.fecha, "2026-10-04");
  context.startVehiculo();
  await output.screen;
  assert.notStrictEqual(state.vehiculoDraft, draft);
  assert.equal(state.vehiculoDraft.fecha, "2026-10-05");
}
async function checkFailureRetry(kind, responder) {
  const {context, state, output} = setup();
  output.responder = responder;
  const begin = kind === "servicio" ? () => context.renderServicioPuntual() : () => { state.pantalla = "vehiculo"; return context.renderVehiculoDraft(); };
  await begin();
  const draft = kind === "servicio" ? state.servicioDraft : state.vehiculoDraft;
  assert.ok(output.html.at(-1).includes("Reintentar"));
  assert.ok(!output.html.at(-1).includes("Guardar"));
  assert.equal(draft.fecha, "");
  assert.equal(state.resumen.fecha, "2020-01-01");
  output.responder = () => ok("2026-10-05");
  await begin();
  assert.strictEqual(kind === "servicio" ? state.servicioDraft : state.vehiculoDraft, draft);
  assert.equal(draft.fecha, "2026-10-05");
}
async function checkRace(kind, navigate) {
  const {context, state, output} = setup();
  let resolveOld;
  output.responder = () => new Promise(resolve => { resolveOld = resolve; });
  const begin = () => kind === "servicio" ? context.renderServicioPuntual() : context.renderVehiculoDraft();
  if (kind === "vehiculo") state.pantalla = "vehiculo";
  const pending = begin();
  await tick();
  const old = kind === "servicio" ? state.servicioDraft : state.vehiculoDraft;
  if (navigate) context.showScreen("dashboard");
  else {
    if (kind === "servicio") state.servicioDraft = null;
    else state.vehiculoDraft = context.emptyVehiculoDraft();
    output.responder = () => ok("2026-10-05");
    await begin();
  }
  const renders = output.html.length;
  resolveOld(ok("2026-10-04"));
  await pending;
  assert.equal(old.fecha, "", "La solicitud vieja no debe escribir el borrador");
  assert.equal(output.html.length, renders, "La respuesta tardía no debe pintar sobre otra pantalla/captura");
  if (!navigate) assert.equal((kind === "servicio" ? state.servicioDraft : state.vehiculoDraft).fecha, "2026-10-05");
}
function realRendering(test) {
  const {context, state, output} = test;
  const timers = new Map();
  let serial = 0;
  let html = "Pantalla anterior";
  let finishDestination;
  const destination = new Promise(resolve => { finishDestination = resolve; });
  const app = {
    classList: {add() {}, remove() {}},
    get innerHTML() { return html; },
    set innerHTML(value) { html = value; output.html.push(value); },
  };
  context.app = app;
  context.window = {
    setTimeout(callback, ms) { assert.equal(ms, 140); timers.set(++serial, callback); return serial; },
    clearTimeout(id) { timers.delete(id); },
  };
  context.revokeEvidenceUrls = () => {};
  context.loadBandeja = () => destination;
  context.loadResumen = () => destination;
  context.metricasMantenimiento = () => ({abiertas: 0, proceso: 0, cerradas: 0});
  context.appGlyph = () => "";
  context.quickActions = () => "";
  state.perfil = {};
  state.counts = {};
  state.requestGeneration.detail = 0;
  state.requestGeneration.history = 0;
  state.resumen = {fecha: "2020-01-01", agenda: []};
  vm.runInContext([
    source("      let pendingRenderTimer =", "      function appGlyph("),
    source("      async function showScreen(", "      function renderLogin("),
    source("      async function renderDashboard(", "      async function renderAgenda("),
  ].join("\n"), context);
  function flush() {
    const callbacks = [...timers.values()];
    timers.clear();
    for (const callback of callbacks) callback();
  }
  return {app, timers, flush, finishDestination};
}
async function checkDOMCommitRace(kind, stage) {
  const test = setup();
  const {context, state, output} = test;
  const real = realRendering(test);
  if (kind === "vehiculo") state.pantalla = "vehiculo";
  if (stage === "form") {
    if (kind === "servicio") state.servicioDraft = {clave: "original", modo: "realizado", valores: {descripcion: "Conservar"}, intento: null, fecha: "2026-10-04"};
    else Object.assign(state.vehiculoDraft, {fecha: "2026-10-04", descripcionFalla: "Conservar"});
  }
  if (stage === "error") output.responder = () => ok("2026-02-29");
  const pending = kind === "servicio" ? context.renderServicioPuntual() : context.renderVehiculoDraft();
  await tick();
  if (stage === "error") {
    real.flush(); // Instala carga, luego programa error; no instala error aún.
    await tick();
  }
  assert.equal(real.timers.size, 1, `${kind}/${stage}: render de captura pendiente`);
  const before = real.app.innerHTML;
  const draft = kind === "servicio" ? state.servicioDraft : state.vehiculoDraft;
  const navigating = context.showScreen("dashboard"); // Destino real esperando loadBandeja/loadResumen.
  assert.equal(state.pantalla, "dashboard");
  real.flush(); // Los 140 ms vencen ANTES que la pantalla destino tenga datos.
  await pending;
  assert.equal(real.app.innerHTML, before, `${kind}/${stage}: no instalar DOM obsoleto durante navegación`);
  assert.strictEqual(kind === "servicio" ? state.servicioDraft : state.vehiculoDraft, draft);
  if (stage === "form") assert.equal(kind === "servicio" ? draft.valores.descripcion : draft.descripcionFalla, "Conservar");
  real.finishDestination();
  await tick();
  real.flush();
  await navigating;
  assert.ok(real.app.innerHTML.includes("Bandeja de mantenimiento"));
}
async function checkSupersededDOMCommit(kind) {
  const test = setup();
  const {context, state} = test;
  const real = realRendering(test);
  if (kind === "servicio") state.servicioDraft = {clave: "viejo", modo: "realizado", valores: {}, fecha: "2026-10-04"};
  else { state.pantalla = "vehiculo"; state.vehiculoDraft.fecha = "2026-10-04"; }
  const begin = () => kind === "servicio" ? context.renderServicioPuntual() : context.renderVehiculoDraft();
  const old = begin();
  await tick();
  assert.equal(real.timers.size, 1);
  let releaseCatalog;
  const catalog = new Promise(resolve => { releaseCatalog = resolve; });
  if (kind === "servicio") {
    state.servicioDraft = {clave: "nuevo", modo: "realizado", valores: {}, fecha: "2026-10-05"};
    context.ensureSucursales = () => catalog;
  } else {
    state.vehiculoDraft = context.emptyVehiculoDraft();
    state.vehiculoDraft.fecha = "2026-10-05";
    context.ensureVehiculoCatalogos = () => catalog;
  }
  const current = begin();
  real.flush(); // Captura anterior caducó, aunque la nueva aún espera su catálogo.
  await old;
  assert.equal(real.app.innerHTML, "Pantalla anterior");
  releaseCatalog();
  await tick();
  real.flush();
  await current;
  assert.notEqual(real.app.innerHTML, "Pantalla anterior");
  assert.equal((kind === "servicio" ? state.servicioDraft : state.vehiculoDraft).fecha, "2026-10-05");
}
async function checkOtherRenderCallers() {
  const test = setup();
  const {context} = test;
  const real = realRendering(test);
  const superseded = context.render("Anterior");
  const current = context.render("Vigente");
  assert.equal(await superseded, false, "Conservar resolución del render reemplazado");
  real.flush();
  assert.equal(await current, true);
  assert.equal(real.app.innerHTML, "Vigente", "Los callers sin guard siguen instalando su DOM");
  let focused = false;
  context.document.getElementById = () => ({focus() { focused = true; }});
  const focusRender = context.render("Con foco", "input-existente");
  real.flush();
  assert.equal(await focusRender, true);
  assert.ok(focused, "Conservar autofocus de los callers existentes");
}
(async () => {
  await checkOtherRenderCallers();
  for (const kind of ["servicio", "vehiculo"]) {
    for (const stage of ["loading", "form", "error"]) await checkDOMCommitRace(kind, stage);
    await checkSupersededDOMCommit(kind);
  }
  await checkRead();
  await checkService();
  await checkVehicle("servicio");
  await checkVehicle("reparacion");
  for (const kind of ["servicio", "vehiculo"]) {
    for (const responder of [() => { throw Error("offline"); }, () => ({ok: false, status: 500}), () => ({ok: true, json: async () => { throw Error("Invalid JSON"); }}), () => ok("2026-02-29")]) await checkFailureRetry(kind, responder);
    await checkRace(kind, true);
    await checkRace(kind, false);
  }
  console.log("fecha ERP PWA: calendario, fuente independiente del reloj, fecha visible/payload, retry, nuevas capturas y races: PASS");
})().catch(error => { console.error(error); process.exit(1); });
