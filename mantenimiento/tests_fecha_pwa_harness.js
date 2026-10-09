"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const template = fs.readFileSync(process.env.PWA_TEMPLATE || "templates/mantenimiento/pwa.html", "utf8");
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
function setup(store = {getItem() {return null;}, setItem() {}, removeItem() {}}, actor = 7) {
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
    perfil: {id: actor}, pantalla: "dashboard", requestGeneration: {capture: 0}, servicioDraft: null,
    resumen: {fecha: "2020-01-01", agenda: ["NO MUTAR"]}, catalogos: {},
    sucursales: [], activos: [], unidades: [], tiposServicio: [], proveedores: [], bandeja: [1],
  };
  const output = {html: [], reads: [], posts: [], responder: () => ok("2026-10-04")};
  const context = {
    state, crypto: require("node:crypto"), FormData,
    sessionStorage: store,
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
  // El éxito conserva el recibo hasta la acción explícita de otra captura.
  output.postResponder = () => ({ok: true, json: async () => ({id: 1})});
  await context.guardarServicioMovil();
  assert.equal(state.servicioDraft.resultado.id, 1);
  await context.guardarServicioMovil();
  assert.equal(output.posts.length, 3, "La confirmación impide un segundo POST");
  output.responder = () => ok("2026-10-05");
  await context.nuevaCapturaServicio();
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
function recoveryStorage() {
  const map = new Map();
  return {map, getItem: key => map.get(key) || null, setItem: (key, value) => map.set(key, value), removeItem: key => map.delete(key)};
}
async function checkRecovery() {
  const store = recoveryStorage();
  const first = setup(store);
  await first.context.renderServicioPuntual("pendiente");
  first.controls["servicio-fecha"].value = "2026-10-02";
  first.controls["servicio-descripcion"].value = "Trabajo pendiente exacto";
  first.section.input();
  const editable = setup(store);
  await editable.context.renderServicioPuntual();
  assert.equal(editable.controls["servicio-fecha"].value, "2026-10-02");
  assert.equal(editable.controls["servicio-descripcion"].value, "Trabajo pendiente exacto");
  assert.equal(editable.output.reads.length, 0, "El borrador restaurado conserva la fecha original");
  first.output.postResponder = () => {throw Error("Respuesta perdida después de commit");};
  await first.context.guardarServicioMovil();
  const original = first.output.posts[0].options.body;
  const frozen = setup(store);
  await frozen.context.renderServicioPuntual();
  assert.equal(frozen.state.servicioDraft.pending, false);
  assert.equal(JSON.stringify(frozen.state.servicioDraft.intento), original);
  assert.ok(frozen.controls["servicio-fecha"].disabled);
  frozen.controls["servicio-fecha"].value = "2099-01-01"; // Incluso una mutación externa no sustituye el payload.
  frozen.output.postResponder = () => ({ok: true, status: 200, json: async()=>({id: 17, folio: "OM-17"})});
  await frozen.context.guardarServicioMovil();
  assert.equal(frozen.output.posts[0].options.body, original);
  assert.equal(frozen.state.servicioDraft.resultado.id, 17);
  const receipt = setup(store);
  await receipt.context.renderServicioPuntual();
  await receipt.context.guardarServicioMovil();
  assert.equal(receipt.output.posts.length, 0);
  assert.ok(receipt.output.html.at(-1).includes("openItemDetail('orden:17'"));
  const old = receipt.state.servicioDraft.clave;
  receipt.output.responder = () => ok("2026-10-05");
  await receipt.context.nuevaCapturaServicio();
  assert.notEqual(receipt.state.servicioDraft.clave, old);
  assert.equal(receipt.controls["servicio-fecha"].value, "2026-10-05");
  // Reload rejections editable, uncertain responses frozen; success navigation never touches another screen.
  for (const status of [400,403,404,409,410,500,0]) {
    const stored = recoveryStorage(), test = setup(stored);
    await test.context.renderServicioPuntual();
    test.output.postResponder = () => {if(!status) throw Error("offline");return {ok:false,status,json:async()=>({error:"Error"})};};
    await test.context.guardarServicioMovil();
    const after = setup(stored);await after.context.renderServicioPuntual();
    assert.equal(!!after.state.servicioDraft.intento, ![400,403,404].includes(status));
    assert.equal(after.state.servicioDraft.clave, test.state.servicioDraft.clave);
  }
  const foreign = setup(store, 8);await foreign.context.renderServicioPuntual();
  assert.equal(foreign.state.servicioDraft.resultado, null);
  assert.equal(foreign.state.servicioDraft.intento, null);
  const corrupt = recoveryStorage();corrupt.setItem("mantenimiento:servicio-equipo:v1:7", "{broken");
  const broken = setup(corrupt);await broken.context.renderServicioPuntual();await broken.context.guardarServicioMovil();
  assert.ok(broken.state.servicioDraft.bloqueado);assert.equal(broken.output.posts.length,0);
  const denied = {getItem(){throw Error("denied");},setItem(){throw Error("quota");},removeItem(){throw Error("denied");}};
  const noStorage = setup(denied);await noStorage.context.renderServicioPuntual();
  noStorage.output.postResponder = ()=>{throw Error("offline");};await noStorage.context.guardarServicioMovil();
  assert.ok(noStorage.state.servicioDraft.bloqueado);assert.equal(noStorage.output.posts.length,0,"GET desconocido bloquea otro UUID automático");
  await noStorage.context.nuevaCapturaServicio();await noStorage.context.guardarServicioMovil();
  assert.equal(noStorage.output.posts.length,1,"Otra captura explícita permite continuar");
  const quota={getItem(){return null;},setItem(){throw Error("quota");},removeItem(){}};
  const firstUse=setup(quota);await firstUse.context.renderServicioPuntual();firstUse.output.postResponder=()=>{throw Error("offline");};await firstUse.context.guardarServicioMovil();
  assert.equal(firstUse.output.posts.length,1);assert.ok(firstUse.controls["servicio-error"].textContent.includes("no puede conservar"));
  // Build an actual complete uncertain receipt for negative schema cases.
  const schemaStore=recoveryStorage(), schema=setup(schemaStore);await schema.context.renderServicioPuntual('pendiente');
  schema.controls['servicio-sucursal'].value='3';schema.controls['servicio-activo'].value='1';schema.section.input();
  schema.output.postResponder=()=>{throw Error('lost');};await schema.context.guardarServicioMovil();
  const validRecord=JSON.parse(schemaStore.getItem('mantenimiento:servicio-equipo:v1:7'));
  for(const mutate of [
    r=>{delete r.intento;},r=>{r.intento=false;},r=>{r.intento=null;},r=>{delete r.fecha;},r=>{r.fecha=null;},r=>{r.resultado=false;},r=>{delete r.resultado;},r=>{delete r.status;},r=>{r.status='editable';r.intento=null;},
    r=>{delete r.intento.modo_servicio;}, r=>{r.intento.modo_servicio='realizado';},
    r=>{delete r.intento.sucursal_id;}, r=>{r.intento.activo_id=null;},
    r=>{r.intento.csrfmiddlewaretoken='forbidden';}, r=>{r.intento.extra='unexpected';},
    r=>{r.valores.fecha_objetivo='2026-10-03';},r=>{r.resultado={id:1,folio:null};},
    r=>{r.resultado={id:1,folio:'OM-1',token:'forbidden'};},
  ]) {
    const tampered=recoveryStorage(), data=JSON.parse(JSON.stringify(validRecord));mutate(data);
    tampered.setItem('mantenimiento:servicio-equipo:v1:7',JSON.stringify(data));
    const negative=setup(tampered);await negative.context.renderServicioPuntual();await negative.context.guardarServicioMovil();
    assert.ok(negative.state.servicioDraft.bloqueado,'Schema incompleto/cambiado debe pedir revisión');
    assert.equal(negative.output.posts.length,0,'Nunca enviar intento corrupto');
  }
  for(const alcance of ["unidad","instalacion"]) {
    const legacyStore = recoveryStorage(), legacy = setup(legacyStore);
    await legacy.context.renderServicioPuntual();legacy.controls["servicio-alcance"].value=alcance;
    legacy.output.postResponder = ()=>{throw Error("legacy offline");};await legacy.context.guardarServicioMovil();
    assert.equal(legacy.state.servicioDraft.intento,null);assert.equal(legacyStore.map.size,0);
    assert.ok(!JSON.parse(legacy.output.posts[0].options.body).clave_captura);
    legacy.output.postResponder=()=>({ok:true,json:async()=>({id:33})});await legacy.context.guardarServicioMovil();
    assert.equal(legacy.state.servicioDraft,null);assert.equal(legacy.state.pantalla,"pendientes");
  }
  for(const success of [false,true]) {
    const raceStore=recoveryStorage(), race=setup(raceStore);
    await race.context.renderServicioPuntual();
    let finish;race.output.postResponder=()=>new Promise(resolve=>{finish=()=>resolve({ok:success,status:success?200:400,json:async()=>success?({id:18,folio:"OM-18"}):({error:"invalid"})});});
    const pending = race.context.guardarServicioMovil();await tick();
    race.context.showScreen("historial");const htmlCount=race.output.html.length;
    race.context.document.getElementById=()=>{throw Error("No tocar DOM de otra pantalla");};
    finish();await pending;assert.equal(race.output.html.length,htmlCount);
    const recovered = setup(raceStore);await recovered.context.renderServicioPuntual();
    assert.equal(!!recovered.state.servicioDraft.resultado,success);
    assert.equal(recovered.state.servicioDraft.intento === null,!success);
  }
  console.log("PWA recovery: fresh DOM/storage, editable/frozen/confirmed, same payload/UUID/date, explicit new, errors, namespaces, legacy, stale DOM: PASS");
}
(async () => {
  await checkRecovery();
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
