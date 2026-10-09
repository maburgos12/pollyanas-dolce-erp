"use strict";
const assert = require("assert");
const fs = require("fs");
const vm = require("vm");

const template = fs.readFileSync("templates/mantenimiento/pwa.html", "utf8");
const start = template.indexOf("      function servicioDraftKey(");
const source = template.slice(start, template.indexOf("      function bloquearServicioCaptura(", start));

async function check(rendered) {
  let finishRender;
  let installed = false;
  let reads = 0;
  let bindings = 0;
  let locked = false;
  const description = {value: ""};
  const draft = {clave: "test-key", modo: "pendiente", valores: {descripcion: "Borrador conservado", fecha_objetivo: "2026-10-04"}, intento: {descripcion: "Borrador conservado", fecha_objetivo: "2026-10-04"}};
  const context = {
    state: {requestGeneration: {capture: 0}, servicioDraft: draft, catalogos: {}, sucursales: [], activos: [], unidades: []},
    ensureSucursales: async () => {}, ensureActivos: async () => {}, ensureVehiculoCatalogos: async () => {},
    ensureCatalogos: async () => {}, ensureProveedores: async () => {},
    esc: String, shell: (html) => html, providerOptions: () => "", actualizarAlcanceServicio() {},
    bloquearServicioCaptura(value) { locked = value; },
    render: () => new Promise(resolve => { finishRender = () => { installed = rendered; resolve(rendered); }; }),
    document: {
      querySelector(selector) { reads += 1; assert.ok(installed, "El borrador se restauró antes de instalar el DOM"); return selector.includes('descripcion') ? description : {value: ""}; },
      getElementById(id) {
        reads += 1;
        assert.ok(installed, "Se conectaron eventos antes de instalar el DOM");
        return id === "servicio-alcance" ? {value: "activo"} : {addEventListener() { bindings += 1; }};
      }
    }
  };
  vm.createContext(context);
  vm.runInContext(source, context);
  const pending = context.renderServicioPuntual();
  await new Promise(resolve => setImmediate(resolve));
  assert.strictEqual(reads, 0);
  finishRender();
  await pending;
  if (rendered) {
    assert.strictEqual(description.value, "Borrador conservado");
    assert.strictEqual(bindings, 2);
    assert.ok(locked);
  } else {
    assert.strictEqual(reads, 0);
    assert.strictEqual(bindings, 0);
  }
}

function checkNativeClose(checked) {
  const dashboard = fs.readFileSync("templates/mantenimiento/dashboard.html", "utf8");
  const start = dashboard.indexOf('  {% if captura_error %}\n  const restored');
  assert.ok(start !== -1);
  const source = dashboard.slice(start, dashboard.indexOf('  {% endif %}', start)).replace('  {% if captura_error %}', '');
  const data = {modo_servicio: "realizado", descripcion: ""};
  if (checked) data.cerrar_servicio = "1";
  const close = {type: "checkbox", checked: false};
  const context = {
    document: {getElementById() { return {textContent: JSON.stringify(data)}; }},
    setModo() { close.checked = true; }, setAlcance() {}, filtrarPorSucursal() {},
    alcance: {value: "activo"}, cerrar: close,
    form: {elements: {namedItem(name) { return name === "cerrar_servicio" ? close : null; }}},
    modal: {classList: {add() {}}}, servicioIniciado: false
  };
  vm.runInNewContext(source, context);
  assert.strictEqual(close.checked, checked, "El fallback HTML debe preservar Cerrar servicio del intento original");
}

function checkNewCaptureSelectReset() {
  const dashboard = fs.readFileSync("templates/mantenimiento/dashboard.html", "utf8");
  const start = dashboard.indexOf("  function clearForm() {");
  const source = dashboard.slice(start, dashboard.indexOf("  function filtrarOpciones(", start));
  const controls = ["sucursal", "activo", "unidad", "instalacion"].map(name => {
    const select = {name, value: "old-selection", visible: "old-selection", changes: 0};
    Object.defineProperty(select, "selectedIndex", {set(index) { if (index === 0) this.value = ""; }});
    select.dispatchEvent = function(event) {
      assert.strictEqual(event.type, "change");
      assert.ok(event.bubbles);
      this.visible = this.value;
      this.changes += 1;
    };
    return select;
  });
  const context = {
    sucursal: controls[0], activo: controls[1], unidad: controls[2], instalacion: controls[3],
    document: {getElementById() { return {value: "old"}; }},
    setSelectValue() {}, filtrarPorSucursal() {},
    Event: function(type, options) { this.type = type; this.bubbles = options.bubbles; }
  };
  vm.runInNewContext(source, context);
  context.clearForm();
  for (const select of controls) {
    assert.strictEqual(select.value, "");
    assert.strictEqual(select.visible, "", `${select.name}: selección visible desactualizada`);
    assert.strictEqual(select.changes, 1);
  }
}

(async () => {
  checkNewCaptureSelectReset();
  checkNativeClose(false);
  checkNativeClose(true);
  await check(true);
  await check(false);
  console.log("capturas PWA render harness: ok");
})().catch(error => { console.error(error); process.exit(1); });
