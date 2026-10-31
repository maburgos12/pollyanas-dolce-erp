// Run: node scripts/tests/test_rentabilidad_ia_panel.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../../rentabilidad/templates/rentabilidad/dashboard.html'), 'utf8');
const handler = source.slice(source.indexOf('async function analizarSucursal('), source.indexOf('async function analizarTodas('));
const row = { style: { display: '' } }, panel = {}, button = { textContent: 'Analizar', disabled: false };
let requests = 0, success = false;
const context = {
  document: { getElementById: id => id.startsWith('ia-row-') ? row : panel },
  getComputedStyle: element => ({ display: element.style.display || 'none' }),
  CSRF: 'test',
  fetch: async () => {
    requests++;
    return { json: async () => success ? { ok: true, resultado: { diagnostico: 'Análisis válido' } } : { ok: false, error: 'Sin crédito' } };
  },
};
vm.createContext(context);
vm.runInContext(handler, context);
(async () => {
  await context.analizarSucursal(1, button);
  assert.equal(requests, 1, 'El primer clic debe consultar aunque CSS oculte la fila');
  assert.equal(row.style.display, 'table-row');
  assert.equal(button.textContent, 'Reintentar');
  assert.match(panel.innerHTML, /Sin crédito/);
  success = true;
  await context.analizarSucursal(1, button);
  assert.equal(requests, 2, 'Reintentar debe consultar, no ocultar el error');
  assert.equal(button.textContent, 'Ocultar');
  assert.equal(button.disabled, false);
  await context.analizarSucursal(1, button);
  assert.equal(requests, 2);
  assert.equal(row.style.display, 'none');
  console.log('Panel IA: consulta al primer clic, muestra error, reintenta y permite ocultar.');
})().catch(error => { console.error(error); process.exitCode = 1; });
