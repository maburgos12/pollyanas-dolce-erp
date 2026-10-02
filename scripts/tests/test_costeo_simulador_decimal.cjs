// Run: node scripts/tests/test_costeo_simulador_decimal.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const template = fs.readFileSync(path.join(__dirname,
  '../../recetas/templates/recetas/costeo_dashboard.html'), 'utf8');
const script = template.match(/<script>([\s\S]*?)<\/script>/)[1];
const elements = new Map();
const document = {
  querySelectorAll: () => [],
  getElementById(id) {
    if (!elements.has(id)) elements.set(id, {
      value: '55', dataset: {}, style: {}, listeners: {},
      innerHTML: '', textContent: '',
      addEventListener(type, handler) { this.listeners[type] = handler; },
    });
    return elements.get(id);
  },
};
const context = { document, Intl, window: { location: { hash: '#simulador' } } };
vm.runInNewContext(script.replace(/\}\)\(\);\s*$/, 
  'globalThis.simulator = { state, els, renderLines }; })();'), context);
const { state, els, renderLines } = context.simulator;
state.lines.push({ name: 'Betún', quantity: 1, unitCost: 96.34, lineCost: 96.34 });
renderLines();
const originalRows = els.lines.innerHTML;
const subtotal = { textContent: '' };
const input = {
  value: '', dataset: { lineQty: '0' },
  closest(selector) {
    return selector === '[data-line-qty]' ? this : {
      querySelector: () => subtotal,
    };
  },
};
for (const value of ['', '0', '0.', '0.5', '0.125', '0.000001']) {
  input.value = value;
  els.lines.listeners.input({ target: input });
  assert.equal(els.lines.innerHTML, originalRows, `Input replaced while typing ${value}`);
  assert.equal(input.value, value);
  assert.equal(state.lines[0].quantity, Number(value));
  const expected = new Intl.NumberFormat('es-MX', {
    style: 'currency', currency: 'MXN',
  }).format(Number(value) * 96.34);
  assert.equal(subtotal.textContent, expected);
  assert.equal(els.total.textContent, expected);
}
console.log('Decimal input preserves the row and updates costs.');
