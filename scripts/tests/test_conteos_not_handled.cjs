// Run: node scripts/tests/test_conteos_not_handled.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../../static/inventario/conteos.js'), 'utf8');
const start = source.indexOf("root.querySelectorAll('[data-not-handled]')");
const handler = source.slice(start, source.indexOf("form.addEventListener('submit'", start));
function setup(quantity, note) {
  const notices = [], events = [];
  const q = { value: quantity }, incidence = { value: note, maxLength: 2000, dispatchEvent: e => events.push(e) };
  const details = { open: false };
  const row = { querySelector: selector => selector === 'textarea' ? incidence : selector === 'details' ? details : q };
  const button = { closest: () => row, addEventListener: (_, fn) => { button.click = fn; } };
  vm.runInNewContext(handler, { root: { querySelectorAll: () => [button] },
    window: { ERPActionUI: { showToast: notice => notices.push(notice) } }, Event: class { constructor(type) { this.type = type; } } });
  return { q, incidence, button, notices, events };
}
const filled = setup('5', 'Nota existente');
filled.button.click();
assert.equal(filled.q.value, '5');
assert.equal(filled.incidence.value, 'Nota existente');
assert.equal(filled.notices.length, 1);
const empty = setup('', '');
empty.button.click();
assert.equal(empty.q.value, '');
assert.equal(empty.incidence.value, 'No se maneja en esta sucursal.');
assert.equal(empty.events[0].type, 'input');
empty.button.click();
assert.equal(empty.incidence.value, '');
const notes = setup('', 'Se revisó con la encargada.');
notes.button.click();
assert.match(notes.incidence.value, /Se revisó con la encargada\.$/);
const full = setup('', 'a'.repeat(2000));
full.button.click();
assert.equal(full.incidence.value.length, 2000);
assert.equal(full.events.length, 0);
assert.equal(full.notices.length, 1);
console.log('No se maneja: conserva cantidades y notas, deja cantidad vacía y respeta el límite.');
