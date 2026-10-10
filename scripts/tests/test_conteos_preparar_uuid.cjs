// Run: node scripts/tests/test_conteos_preparar_uuid.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { webcrypto } = require('node:crypto');
const source = fs.readFileSync(path.join(__dirname, '../../static/inventario/conteos_preparar.js'), 'utf8');
const uuid = source.slice(source.indexOf('function uuid()'), source.indexOf('function summary()'));
// Probar el API disponible en navegadores que carecen de randomUUID.
const context = vm.createContext({ window: { crypto: { getRandomValues: webcrypto.getRandomValues.bind(webcrypto) } } });
vm.runInContext(uuid, context);
const ids = Array.from({ length: 100 }, () => vm.runInContext('uuid()', context));
for (const id of ids) assert.match(id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
assert.equal(new Set(ids).size, ids.length);
console.log('Preparación de conteos: UUID v4 válido sin randomUUID.');
const preserve = source.slice(source.indexOf('selected.forEach('), source.indexOf('catalogBranch=branch;'));
const row = { hidden: false, checkbox: { checked: true, value: 'p1' } };
const appended = [];
const display = vm.createContext({ selected: new Map([['p1', row]]), search: { elements: { q: { value: 'PMH028' } } }, fragment: { appendChild: item => appended.push(item) } });
vm.runInContext(preserve, display);
assert.equal(row.hidden, true);
assert.equal(row.checkbox.checked, true);
assert.equal(appended[0], row);
display.search.elements.q.value = '';
vm.runInContext(preserve, display);
assert.equal(row.hidden, false);
console.log('Búsqueda: selecciones sin coincidencia ocultas, conservadas y visibles al limpiar.');
