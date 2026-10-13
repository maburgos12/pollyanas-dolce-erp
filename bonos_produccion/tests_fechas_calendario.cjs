const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const html = fs.readFileSync(`${__dirname}/templates/bonos_produccion/index.html`, 'utf8');
const context = {
  Date, Intl, MESES: ['', 'Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'],
  h: (tag, props, ...children) => ({tag, props, children}),
  React: {useState: () => [null, () => {}], useEffect: () => {}},
  AreaChips: '', EmployeeSearchList: '', Badge: '', BonusRight: '',
  AREA_TODAS: 'TODAS', AREA_LABELS: {ARMADO: 'Armado'}, RRHH_LABELS: {},
  initials: () => 'GL', money: String,
};
vm.createContext(context);
vm.runInContext(html.slice(html.indexOf('function fechaLocal('), html.indexOf('const IconFactory=')), context);
vm.runInContext(html.slice(html.indexOf('function CapturaTab('), html.indexOf('function ResumenTab(')), context);
context.fechaHoy = () => '2026-10-13';
const periodo = {fecha_inicio: '2026-09-28', fecha_fin: '2026-10-29'};
const registros = [28,29].map(dia => ({id: dia, dia, fecha: `2026-09-${dia}`, tiene_asistencia: true}));
const fechas = context.fechasDelCorte(periodo, null, 10, 2026);
assert.equal(context.fechaRegistro({dia:28}, fechas), null, 'No adivinar fecha legacy ambigua');
assert.equal(context.fechaRegistro({dia:2}, fechas), '2026-10-02');
const calls = [];
const tree = context.CapturaTab({
  bonosArea: [], area: 'ARMADO', selectedBono: {id:138, empleado_nombre:'Prueba', area:'ARMADO'},
  registros, periodo, mes:10, anio:2026,
  guardarDia: (fecha, patch) => {calls.push({fecha,patch});return Promise.resolve();},
});
const cells = [];
function walk(node) {
  if (Array.isArray(node)) return node.forEach(walk);
  if (!node || typeof node !== 'object') return;
  if (node.props?.className?.startsWith('day-cell ')) cells.push(node);
  walk(node.children);
}
walk(tree);
assert.equal(cells.length, 32);
assert.equal(cells.filter(c => c.props.className.includes('worked')).length, 2);
for (const day of ['28','29']) {
  const sep = cells.find(c => c.props.title === `2026-09-${day}`);
  const oct = cells.find(c => c.props.title === `2026-10-${day}`);
  assert.ok(sep.props.className.includes('worked'));
  assert.ok(!oct.props.className.includes('worked'));
  assert.equal(oct.props.disabled, true);
  oct.props.onClick();
}
assert.equal(calls.length, 0, 'Celdas futuras no envían cambios');
cells.find(c => c.props.title === '2026-09-28').props.onClick();
assert.equal(calls[0].fecha, '2026-09-28');
assert.equal(calls[0].patch.tiene_asistencia, false);
console.log('Calendario: fecha completa, legado ambiguo, fechas futuras y captura correctos');
