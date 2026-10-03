// Adapter regression: real script, repeated native controls and core error lifecycle.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('static/activos/capturas.js', 'utf8');
function fixture() {
  const controls = [
    {name:'clave_captura', type:'hidden', value:'uuid-original'},
    {name:'activo_id', type:'select-one', value:'42'},
    {name:'solicitud_id', type:'hidden', value:'3'},
    {name:'solicitud_id', type:'hidden', value:'5'},
    {name:'descripcion', type:'textarea', value:'Trabajo'},
    {name:'evidencias', type:'file', files:['evidencia-original']}
  ];
  controls.forEach(control => {control.disabled=false; control.dataset={};});
  const listeners = {};
  const feedback = {textContent:''};
  const details = {open:false};
  const form = {
    dataset:{},
    querySelector: () => feedback,
    querySelectorAll: () => controls,
    closest: () => details,
    addEventListener: (name, callback) => {listeners[name]=callback;}
  };
  const draft = {clave_captura:['uuid-original'], activo_id:['42'], solicitud_id:['3','5'], descripcion:['Trabajo']};
  const document = {querySelector: () => form, getElementById: () => ({textContent:JSON.stringify(draft)})};
  vm.runInNewContext(source,{document,CustomEvent:class {},console});
  return {form, controls, details, feedback, fire:(name,detail={})=>listeners[name]({detail})};
}
const restored=fixture();
assert.deepEqual(restored.controls.filter(c=>c.name==='solicitud_id').map(c=>c.value),['3','5']);
assert.equal(restored.details.open,true);
for (const code of [400,403,404,409,410,0,500]) {
  const f=fixture();
  const snapshot={id:'original FormData'};
  f.form._captureSnapshot=snapshot;
  f.fire('erp:action-start',{formData:snapshot});
  assert.ok(f.controls.every(c=>c.disabled));
  // shared ERPActionUI clears these definitive rejected snapshots before its event.
  if ([400,403,404].includes(code)) f.form._captureSnapshot=null;
  f.fire('erp:action-error',{statusCode:code,message:'Respuesta '+code});
  if ([400,403,404].includes(code)) {
    assert.ok(f.controls.every(c=>!c.disabled),'Los controles deben poder reenviarse tras '+code);
    assert.equal(f.form._captureSnapshot,undefined);
  } else {
    assert.ok(f.controls.every(c=>c.disabled));
    assert.equal(f.form._captureSnapshot,snapshot);
  }
  assert.equal(f.controls[0].value,'uuid-original');
  assert.deepEqual(f.controls[5].files,['evidencia-original']);
  assert.deepEqual(f.controls.filter(c=>c.name==='solicitud_id').map(c=>c.value),['3','5']);
  assert.equal(f.feedback.textContent,'Respuesta '+code);
}
console.log('PASS: repeated solicitud_id; 400/403/404 unlock; 409/410/unknown/500 keep original snapshot/UUID/files.');
