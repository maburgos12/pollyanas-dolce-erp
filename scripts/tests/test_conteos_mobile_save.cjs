// Run: node scripts/tests/test_conteos_mobile_save.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, '../../static/inventario/conteos.js'), 'utf8');
const start = source.indexOf('    async function save(');
const code = source.slice(start, source.indexOf('    function persist()', start));
function setup() {
  const requests = [], notices = [], stateChanges = [];
  let current = { cantidad_1: '2.5' }, receipt, transport;
  const version = { value: '2' };
  const root = { dataset: { count: '44', version: '2' }, querySelectorAll: () => [version] };
  const elements = [{ disabled: false }];
  Object.assign(elements, { version, request_id: { value: 'intent-1' }, csrfmiddlewaretoken: { value: 'csrf' } });
  const form = { elements, onformdata: null, action: { name: 'shadowed-by-buttons' }, getAttribute: () => '/app/conteos/44/accion/' };
  class FormData {
    constructor(input) { this.data = new Map(input ? [['version',version.value],['request_id',elements.request_id.value],['lecturas_json',JSON.stringify(current)]] : []); }
    set(k,v) { this.data.set(k,v); } append(k,v) { this.set(k,v); } get(k) { return this.data.get(k); } entries() { return this.data.entries(); }
  }
  const context = { root, form, activeRoot: root, saveTimer:null, saving:false, stopped:false, conflict:false, pending:null,
    navigator:{onLine:true}, confirmed:null, valid:()=>true, values:()=>({...current}),
    window:{clearTimeout(){},setTimeout(){return 1;},scrollY:0}, sessionStorage:{setItem(){}},
    state:(_,message)=>stateChanges.push(message), notice:message=>notices.push(message),
    persist(){}, scheduleSave(){}, remove(){}, key:()=>'', uuid:()=> 'next-intent', report(){},
    Blob, AbortController, FormData,
    DOMParser:class { parseFromString(){return {querySelector:()=>receipt};} },
    fetch:async(url,options)=>{requests.push({url,body:Object.fromEntries(options.body.entries())});return transport(options);}
  };
  vm.createContext(context);vm.runInContext(code,context);
  const success = (options, serverVersion='3') => {
    receipt={dataset:{ack:options.body.get('request_id'),count:'44',version:serverVersion}};
    return {ok:true,headers:{get:()=> 'application/json'},json:async()=>({ok:true,result:{version:3},html:'server html'})};
  };
  transport=success;
  return {context,requests,notices,stateChanges,form,success,setTransport:fn=>transport=fn,setValues:v=>current=v};
}
(async()=>{
  const normal=setup();await normal.context.save('guardar',true);
  assert.equal(normal.requests[0].url,'/app/conteos/44/accion/'); // form.action is shadowed by named controls in Safari too.
  assert.equal(normal.context.root.dataset.version,'3');assert.equal(normal.context.pending,null);
  assert.match(normal.stateChanges.at(-1),/Guardado en servidor/);
  const delayed=setup();delayed.setTransport(options=>{delayed.setValues({cantidad_1:'9'});return delayed.success(options);});
  await delayed.context.save('guardar',true);
  assert.equal(delayed.context.confirmed,JSON.stringify({cantidad_1:'2.5'}));
  assert.deepEqual(delayed.context.values(),{cantidad_1:'9'});
  const uncertain=setup();uncertain.setTransport(()=>{throw new Error('connection lost');});
  await uncertain.context.save('guardar',true);
  assert.equal(uncertain.context.stopped,true);assert(uncertain.context.pending);
  uncertain.setValues({cantidad_1:'7'});uncertain.form.elements.request_id.value='newer-edit';
  uncertain.setTransport(uncertain.success);await uncertain.context.save('guardar',false);
  assert.deepEqual(uncertain.requests[0].body,uncertain.requests[1].body);
  assert.deepEqual(uncertain.context.values(),{cantidad_1:'7'});
  const auth=setup();auth.setTransport(()=>({redirected:true,headers:{get:()=> 'text/html'}}));
  await auth.context.save('guardar',true);assert(auth.context.pending);assert.match(auth.notices[0],/sesión/);
  const conflict=setup();conflict.setTransport(()=>({status:409,ok:false,headers:{get:()=> 'application/json'},json:async()=>({ok:false,toast:{message:'El conteo cambió'}})}));
  await conflict.context.save('guardar',true);assert.equal(conflict.context.pending,null);assert.equal(conflict.context.root.dataset.version,'2');assert(conflict.context.stopped);
  const replay=setup();replay.setTransport(options=>replay.success(options,'4'));await replay.context.save('guardar',false);
  assert.equal(replay.context.root.dataset.version,'2');assert(replay.context.stopped);
  const locked=setup();locked.context.saving=true;await locked.context.save('enviar',false);assert.equal(locked.requests.length,0);
  console.log('Mobile save: real action URL, concurrent edits, exact replay, auth, stale versions and serialization passed.');
})().catch(error=>{console.error(error);process.exit(1);});
