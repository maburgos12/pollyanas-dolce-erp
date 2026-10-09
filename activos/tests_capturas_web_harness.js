// Execute the real capture adapter AND the shared action engine across fresh DOMs.
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const crypto = require('node:crypto');
const source = fs.readFileSync(process.env.CAPTURE_SCRIPT || 'static/activos/capturas.js', 'utf8');
const engine = fs.readFileSync('static/js/erp_actions.js', 'utf8');
const tick = () => new Promise(resolve => setTimeout(resolve, 2));
function storage() {
  const map = new Map();
  return {map, getItem: key => map.get(key) || null, setItem: (key, value) => map.set(key, value), removeItem: key => map.delete(key)};
}
function file(bytes) {
  const blob = new Blob([bytes], {type: 'application/pdf'});
  return Object.assign(blob, {name: 'soporte.pdf', lastModified: 1});
}
function fixture(store = storage(), options = {}) {
  let controls = [
    {name:'csrfmiddlewaretoken', type:'hidden', value: options.csrf || 'csrf-current'},
    {name:'clave_captura', type:'hidden', value:'11111111-1111-4111-8111-111111111111'},
    {name:'action', type:'hidden', value:'create_orden'},
    {name:'activo_id', type:'select-one', value:'42'},
    {name:'solicitud_id', type:'hidden', value:'3'},
    {name:'solicitud_id', type:'hidden', value:'5'},
    {name:'descripcion', type:'textarea', value:'Trabajo'},
    {name:'cerrar', type:'checkbox', value:'1', checked:false},
    {name:'checks', type:'checkbox', value:'a', checked:true},
    {name:'checks', type:'checkbox', value:'b', checked:false},
    {name:'tags', type:'select-multiple', multiple:true, value:'a', options:[{value:'a',selected:true},{value:'b',selected:true},{value:'c',selected:false}]},
    {name:'evidencias', type:'file', files:options.files || []}
  ];
  if (options.surface && options.surface !== "ordenes") controls = controls.filter(control => control.name !== "action");
  if (options.noSolicitudes) controls = controls.filter(control => control.name !== "solicitud_id");
  controls.forEach(control => {control.disabled=false; control.dataset={};});
  const submit={type:'submit',textContent:'Guardar',dataset:{},disabled:false,focus(){}};
  const listeners = {};
  const node = () => ({children:[],dataset:{},_text:'',set textContent(value){this._text=value;this.children=[];},get textContent(){return this._text;},appendChild(child){this.children.push(child);},remove(){},focus(){}});
  const feedback=node(), details={open:false}, requests=[];
  let submissionActive = false;
  const form = {
    dataset:{captureSnapshot:'true',actionInlineFeedback:'true'}, method:'post',
    elements:{clave_captura:controls.find(c=>c.name==='clave_captura')},
    querySelector(selector) {return selector.includes('submit') ? submit : feedback;},
    querySelectorAll(selector) {return selector.includes('data-solicitud-borrador') ? [] : controls;},
    closest:()=>details, getAttribute:()=>null, reportValidity:()=>true,
    reset(){controls[1].value='11111111-1111-4111-8111-111111111111';controls[6].value='';controls[11].files=[];},
    addEventListener(name,callback,capture=false){(listeners[name] ||= []).push({callback,capture});},
    dispatchEvent(event){event.currentTarget=this;for(const {callback} of listeners[event.type] || []) callback(event);},
    requestSubmit(button=submit){
      if (submissionActive) return;
      submissionActive = true;setTimeout(()=>{submissionActive=false;},0);
      let stopped=false;
      const event={type:'submit',currentTarget:form,submitter:button,preventDefault(){},stopImmediatePropagation(){stopped=true;}};
      for(const capture of [true,false]) for(const entry of listeners.submit || []) if(entry.capture === capture && !stopped) entry.callback(event);
    }
  };
  class BrowserFormData extends FormData {
    constructor(input) {
      super();if(!input) return;
      controls.filter(c=>!c.disabled).forEach(c=>{
        if(c.type==='file') c.files.forEach(f=>this.append(c.name,f,f.name));
        else if(c.multiple) c.options.filter(o=>o.selected).forEach(o=>this.append(c.name,o.value));
        else if(!['checkbox','radio'].includes(c.type) || c.checked) this.append(c.name,c.value);
      });
    }
  }
  const context = {
    crypto: options.crypto || crypto.webcrypto, FormData: BrowserFormData, Uint8Array, URL,
    sessionStorage:store, CustomEvent:class {constructor(type,options={}){this.type=type;this.detail=options.detail;}},
    document:{
      querySelector:()=>form,
      querySelectorAll:selector=>selector.includes('form[') ? [form] : [],
      getElementById(id){if(id==='captura-contexto') return {textContent:JSON.stringify({actor:options.actor || '7',superficie:options.surface || 'ordenes'})};if(id==='captura-borrador') return {textContent:JSON.stringify(options.draft || null)};return null;},
      createElement:()=>node(), createTextNode:text=>({textContent:text}), contains:()=>true
    },
    window:{sessionStorage:store,location:{href:'https://erp.example/activos/'+(options.surface || 'ordenes')+'/',origin:'https://erp.example'},setTimeout,clearTimeout},
    fetch:async (url,request)=>{
      requests.push(request);
      assert.ok(store.map?.size || options.storageFails,'El intento debe conservarse antes de fetch');
      return options.response ? options.response(requests.length) : {ok:false,status:500,headers:{get:()=> 'application/json'},json:async()=>({ok:false})};
    }
  };
  vm.runInNewContext(engine,context);
  vm.runInNewContext(source,context);
  return {form,controls,feedback,requests,details,submit,store,fire:(name,detail={})=>form.dispatchEvent({type:name,detail}),
    async save(){form.requestSubmit();for(let i=0;i<10;i++) await tick();},
    record(){return JSON.parse(store.getItem('activos:captura:v1:'+(options.actor || '7')+':'+(options.surface || 'ordenes')));}};
}
async function check() {
  // Previous regression contract: exact repeated fields, error states, original file.
  for (const code of [400,403,404,409,410,0,500]) {
    const f=fixture(storage(),{files:[file('A')],response:async()=>{
      if(!code) throw Error('offline');
      return {ok:false,status:code,headers:{get:()=> 'application/json'},json:async()=>({ok:false,toast:{message:'Respuesta '+code}})};
    }});
    await f.save();
    assert.equal(f.requests.length,1);
    assert.deepEqual(f.requests[0].body.getAll('solicitud_id'),['3','5']);
    assert.equal(f.controls[1].value,'11111111-1111-4111-8111-111111111111');
    assert.equal(await f.controls[11].files[0].text(),'A');
    if([400,403,404].includes(code)) {assert.ok(f.controls.every(c=>!c.disabled));assert.equal(f.form._captureSnapshot,undefined);}
    else {assert.ok(f.controls.every(c=>c.disabled));assert.ok(f.form._captureSnapshot);}
  }
  const store=storage(), original=fixture(store,{files:[file('A')]});
  await original.save();
  const saved=original.record();
  assert.equal(saved.status,'incierto');
  assert.equal(saved.files[0].sha256,crypto.createHash('sha256').update('A').digest('hex'));
  assert.ok(!JSON.stringify(saved).includes('csrf-current'));
  assert.ok(!JSON.stringify(saved).includes('arrayBuffer'));
  const reload=fixture(store,{csrf:'csrf-fresh'});
  assert.equal(reload.controls[1].value,'11111111-1111-4111-8111-111111111111');
  assert.deepEqual(reload.controls.filter(c=>c.name==='solicitud_id').map(c=>c.value),['3','5']);
  assert.equal(reload.controls[7].checked,false);assert.equal(reload.controls[8].checked,true);assert.equal(reload.controls[9].checked,false);
  assert.deepEqual(reload.controls[10].options.map(o=>o.selected),[true,true,false]);
  assert.equal(reload.controls[11].disabled,false);
  await reload.save();assert.equal(reload.requests.length,0,'Falta adjunto: nunca crear un File ficticio');
  reload.controls[11].files=[file('B')];await reload.save();assert.equal(reload.requests.length,0,'Mismo nombre/tamaño, bytes distintos: bloquear');
  reload.controls[11].files=[file('A')];await reload.save();assert.equal(reload.requests.length,1);
  assert.equal(reload.requests[0].body.get('csrfmiddlewaretoken'),'csrf-fresh');
  assert.equal(reload.requests[0].body.get('clave_captura'),'11111111-1111-4111-8111-111111111111');
  assert.deepEqual(reload.requests[0].body.getAll('tags'),['a','b']);
  assert.deepEqual(reload.requests[0].body.getAll('checks'),['a']);
  assert.equal(reload.requests[0].body.has('cerrar'),false);
  const changedDOM=fixture(store,{noSolicitudes:true,files:[file('A')]});
  assert.equal(changedDOM.controls.find(c=>c.name==='descripcion').value,'Trabajo');
  await changedDOM.save();assert.equal(changedDOM.requests.length,1);
  assert.deepEqual(changedDOM.requests[0].body.getAll('solicitud_id'),['3','5'],'Las fallas ya resueltas no cambian el intento congelado');
  assert.ok(changedDOM.feedback.textContent.includes('Solicitudes del intento: 3, 5'));
  // Cross user/surface namespaces never restore another attempt.
  for(const options of [{actor:'8'},{surface:'reportes'}]) {
    const other=fixture(store,options);assert.equal(other.form._captureSnapshot,undefined);assert.equal(other.controls[6].disabled,false);
  }
  const success=fixture(store,{files:[file('A')],response:()=>({ok:true,status:200,headers:{get:()=> 'application/json'},json:async()=>({ok:true,enlace:'/activos/ordenes/?orden=1',siguiente_clave:'22222222-2222-4222-8222-222222222222',toast:{message:'Registrado'}})})});
  await success.save();assert.equal(success.record().status,'confirmado');
  const receipt=fixture(store);await receipt.save();assert.equal(receipt.requests.length,0);
  assert.ok(receipt.feedback.children.some(c=>c.href==='https://erp.example/activos/ordenes/?orden=1'));
  receipt.feedback.children.find(c=>c.dataset?.nuevaCaptura).onclick();
  assert.equal(receipt.controls[1].value,'22222222-2222-4222-8222-222222222222');assert.equal(receipt.record().status,'editable');
  // Editable drafts retain unchecked values and repeated controls as well.
  const draftStore=storage(), editable=fixture(draftStore);
  editable.controls[6].value='Borrador';editable.fire('input');
  const edited=fixture(draftStore);assert.equal(edited.controls[6].value,'Borrador');assert.ok(edited.controls.every(c=>!c.disabled));
  const corrupt=storage();corrupt.setItem('activos:captura:v1:7:ordenes','{broken');
  const broken=fixture(corrupt);await broken.save();assert.equal(broken.requests.length,0);assert.ok(broken.feedback.textContent.includes('No pudimos leer'));
  assert.equal(broken.details.open,true,'La advertencia corrupta no puede quedar oculta en details');
  const missingKey=storage();const invalid={...saved,entries:saved.entries.filter(entry=>entry[0]!=='clave_captura')};
  missingKey.setItem('activos:captura:v1:7:ordenes',JSON.stringify(invalid));
  const invalidAttempt=fixture(missingKey);await invalidAttempt.save();assert.equal(invalidAttempt.requests.length,0,'Nunca usar el UUID nuevo del DOM si el registro no tiene clave');
  const unavailable={getItem(){throw Error('denied');},setItem(){throw Error('quota');},removeItem(){}};
  const noStorage=fixture(unavailable,{storageFails:true});await noStorage.save();assert.equal(noStorage.requests.length,0,'GET desconocido no autoriza iniciar otro intento');
  noStorage.feedback.children.find(c=>c.dataset?.nuevaCaptura).onclick();await noStorage.save();assert.equal(noStorage.requests.length,1,'Nueva captura explícita puede continuar con advertencia');
  const quotaOnly={getItem(){return null;},setItem(){throw Error('quota');},removeItem(){}};
  const firstUse=fixture(quotaOnly,{storageFails:true});await firstUse.save();assert.equal(firstUse.requests.length,1);assert.ok(firstUse.feedback.textContent.includes('no permite conservar'));
  for(const mutate of [
    r=>{r.controls=false;},r=>{r.controls=null;},r=>{r.entries=false;},r=>{r.files=null;},r=>{r.result=false;},r=>{delete r.status;},
    r=>{r.entries=r.entries.filter(entry=>entry[0]!=='action');},
    r=>{r.entries.find(entry=>entry[0]==='action')[1]='update_costos';},
    r=>{r.controls.find(control=>control?.name==='action').value='update_costos';},
    r=>{r.entries.push(['unexpected_field','sorpresa']);},
    r=>{r.endpoint='/activos/reportes/';},
  ]) {
    const tampered=storage(), data=JSON.parse(JSON.stringify(saved));mutate(data);
    tampered.setItem('activos:captura:v1:7:ordenes',JSON.stringify(data));
    const negative=fixture(tampered);await negative.save();assert.equal(negative.requests.length,0,'Registro inválido no debe enviarse');
    assert.ok(negative.feedback.textContent.includes('No pudimos leer'));
  }
  for(const surface of ['reportes','registro-rapido']) {
    const scoped=storage(), valid=fixture(scoped,{surface});await valid.save();
    const fresh=fixture(scoped,{surface});await fresh.save();assert.equal(fresh.requests.length,1,'El schema sin action sí recupera su endpoint');
    const data=valid.record();data.entries.push(['action','create_orden']);scoped.setItem('activos:captura:v1:7:'+surface,JSON.stringify(data));
    const extraAction=fixture(scoped,{surface});await extraAction.save();assert.equal(extraAction.requests.length,0,'Otra operación no se acepta en reportes/registro');
  }
  // Repeated clicks while hashing cannot start another fetch or alter the snapshot.
  let finish;
  const slow=fixture(storage(),{files:[file('A')],crypto:{randomUUID:crypto.randomUUID,subtle:{digest:()=>new Promise(resolve=>{finish=()=>resolve(new Uint8Array(32).buffer);})}}});
  slow.form.requestSubmit();await tick();slow.form.requestSubmit();assert.equal(slow.requests.length,0);assert.ok(slow.controls.every(c=>c.disabled));
  finish();for(let i=0;i<10;i++) await tick();assert.equal(slow.requests.length,1);
  console.log('PASS: real shared motor + fresh DOM recovery, repeated fields, exact checks/multiple, current CSRF, SHA/missing/different files, receipts, explicit new, storage errors, namespaces, double click, rejection/uncertain lifecycle.');
}
check().catch(error=>{console.error(error);process.exit(1);});
