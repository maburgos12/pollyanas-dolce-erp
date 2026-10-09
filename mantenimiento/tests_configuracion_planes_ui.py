"""Run actual mobile configuration JavaScript against controlled responses."""
import subprocess
from pathlib import Path
from django.conf import settings
from django.test import SimpleTestCase


class ConfiguracionPlanesJavaScriptTests(SimpleTestCase):
    def run_node(self,code):
        result=subprocess.run(['node','-e',code],text=True,capture_output=True)
        self.assertEqual(result.returncode,0,result.stderr)

    def test_retry_frozen_double_submit_new_edits_navigation_and_empty_success(self):
        source=Path(settings.BASE_DIR,'templates/mantenimiento/pwa.html').read_text()
        script=source[source.index('      function leerPlanForm'):source.index('      async function eliminarPlanMovil')]
        self.run_node('''const assert=require('node:assert/strict'); const crypto=require('node:crypto');
const values={activo:'1',nombre:'Original',tipo:'PREVENTIVO',estatus:'ACTIVO',frecuencia:'30',tolerancia:'0',ultima:'',proxima:'',responsable:'',instrucciones:''};
const fields=Object.fromEntries(Object.entries(values).map(([key,value])=>['#plan-'+key,{value}]));
const button={disabled:false,textContent:''}, feedback={textContent:''}, restart={hidden:true};
fields['#plan-config-save']=button; fields['#plan-config-feedback']=feedback; fields['#plan-config-new-attempt']=restart;fields['#plan-config-review']={hidden:true};
const form={dataset:{draftKey:'pd_plan_config_1_nuevo'},querySelector:key=>fields[key]};
let current=form; const document={getElementById:id=>current};
let storageFail=false;const stored=new Map(); const sessionStorage={setItem:(k,v)=>{if(storageFail)throw new Error('storage');stored.set(k,v);},getItem:k=>stored.get(k),removeItem:k=>{if(storageFail)throw new Error('storage');stored.delete(k);}};
const draft={actor:'1',version:2,id:null,revision_en:null,base:{},valores:{},intento:null,pending:false};
const state={perfil:{id:1},planConfigDrafts:new Map([['pd_plan_config_1_nuevo',draft]]),planes:[],resumen:{}};
let calls=[],mode='offline',resolve, started;
async function apiFetch(url,options){calls.push({url,body:JSON.parse(options.body)}); if(mode==='offline')throw new TypeError('network'); if(mode==='wait')await new Promise(r=>{resolve=r;started();}); if(mode==='poststorage')storageFail=true;return {ok:!['denied403','denied404'].includes(mode),status:mode==='denied403'?403:mode==='denied404'?404:200,json:async()=>mode==='empty'?{}:{id:mode==='zero'?0:mode==='negative'?-2:mode==='mismatch'?18:17,activo_id:mode==='wrongasset'?2:1,revision_en:'r1',ultima_ejecucion:'2026-10-01',proxima_ejecucion:'2026-10-11'}};}
''' + script + '''
(async()=>{
 fields['#plan-activo'].value='';await guardarPlanMovil();assert.equal(calls.length,0);assert.equal(draft.intento,null);assert.notEqual(fields['#plan-activo'].disabled,true);assert.match(feedback.textContent,/Selecciona un equipo/);
 fields['#plan-activo'].value='1';storageFail=true;await guardarPlanMovil();assert.equal(calls.length,0);assert.ok(draft.intento);const neverSent=draft.intento.payload.clave_captura;assert.equal(draft.pending,false);assert.match(feedback.textContent,/No se envió/);storageFail=false;
 await guardarPlanMovil();assert.equal(calls[0].body.clave_captura,neverSent); assert.equal(button.disabled,false); assert.equal(fields['#plan-nombre'].value,'Original'); const key=draft.intento.payload.clave_captura;
 fields['#plan-nombre'].value='Nuevos datos'; draft.valores=leerPlanForm(form);
 storageFail=true;await guardarPlanMovil();assert.equal(calls.length,1);assert.equal(draft.intento.payload.clave_captura,key);storageFail=false;
 mode='wait'; const waiting=new Promise(r=>started=r); const saving=guardarPlanMovil(); await waiting;
 await guardarPlanMovil(); assert.equal(calls.length,2); assert.equal(calls[1].body.nombre,'Original'); assert.equal(calls[1].body.clave_captura,key);
 // Navigation removes live form; the old form and saved newer values remain safe.
 current=null; resolve(); await saving; assert.equal(draft.valores.nombre,'Nuevos datos'); assert.equal(draft.id,17);assert.equal(draft.valores.ultima_ejecucion,'2026-10-01');assert.equal(fields['#plan-ultima'].value,'2026-10-01');assert.equal(fields['#plan-activo'].disabled,true);
 assert.equal(stored.has('pd_plan_config_1_nuevo'),false); assert.equal(JSON.parse(stored.get('pd_plan_config_1_17')).valores.nombre,'Nuevos datos');
 current=form; mode='empty'; await guardarPlanMovil(17); assert.equal(calls.at(-1).url,'/planes/17/'); assert.equal(calls.at(-1).body.nombre,'Nuevos datos'); assert.ok(draft.intento); assert.match(feedback.textContent,/No se confirmó/);
 const failed=draft.intento;
 for(const bad of ['zero','negative','mismatch','wrongasset','denied403','denied404']) {mode=bad;await guardarPlanMovil(17);assert.equal(draft.intento,failed);assert.equal(draft.intento.payload.clave_captura,failed.payload.clave_captura);assert.doesNotMatch(feedback.textContent,/Plan guardado/);nuevoIntentoPlan(17);assert.equal(draft.intento,failed);}
 mode='ok'; await guardarPlanMovil(17); assert.equal(calls.at(-1).body.clave_captura,failed.payload.clave_captura); assert.equal(draft.intento,null);mode='poststorage';await guardarPlanMovil(17);assert.equal(draft.intento,null);assert.equal(draft.id,17);assert.match(feedback.textContent,/Plan guardado/);assert.match(feedback.textContent,/ya está guardado/);
})().catch(e=>{console.error(e);process.exit(1);});''')

    def test_all_pages_or_error_preserves_previous_authorized_result(self):
        source=Path(settings.BASE_DIR,'templates/mantenimiento/pwa.html').read_text()
        script=source[source.index('      async function loadPlanPages'):source.index('      async function loadCancelaciones')]
        self.run_node('''const assert=require('node:assert/strict');const state={planes:[{id:99}],planChoices:{}};let fail=false,calls=[];
async function apiFetch(url){calls.push(url); const page=Number(new URL(url,'https://local').searchParams.get('page'));return {ok:!(fail&&page===2),json:async()=>({items:[{id:page}],pagination:{page,has_next:page===1},choices:{tipos:[]}})};}
''' + script + '''
(async()=>{await loadPlanes(true);assert.deepEqual(state.planes,[{id:1},{id:2}]);assert.equal(calls.length,2);fail=true;await assert.rejects(loadPlanes(true));assert.deepEqual(state.planes,[{id:1},{id:2}]);})().catch(e=>{console.error(e);process.exit(1);});''')

    def test_retire_lost_confirmation_reloads_same_uuid_after_row_disappears(self):
        source=Path(settings.BASE_DIR,'templates/mantenimiento/pwa.html').read_text()
        script=source[source.index('      function guardarDraftPlan'):source.index('      function nuevoIntentoPlan')]
        script+=source[source.index('      async function eliminarPlanMovil'):source.index('      async function renderCancelaciones')]
        self.run_node('''const assert=require('node:assert/strict');const crypto=require('node:crypto');const state={perfil:{id:1},pantalla:'planes',requestGeneration:{capture:0},planDeleteDrafts:new Map(),planes:[{id:9,revision_en:'r1'},{id:10,revision_en:'r1'}],resumen:null};
let storageFail=false;const stored=new Map(); const sessionStorage={setItem:(k,v)=>{if(storageFail)throw new Error('storage');stored.set(k,v);},getItem:k=>stored.get(k),removeItem:k=>stored.delete(k)}; const button={},feedback={}; const document={querySelector:()=>button,getElementById:()=>feedback};const window={confirm:()=>true};let fail=true,navigate=false,calls=[],rendered=0;async function renderPlanes(){rendered++;}
async function apiFetch(url,options){calls.push(JSON.parse(options.body));if(fail)throw new Error('offline');if(navigate){state.pantalla='historial';state.requestGeneration.capture++;}return {status:204};}
''' + script + '''
(async()=>{storageFail=true;await eliminarPlanMovil(9);assert.equal(calls.length,0);const neverSent=state.planDeleteDrafts.get('pd_plan_delete_1_9').clave_captura;assert.equal(state.planDeleteDrafts.get('pd_plan_delete_1_9').pending,false);assert.match(feedback.textContent,/No se envió/);storageFail=false;await eliminarPlanMovil(9);assert.equal(calls[0].clave_captura,neverSent);const key=calls[0].clave_captura;assert.ok(stored.get('pd_plan_delete_1_9'));state.planDeleteDrafts.clear();state.planes=[];fail=false;storageFail=true;await eliminarPlanMovil(9);assert.equal(calls.length,1);assert.equal(state.planDeleteDrafts.get('pd_plan_delete_1_9').clave_captura,key);storageFail=false;await eliminarPlanMovil(9);assert.equal(calls[1].clave_captura,key);assert.equal(stored.size,0);assert.equal(rendered,1);state.planes=[{id:10,revision_en:'r1'}];navigate=true;await eliminarPlanMovil(10);assert.equal(rendered,1);assert.equal(state.pantalla,'historial');})().catch(e=>{console.error(e);process.exit(1);});''')

    def test_real_form_reopen_pending_then_new_plan_after_success(self):
        source=Path(settings.BASE_DIR,'templates/mantenimiento/pwa.html').read_text()
        script=source[source.index('      async function renderPlanForm'):source.index('      async function eliminarPlanMovil')]
        self.run_node('''const assert=require('node:assert/strict');const crypto=require('node:crypto');
const state={perfil:{id:1},pantalla:'planes',requestGeneration:{capture:0},planes:[],planChoices:{tipos:[],estatus:[]},planConfigAssets:[],planConfigDrafts:new Map(),resumen:null};
const stored=new Map();const sessionStorage={setItem:(k,v)=>stored.set(k,v),getItem:k=>stored.get(k)||null,removeItem:k=>stored.delete(k)};
const esc=s=>String(s);const shell=s=>s;let current=null;
const map={activo:'activo_id',nombre:'nombre',tipo:'tipo',estatus:'estatus',frecuencia:'frecuencia_dias',tolerancia:'tolerancia_dias',ultima:'ultima_ejecucion',proxima:'proxima_ejecucion',responsable:'responsable',instrucciones:'instrucciones'};
async function loadPlanes(){}async function loadPlanPages(){return {items:[{id:1,nombre:'Horno',codigo:'H1'}]};}
async function render(html){
 await new Promise(resolve=>setImmediate(resolve));
 if(!html.includes('plan-config-form'))return;
 const key=html.match(/data-draft-key="([^"]+)"/)[1];const d=state.planConfigDrafts.get(key);const nodes={};
 Object.entries(map).forEach(([name,prop])=>nodes['#plan-'+name]={tagName:'INPUT',value:String(d.valores[prop]||''),listeners:{},addEventListener(n,f){this.listeners[n]=f;}});
 nodes['#plan-config-save']={disabled:html.includes('id="plan-config-save" disabled'),textContent:''};nodes['#plan-config-feedback']={textContent:''};nodes['#plan-config-new-attempt']={hidden:true};nodes['#plan-config-review']={hidden:true};
 current={dataset:{draftKey:key},querySelector:k=>nodes[k],querySelectorAll:()=>Object.entries(map).map(([name])=>nodes['#plan-'+name])};
}
const document={getElementById:id=>id==='plan-config-form'?current:null};
const window={confirm:()=>true};let calls=[],hold=true,stale=false,resolve,started;async function apiFetch(url,options){calls.push({url,body:JSON.parse(options.body)});if(hold)await new Promise(r=>{resolve=r;started();});return {ok:!stale,status:stale?409:200,json:async()=>stale?{error:'Plan actualizado',error_code:'plan_revision_conflict'}:{id:calls.length===1?17:18,activo_id:1,revision_en:'r1',ultima_ejecucion:'2026-10-01',proxima_ejecucion:'2026-10-11'}};}
''' + script + '''
(async()=>{
 await renderPlanForm(); current.querySelector('#plan-activo').value='1';current.querySelector('#plan-nombre').value='Original';current.querySelector('#plan-nombre').listeners.input();
 const waiting=new Promise(r=>started=r);const saving=guardarPlanMovil();await waiting;
 current=null;await renderPlanForm();assert.equal(current.querySelector('#plan-config-save').disabled,true);
 current.querySelector('#plan-nombre').value='Edición pendiente';current.querySelector('#plan-nombre').listeners.input();
 resolve();await saving;assert.equal(current.querySelector('#plan-config-save').disabled,false);assert.match(current.querySelector('#plan-config-feedback').textContent,/guardado/);assert.equal(current.querySelector('#plan-nombre').value,'Edición pendiente');
 await renderPlanForm();assert.equal(state.planConfigDrafts.get('pd_plan_config_1_nuevo').id,null);assert.equal(current.querySelector('#plan-nombre').value,'');
 hold=false;current.querySelector('#plan-activo').value='1';current.querySelector('#plan-nombre').value='Segundo';current.querySelector('#plan-nombre').listeners.input();await guardarPlanMovil();
 assert.equal(calls[0].url,'/planes/');assert.equal(calls[1].url,'/planes/');assert.notEqual(calls[0].body.clave_captura,calls[1].body.clave_captura);
 state.planes=[{...calls[0].body,id:17,activo_id:1,nombre:'Nombre posterior',revision_en:'r2',ultima_ejecucion:'2026-10-02',proxima_ejecucion:'2026-10-12'}];
 await renderPlanForm(17); current.querySelector('#plan-nombre').value='Mi cambio actual';current.querySelector('#plan-nombre').listeners.input();stale=true;await guardarPlanMovil(17);
 const staleDraft=state.planConfigDrafts.get('pd_plan_config_1_17');assert.ok(staleDraft.intento);assert.equal(staleDraft.stale,true);await revisarPlanActual(17);
 assert.equal(staleDraft.intento,null);assert.equal(staleDraft.revision_en,'r2');assert.equal(current.querySelector('#plan-nombre').value,'Mi cambio actual');assert.equal(current.querySelector('#plan-ultima').value,'2026-10-02');
 const baselineCalls=calls.length;
 for (const attempt of [0,false,'',undefined,{id:null,payload:calls[0].body},{payload:calls[0].body},{id:17,payload:{...calls[0].body,nombre:{nested:'invalid'}}}]) {
   state.planConfigDrafts.clear();stored.set('pd_plan_config_1_17',JSON.stringify({actor:'1',version:2,id:17,revision_en:'r1',valores:{nombre:'Original'},intento:attempt}));current=null;
   await renderPlanForm(17);await new Promise(resolve=>setImmediate(resolve));assert.equal(current,null);assert.equal(calls.length,baselineCalls);
 }
 state.planConfigDrafts.clear();stored.set('pd_plan_config_1_nuevo','{malformed');current=null;await renderPlanForm();await new Promise(resolve=>setImmediate(resolve));assert.equal(current,null);assert.equal(calls.length,baselineCalls);
})().catch(e=>{console.error(e);process.exit(1);});''')

    def test_stale_retire_review_guards_double_click_cancel_and_fetch_error(self):
        source=Path(settings.BASE_DIR,'templates/mantenimiento/pwa.html').read_text()
        script=source[source.index('      function guardarDraftPlan'):source.index('      async function revisarPlanActual')]
        script+=source[source.index('      async function eliminarPlanMovil'):source.index('      async function renderCancelaciones')]
        self.run_node('''const assert=require('node:assert/strict');const crypto=require('node:crypto');
const state={perfil:{id:1},pantalla:'planes',requestGeneration:{capture:0},planes:[{id:9,nombre:'Plan',revision_en:'r2'}],planDeleteDrafts:new Map(),resumen:null};
let storageFail=false;const stored=new Map();const sessionStorage={setItem:(k,v)=>{if(storageFail)throw new Error('storage');stored.set(k,v);},getItem:k=>stored.get(k)||null,removeItem:k=>stored.delete(k)};
const button={},feedback={};const document={querySelector:()=>button,getElementById:()=>feedback};let confirms=0,allow=true;const window={confirm:()=>{confirms++;return allow;}};
let getCalls=0,deleteCalls=[],resolve,started,fail=false,hold=false,flipStorage=false;
async function loadPlanes(){getCalls++;if(fail)throw new Error('consulta falló');if(hold)await new Promise(r=>{resolve=r;started();});if(flipStorage)storageFail=true;}
async function apiFetch(url,options){deleteCalls.push(JSON.parse(options.body));return {status:204};}async function renderPlanes(){}
function stale(){state.planes=[{id:9,nombre:'Plan',revision_en:'r2'}];const d={actor:'1',id:9,revision_en:'r1',clave_captura:crypto.randomUUID(),stale:true,pending:false};state.planDeleteDrafts.set('pd_plan_delete_1_9',d);return d;}
''' + script + '''
(async()=>{
 const d=stale();hold=true;const waiting=new Promise(r=>started=r);const saving=eliminarPlanMovil(9);await waiting;
 await eliminarPlanMovil(9);assert.equal(getCalls,1);assert.equal(confirms,0);assert.equal(button.disabled,true);
 resolve();await saving;assert.equal(confirms,1);assert.equal(deleteCalls.length,1);assert.equal(deleteCalls[0].revision_en,'r2');assert.equal(d.pending,false);assert.equal(stored.size,0);
 hold=false;allow=false;const cancel=stale();const original=cancel.clave_captura;await eliminarPlanMovil(9);assert.equal(cancel.pending,false);assert.equal(cancel.clave_captura,original);assert.equal(deleteCalls.length,1);assert.equal(confirms,2);assert.equal(button.disabled,false);
 fail=true;allow=true;await eliminarPlanMovil(9);assert.equal(cancel.pending,false);assert.equal(cancel.clave_captura,original);assert.equal(deleteCalls.length,1);assert.match(feedback.textContent,/consulta falló/);assert.equal(button.disabled,false);
 fail=false;flipStorage=true;await eliminarPlanMovil(9);assert.equal(cancel.pending,false);assert.equal(deleteCalls.length,1);assert.notEqual(cancel.clave_captura,original);const reviewedKey=cancel.clave_captura;assert.match(feedback.textContent,/No se envió/);
 storageFail=false;flipStorage=false;await eliminarPlanMovil(9);assert.equal(deleteCalls.length,2);assert.equal(deleteCalls[1].clave_captura,reviewedKey);
})().catch(e=>{console.error(e);process.exit(1);});''')
