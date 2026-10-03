"""Execute the real inline JavaScript with a minimal DOM/network harness."""
import subprocess
from pathlib import Path
from django.conf import settings
from django.test import SimpleTestCase


class PlanExecutionJavaScriptTests(SimpleTestCase):
    def run_node(self, code):
        result = subprocess.run(['node', '-e', code], text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_initial_hash_opens_authorized_order_tab(self):
        source = Path(settings.BASE_DIR, 'templates/mantenimiento/dashboard.html').read_text()
        script = source.split('// ── Tab switching ──')[1].split('\n(() => {\n  const drawer')[0]
        self.run_node('''const assert = require('node:assert/strict');
const tabs = ['tab-equipos','tab-dashboard','tab-seguimiento'].map(id => ({dataset:{tab:id},active:false,classList:{toggle(name,v){ tabs.find(x => x.dataset.tab===id).active=v; }},addEventListener(){}}));
const panels = tabs.map(t => ({id:t.dataset.tab,classList:{toggle(){}}}));
const document = {querySelectorAll: s => s==='[data-tab]' ? tabs : panels,getElementById: id => panels.find(p=>p.id===id)};
const location = {hash:'#tab-seguimiento',search:'?open=orden:1'};
const sessionStorage = {setItem(){}, getItem(){return 'tab-dashboard';}};
const window = {addEventListener(){}};
''' + script + '''\nassert.equal(tabs.find(t=>t.dataset.tab==='tab-seguimiento').active,true);''')

    def test_fresh_server_date_required_and_retry_keeps_original_across_midnight(self):
        source = Path(settings.BASE_DIR, 'templates/mantenimiento/pwa.html').read_text()
        script = source[source.index('      async function ejecutarPlanAgenda'):source.index('      function planCard')]
        script += source[source.index('      async function returnFromDetail'):source.index('      function invalidateDetail')]
        script += source[source.index('      async function loadBandeja'):source.index('      async function loadResumen')]
        self.run_node('''const assert = require('node:assert/strict'); const crypto = require('node:crypto');
const state = {planExecutionDrafts:new Map(),bandeja:[],ordenes:[],counts:{cerrados:3},perfil:{id:1},requestGeneration:{detail:0,inbox:0}}; const esc = s=>String(s); const formatFechaCorta=s=>s;
const fields = {disabled:false}; const feedback = {}; const button = {disabled:false};
const form = {elements:{fecha_ejecucion:{value:''},notas:{value:''},intervencion_adicional:{checked:false},motivo_adicional:{value:''}},
 children:[], addEventListener(){}, reportValidity(){return true;}, appendChild(node){this.children.push(node);}, querySelector(s){return s==='fieldset'?fields:s==='[type="submit"]'?button:s==='.link-feedback'?feedback:{innerHTML:''};}};
const region = {innerHTML:'',querySelector(){return form;},closest(){return null;}};
const agendaCounters=[{dataset:{agendaCount:'vencidos'},textContent:'2'},{dataset:{agendaCount:'urgentes'},textContent:'1'},{dataset:{agendaCount:'programados'},textContent:'0'},{dataset:{agendaCount:'vencidos'},textContent:'2'}];
const document = {getElementById(){return region;},createElement(){return {};},querySelectorAll:s=>s==='[data-agenda-count]'?agendaCounters:[]};
const stored = new Map(); const sessionStorage = {getItem:k=>stored.get(k)||null,setItem:(k,v)=>stored.set(k,v),removeItem:k=>stored.delete(k)};
const app={querySelectorAll:s=>s==='[data-plan-execution-pwa]'?[{dataset:{planExecutionPwa:'1'}}]:[form.elements.fecha_ejecucion]}; async function render(){form.onsubmit=null;}
const window = {scrollY:17,scrollTo(x,y){assert.equal(y,17);}};
let inboxOK=true, bodyGate=null, bodyStarted=null; async function apiV2Fetch(){return {ok:inboxOK,json:async()=>{if(bodyGate){bodyStarted();await bodyGate;}return {results:[],counts:{abiertos:0,en_proceso:0,criticos:0,cerrados:4}};}};} let fresh=null; let calls=[]; async function loadResumen(force){assert.equal(force,true); state.resumen=fresh;return fresh;}
let fail=true; async function apiFetch(url, options){ calls.push(JSON.parse(options.body)); if(fail) throw new Error('offline'); return {ok:true,json:async()=>({ok:true,proxima_ejecucion:'2026-10-12',orden:{id:1,folio:'OT-1'}})}; }
''' + script + '''
(async()=>{
 await ejecutarPlanAgenda(1); assert.equal(state.planExecutionDrafts.size,0); assert.match(region.innerHTML,/Recupera la conexión/);
 fresh={fecha:'2026-10-02'}; await ejecutarPlanAgenda(1); const draft=state.planExecutionDrafts.get('pd_plan_execution_1_1'); assert.equal(draft.fecha,'2026-10-02');
 draft.notas='Trabajo original'; draft.adicional=true; draft.motivo='Independiente'; await form.onsubmit({preventDefault(){}}); const key=draft.clave; state.detailReturn={html:'restored',values:['2026-10-03'],scroll:17}; await returnFromDetail(); assert.equal(typeof form.onsubmit,'function'); assert.match(region.innerHTML,/value="2026-10-02"/); assert.equal(draft.intento.intervencion_adicional,true);
 fresh={fecha:'2026-10-03'}; state.perfil.id=2; await ejecutarPlanAgenda(1); assert.notEqual(state.planExecutionDrafts.get('pd_plan_execution_2_1').clave,key); state.perfil.id=1; state.planExecutionDrafts.clear(); await ejecutarPlanAgenda(1); assert.equal(state.planExecutionDrafts.get('pd_plan_execution_1_1').fecha,'2026-10-02');
 fresh.agenda_counts={vencidos:0,urgentes:1,programados:2}; fail=false; await form.onsubmit({preventDefault(){}});
 assert.equal(agendaCounters[0].textContent,0); assert.equal(agendaCounters[2].textContent,2); assert.equal(agendaCounters[3].textContent,0);
 assert.equal(state.counts.cerrados,4); assert.deepEqual(state.bandeja,[]); assert.doesNotMatch(feedback.textContent,/recupera la conexión/);
 assert.deepEqual(calls[0],calls[1]); assert.equal(calls[1].fecha_ejecucion,'2026-10-02'); assert.equal(calls[1].notas,'Trabajo original'); assert.equal(state.planExecutionDrafts.get('pd_plan_execution_1_1').guardado,true);
 await ejecutarPlanAgenda(1,true); assert.equal(state.planExecutionDrafts.get('pd_plan_execution_1_1').fecha,'2026-10-03'); assert.notEqual(state.planExecutionDrafts.get('pd_plan_execution_1_1').clave,key);
 agendaCounters[0].textContent=7; agendaCounters[2].textContent=4; fresh=null; await form.onsubmit({preventDefault(){}}); assert.equal(agendaCounters[0].textContent,7); assert.equal(agendaCounters[2].textContent,4); assert.equal(state.planExecutionDrafts.get('pd_plan_execution_1_1').guardado,true); assert.match(feedback.textContent,/ejecución está guardada.*contadores/);
 fresh={fecha:'2026-10-04',agenda_counts:{vencidos:0,urgentes:0,programados:5}}; await ejecutarPlanAgenda(1,true); inboxOK=false; state.counts={cerrados:9}; const oldCounts=state.counts; agendaCounters[0].textContent=7; await form.onsubmit({preventDefault(){}});
 assert.equal(state.counts,oldCounts); assert.equal(agendaCounters[0].textContent,7); assert.equal(state.planExecutionDrafts.get('pd_plan_execution_1_1').guardado,true); assert.equal(button.disabled,true); assert.match(feedback.textContent,/ejecución está guardada.*contadores/); assert.match(form.children.at(-2).textContent,/Abrir orden/); assert.equal(typeof form.children.at(-2).onclick,'function');
 await ejecutarPlanAgenda(1,true); inboxOK=true; let releaseBody; bodyGate=new Promise(resolve=>releaseBody=resolve); const started=new Promise(resolve=>bodyStarted=resolve); const saving=form.onsubmit({preventDefault(){}}); await started; state.requestGeneration.inbox++; releaseBody(); await saving; assert.equal(state.counts,oldCounts); assert.equal(agendaCounters[0].textContent,7); assert.equal(state.planExecutionDrafts.get('pd_plan_execution_1_1').guardado,true); assert.match(feedback.textContent,/ejecución está guardada.*contadores/); assert.equal(button.disabled,true); assert.match(form.children.at(-2).textContent,/Abrir orden/);
})().catch(e=>{console.error(e);process.exit(1);});''')

    def test_web_reload_restores_business_payload_with_current_csrf_and_actor_isolation(self):
        source = Path(settings.BASE_DIR, 'templates/mantenimiento/dashboard.html').read_text()
        start = source.index("(function () {\n  document.querySelectorAll('[data-plan-capture]')")
        script = source[start:source.index('</script>', start)]
        self.run_node('''const vm=require('node:vm'); const assert=require('node:assert/strict');
const stored=new Map(); const sessionStorage={getItem:k=>stored.get(k)||null,setItem:(k,v)=>stored.set(k,v),removeItem:k=>stored.delete(k)};
function fixture(actor,key,date,csrf){
 const handlers={}; const fields={inert:false}; const error={hidden:true}; const submit={textContent:'Guardar'};
 const inputs={clave_captura:{type:'hidden',value:key},fecha_ejecucion:{type:'date',value:date},notas:{type:'textarea',value:'Original'},intervencion_adicional:{type:'checkbox',checked:false},motivo_adicional:{type:'textarea',value:''},csrfmiddlewaretoken:{type:'hidden',value:csrf}};
 const form={dataset:{planCapture:'1'},querySelector(s){if(s==='fieldset')return fields;if(s==='.plan-capture-error')return error;if(s==='[type="submit"]')return submit;return inputs[s.match(/name="([^"]+)"/)[1]];},addEventListener(n,f){handlers[n]=f;}};
 Object.values(inputs).forEach(input=>input.addEventListener=()=>{});
 class FormData {constructor(f){this.values={};Object.entries(inputs).forEach(([k,v])=>{if(v.type!=='checkbox'||v.checked)this.values[k]=v.type==='checkbox'?'on':v.value;});}get(k){return this.values[k]??null;}}
 const document={querySelectorAll(){return [form];},getElementById(){return form;}};
 return {context:{document,window:{},sessionStorage,FormData,crypto:require('node:crypto')},handlers,form,fields,inputs,FormData};
}
''' + 'const script=' + __import__('json').dumps(script) + ''';
let first=fixture(41,'uuid-original','2026-10-02','csrf-old'); vm.runInNewContext(script.replaceAll('{{ request.user.pk }}','41'),first.context);
first.handlers['erp:action-start']({detail:{formData:new first.FormData(first.form)}});
assert.equal(JSON.stringify([...stored.values()]).includes('csrf-old'),false);
let reloaded=fixture(41,'uuid-new','2026-10-03','csrf-new');vm.runInNewContext(script.replaceAll('{{ request.user.pk }}','41'),reloaded.context);
assert.equal(reloaded.form._captureSnapshot.get('clave_captura'),'uuid-original');assert.equal(reloaded.form._captureSnapshot.get('fecha_ejecucion'),'2026-10-02');assert.equal(reloaded.form._captureSnapshot.get('csrfmiddlewaretoken'),'csrf-new');assert.equal(reloaded.fields.inert,true);
let other=fixture(42,'other-key','2026-10-03','csrf-other');vm.runInNewContext(script.replaceAll('{{ request.user.pk }}','42'),other.context);assert.equal(other.form._captureSnapshot,undefined);
reloaded.handlers['erp:action-error']({detail:{statusCode:409,message:'Esta captura ya se envió con otros datos.'}});assert.equal(stored.size,1);assert.equal(reloaded.fields.inert,true);
reloaded.handlers['erp:action-error']({detail:{statusCode:400,message:'Fecha inválida'}});assert.equal(stored.size,1);assert.equal(reloaded.fields.inert,false);assert.equal(JSON.parse([...stored.values()][0]).clave_captura,'uuid-original');
''')

    def test_plan_card_identity_does_not_leak_to_other_dashboard_sections(self):
        source = Path(settings.BASE_DIR, 'templates/mantenimiento/dashboard.html').read_text()
        marker = 'id="planCard{{ plan.pk }}"'
        self.assertEqual(source.count(marker), 1)
        plans = source.split('{% for plan in planes_proximos %}', 1)[1].split('{% endfor %}', 1)[0]
        self.assertIn(marker, plans)

    def test_web_active_order_race_refreshes_only_authorized_links_outside_fieldset(self):
        source = Path(settings.BASE_DIR, 'templates/mantenimiento/dashboard.html').read_text()
        plan_form = source.split('id="planForm{{ plan.pk }}"',1)[1].split('</form>',1)[0]
        self.assertIn('class="plan-open-orders"', plan_form.split('</fieldset>',1)[1])
        start=source.index("(function () {\n  document.querySelectorAll('[data-plan-capture]')")
        script=source[start:source.index('</script>',start)].replace("{% url 'mantenimiento:dashboard' %}",'/mantenimiento/')
        self.run_node('''const vm=require('node:vm'),assert=require('node:assert/strict');
const handlers={}, fields={inert:false}, error={}, links={children:[],replaceChildren(...nodes){this.children=nodes;}};
const checkbox={addEventListener(){}};const form={dataset:{planCapture:'1'},_captureSnapshot:{original:true},querySelector(s){return s==='fieldset'?fields:s==='.plan-capture-error'?error:s==='.plan-open-orders'?links:checkbox;},addEventListener(n,f){handlers[n]=f;}};
const document={querySelectorAll(){return [form];},createElement(){return {};}};
const window={location:{origin:'https://erp.test'}};let calls=[], fail=false;
class DOMParser{parseFromString(){return {querySelector(s){assert.equal(s,'#planForm1 .plan-open-orders');return {querySelectorAll(){return [{textContent:'Abrir orden nueva',getAttribute:()=>'/mantenimiento/?open=orden:99#tab-seguimiento'},{textContent:'No copiar',getAttribute:()=> 'https://external.test/mantenimiento/?open=orden:99#tab-seguimiento'}];}};}};}}
async function fetch(url,options){calls.push([url,options]);if(fail)throw new Error('offline');return {ok:true,text:async()=>'<dashboard actualizado>'};}
vm.runInNewContext(''' + __import__('json').dumps(script) + ''',{document,window,sessionStorage:{getItem(){return null;}},DOMParser,URL,fetch});
(async()=>{
await handlers['erp:action-error']({detail:{statusCode:409,message:'El plan ya tiene órdenes abiertas.'}});
assert.equal(calls[0][0],'/mantenimiento/');assert.equal(calls[0][1].cache,'no-store');assert.equal(calls[0][1].credentials,'same-origin');
assert.equal(links.children.length,1);assert.equal(links.children[0].href,'https://erp.test/mantenimiento/?open=orden:99#tab-seguimiento');assert.equal(fields.inert,false);
const old=links.children[0]; fail=true;await handlers['erp:action-error']({detail:{statusCode:409,message:'El plan ya tiene órdenes abiertas.'}});assert.equal(links.children[0],old);assert.match(error.textContent,/Seguimiento/);
})().catch(e=>{console.error(e);process.exit(1);});''')

    def test_agenda_moves_the_same_confirmed_card_out_of_old_group_with_form_intact(self):
        source=Path(settings.BASE_DIR,'templates/mantenimiento/pwa.html').read_text()
        script=source[source.index('      async function ejecutarPlanAgenda'):source.index('      function planCard')]
        self.run_node('''const assert=require('node:assert/strict'),crypto=require('node:crypto');
const state={perfil:{id:1},planExecutionDrafts:new Map(),counts:{cerrados:1}};const esc=s=>String(s),formatFechaCorta=s=>s,agendaLabel=row=>row.proxima_ejecucion;
const fields={disabled:false},feedback={},button={}; const form={elements:{},addEventListener(){},reportValidity(){return true;},appendChild(){},querySelector:s=>s==='fieldset'?fields:s==='[type="submit"]'?button:feedback};
const chip={};const card={dataset:{},querySelector(){return chip;},classList:{remove(){},add(){}}};
const oldGroup={children:[card],querySelector(){return this.children[0]||null;}};card.parentNode=oldGroup;card.closest=()=>oldGroup;
const saved={hidden:true,children:[],appendChild(node){node.parentNode.children.splice(node.parentNode.children.indexOf(node),1);this.children.push(node);node.parentNode=this;}};
const region={innerHTML:'',querySelector(){return form;},closest(){return card;}};
const count={dataset:{agendaCount:'urgentes'},textContent:1};const label={textContent:'Próximo trabajo'};let showResults=true;
const document={getElementById:id=>id==='plan-execution-results'?(showResults?saved:null):id==='plan-next-work-label'?label:region,createElement(){return {};},querySelectorAll:s=>s==='[data-agenda-count]'?[count]:[]};
const window={scrollY:91,scrollTo(x,y){assert.equal(y,91);}};const sessionStorage={getItem(){return null;},setItem(){},removeItem(){}};
async function loadResumen(){state.resumen={fecha:'2026-10-03',agenda_counts:{vencidos:0,urgentes:0,programados:5},agenda:[{id:1,tipo:'plan',estado:'programado',proxima_ejecucion:'2026-11-02'}]};return state.resumen;}
async function loadBandeja(){return [];}
async function apiFetch(){return {ok:true,json:async()=>({ok:true,orden:{id:1,folio:'OM-1'},proxima_ejecucion:'2026-11-02'})};}
''' + script + '''
(async()=>{
await ejecutarPlanAgenda(1);const handler=form.onsubmit,draft=state.planExecutionDrafts.get('pd_plan_execution_1_1'),key=draft.clave;
await handler({preventDefault(){}});
assert.equal(oldGroup.children.length,0);assert.equal(oldGroup.hidden,true);assert.equal(saved.children[0],card);assert.equal(saved.hidden,false);assert.equal(form.onsubmit,handler);assert.equal(draft.clave,key);assert.equal(draft.guardado,true);assert.equal(fields.disabled,true);assert.match(feedback.textContent,/Ejecución guardada/);assert.equal(count.textContent,0);
showResults=false;card.dataset.dashboardPlan='1';await ejecutarPlanAgenda(1,true);await form.onsubmit({preventDefault(){}});assert.equal(label.textContent,'Última ejecución registrada');
})().catch(e=>{console.error(e);process.exit(1);});''')

    def test_strict_inbox_requires_current_complete_counts_and_accepts_empty_success(self):
        source = Path(settings.BASE_DIR, 'templates/mantenimiento/pwa.html').read_text()
        script = source[source.index('      async function loadBandeja'):source.index('      async function loadResumen')]
        self.run_node('''const assert=require('node:assert/strict');
const oldCounts={abiertos:2,en_proceso:1,criticos:1,cerrados:3};const oldItems=[{id:7}];
const state={bandeja:oldItems,counts:oldCounts,requestGeneration:{inbox:0}};
let payload={results:[]},httpOK=true;async function apiV2Fetch(){return {ok:httpOK,json:async()=>payload};}
''' + script + '''
(async()=>{
for(const counts of [undefined,{}, {abiertos:0,en_proceso:0,criticos:0,cerrados:-1}, {abiertos:0,en_proceso:0,criticos:0,cerrados:'0'}]){
 payload={results:[],counts};await assert.rejects(loadBandeja(true,true));assert.equal(state.counts,oldCounts);assert.equal(state.bandeja,oldItems);
}
httpOK=false;await assert.rejects(loadBandeja(true,true));assert.equal(state.counts,oldCounts);assert.equal(state.bandeja,oldItems);
httpOK=true;payload={results:[],counts:{abiertos:0,en_proceso:0,criticos:0,cerrados:0}};assert.deepEqual(await loadBandeja(true,true),[]);assert.deepEqual(state.counts,payload.counts);
state.counts=oldCounts;payload={results:[]};await loadBandeja(true);assert.equal(state.counts,oldCounts);
httpOK=false;state.bandeja=oldItems;await loadBandeja(true);assert.deepEqual(state.bandeja,[]);assert.equal(state.counts,oldCounts);
state.bandeja=oldItems;httpOK=true;let releaseBody,announceBody;const started=new Promise(resolve=>announceBody=resolve);const body=new Promise(resolve=>releaseBody=resolve);apiV2Fetch=async()=>({ok:true,json:()=>{announceBody();return body;}});const stale=loadBandeja(true,true);await started;state.requestGeneration.inbox++;releaseBody({results:[],counts:{abiertos:0,en_proceso:0,criticos:0,cerrados:0}});await assert.rejects(stale);assert.equal(state.counts,oldCounts);assert.equal(state.bandeja,oldItems);
})().catch(e=>{console.error(e);process.exit(1);});''')
