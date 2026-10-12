const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const context = vm.createContext({});
vm.runInContext(fs.readFileSync('static/js/orquestacion/agent-results.js', 'utf8'), context);
const view = context.ERPAgentView;
const hostile = '<img src=x onerror="alert(1)">';
const purchase = {kind:'purchase',draft_id:'purchase-test',status:'AWAITING_CONFIRMATION',version:1,payload_hash:'test-hash',area:'Producción',solicitante:'Carolina',
  fields:{area_id:1,solicitante_id:2,motivo:hostile,compra_reportada:true,envio_global:'129.00',total_reportado:'342.84'},
  items:[{descripcion:'Glicerina',cantidad:'2',unidad:'frasco',costo_unitario_estimado:'40.42',precio_total:'80.84'}],missing_fields:[],files:[]};
const purchaseHtml = view.incident(purchase);
assert(purchaseHtml.includes('40.42'));
assert(purchaseHtml.includes('sin repartir'));
assert(purchaseHtml.includes('captura de compra es opcional'));
assert(purchaseHtml.includes('data-domain="purchases"'));
assert(purchaseHtml.includes('Confirmar y enviar solicitud'));
assert(!purchaseHtml.includes('<img src=x'));
assert(!view.incident({...purchase,status:'EXECUTED',folio:'SCD-TEST',url:'/compras/departamentales/1/'}).includes('data-incident-id'));
assert(view.incident({...purchase,status:'WAITING_INFORMATION',fields:{},missing_fields:['area_id']}).includes(' disabled>Asociar captura'));
const wf = {public_id:'a" onmouseover="alert(1)',status:'WAITING_SELECTION',version:3,query:hostile,missing_fields:['asset'], options:[{position:1,available:false},{position:2,available:true,asset:{nombre:hostile,codigo:'TEST'}}]};
const html = view.workflow(wf,true);
assert(!html.includes('<img'));
assert(html.includes('&lt;img'));
assert(html.includes('Opción ya no disponible'));
assert(html.includes('2. '));
assert(html.includes('Continuar en la conversación'));
for (const status of ['RUNNING','REVALIDATION_REQUIRED','COMPLETED','CANCELLED','EXPIRED']) assert(!view.workflow({...wf,status},true).includes('data-workflow-id'));
const p = {activo:{nombre:hostile,codigo:'EQ1'},identidad:{marca:'Marca'},puede_ver_costos:false,costos:{adquisicion:'987.65'},history:{event_limit:10,orders_truncated:true},ordenes_recientes:[{folio:'OM1',estatus_label:'Pendiente',fecha:'2026-10-08'}]};
const call = {tool_key:'erp.get_asset_context',tool_display_name:'Consultar ficha',status:'complete',result:{result:{status:'ok',sources:['activos.Activo'],as_of:'2026-10-08T10:00:00Z',payload:p}}};
const result = view.tool(call);
assert(!result.includes('<img'));
assert(!result.includes('987.65'));
assert(result.includes('Historial parcial'));
assert(result.includes('OM1'));
assert(result.includes('activos.Activo'));
assert(view.tool({...call,status:'error'}).includes('No completada'));
assert(!view.tool({...call,status:'error'}).includes('OM1'));
assert(view.tool({...call,requires_approval:true}).includes('no se presenta como ejecutada'));
assert(!view.tool({...call,requires_approval:true}).includes('OM1'));
p.puede_ver_costos=true;
assert(view.tool(call).includes('987.65'));
assert(view.tool({tool_key:'workflow.resume_asset_maintenance',status:'complete',result:{result:{status:'COMPLETED',payload:{workflow:{...wf,status:'COMPLETED'},consultation:p}}}}).includes('OM1'));
assert(view.tool({status:'complete',tool_key:'read_unavailable',result:{}}).includes('El servidor registró el resultado'));
assert(!view.tool({status:'running',...call,status:'running'}).includes('OM1'));
assert(view.escape(null)==='');
assert(view.date('not-a-date')==='not-a-date');

const outside = view.tool({tool_key:'read.explain_limit',status:'complete',result:{result:{status:'out_of_scope',payload:{message:`Las consultas de otros módulos aún no están habilitadas. ${hostile}`}}}});
assert(outside.includes('Las consultas de otros módulos'));
assert(outside.includes('Fuera del alcance'));
assert(!outside.includes('Completada'));
assert(outside.includes('&lt;img'));
assert(!outside.includes('<img'));

const emptyPlans = view.tool({tool_key:'erp.get_pending_maintenance',status:'complete',result:{result:{status:'no_data',payload:{fecha_hasta:'2026-10-31'}}}});
assert(emptyPlans.includes('Hasta '));
assert(emptyPlans.includes('ausencia de servicio'));
assert(emptyPlans.includes('Sin fecha'));

assert(view.workflow({...wf,missing_fields:['query_or_asset']}).includes('Nombre o código del equipo'));
assert(view.workflow({...wf,missing_fields:['asset_selection']}).includes('Elegir uno de los equipos encontrados'));


// Run the actual template controller with a minimal DOM and controlled transport.
async function controllerChecks() {
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id, {textContent:'',innerHTML:'',value:'',disabled:false,hidden:false,
      open:false,scrollTop:0,scrollHeight:0,clientHeight:0,children:[],listeners:{},
      appendChild(child){this.children.push(child);},setAttribute(){},focus(){},
      querySelectorAll(){return [];},addEventListener(name,fn){this.listeners[name]=fn;}});
    return elements.get(id);
  };
  element('erp-chat-conversations-data').textContent='[]';
  element('erp-chat-selected-data').textContent=JSON.stringify({conversation:{id:'conversation-1'},messages:[]});
  element('erp-chat-runtime-status').textContent='{"ready":true}';
  const requests=[];
  let mode='truncated';
  let submittedId;
  let audioFile = new File(['fixture'], 'nota.wav', {type:'audio/wav'});
  let clears = 0;
  let releaseCreate;
  const controller = vm.createContext({ERPAgentView:view,TextDecoder,Date,FormData,
    crypto:require('node:crypto').webcrypto,
    ERPVoice:{attach:()=>({get file(){return audioFile;},clear(){audioFile=null;clears++;},setBusy(){}})},
    window:{matchMedia:()=>({matches:true})},
    document:{getElementById:element,querySelector:element,querySelectorAll:()=>[],createElement:()=>element(`node-${elements.size}`)},
    fetch:async (url,options={}) => {
      requests.push({url,method:options.method||'GET'});
      if (mode==='offline') throw new TypeError('Failed to fetch');
      if (url.endsWith('/new/')) await new Promise(resolve => {releaseCreate=resolve;});
      if (url.endsWith('/stream/')) {
        submittedId = options.body instanceof FormData ? options.body.get('client_request_id') : JSON.parse(options.body).client_request_id;
        let consumed=false;
        const event = ['truncated','pending','unrelated','photo-recovery'].includes(mode) ? 'event: chunk\ndata: {"delta":"parcial"}\n\n' : 'event: done\ndata: {"content":"Lista"}\n\n';
        return {ok:true,body:{getReader:()=>({read:async()=>consumed ? {done:true} : (consumed=true,{value:Buffer.from(event),done:false})})}};
      }
      return {ok:true,json:async()=>url.endsWith('/new/') ? {conversation:{id:'conversation-2'}} : url.endsWith('/conversations/') ? {items:[]} : {conversation:{id:'conversation-1'},messages:[{id:'a',client_request_id:mode==='unrelated' ? 'other-request' : submittedId,role:'assistant',status:mode==='pending' ? 'streaming' : mode==='server-error' ? 'error' : 'complete',content:'Lista',tool_calls:mode==='photo-recovery' ? [{tool_key:'purchase.prepare',status:'complete',result:{result:{payload:{incident:purchase}}}}] : []}]}};
    },Buffer,
  });
  const template=fs.readFileSync('templates/orquestacion/chat.html','utf8');
  const script=template.match(/<script>\s*([\s\S]*?)<\/script>/)[1].replace('})();','globalThis.testController={state,sendMessage,createConversation,refreshProcesses};})();');
  vm.runInContext(script,controller);
  const {state,sendMessage}=controller.testController;
  assert.equal(element('.agent-history').open,false);
  element('erp-chat-input').value='Consulta';
  await sendMessage();
  assert(element('erp-chat-status').textContent.includes('recuperada'));
  assert.equal(state.activeConversation.messages.at(-1).status,'complete');
  assert.equal(clears,1,'Accepted audio must not remain staged after recovery');
  assert(submittedId,'Correlate recovery with the submitted turn, not another message');
  assert.equal(requests.filter(r=>r.method==='POST').length,1,'No automatic retry after incomplete SSE');
  assert.equal(element('erp-chat-send-btn').disabled,false);
  for (const uncertain of ['pending','unrelated']) {
    mode=uncertain; element('erp-chat-input').value='Consulta incierta';
    // These modes simulate an interrupted response and a GET that is not final for this exact turn.
    const previous=requests.filter(r=>r.method==='POST').length;
    await sendMessage();
    assert.equal(requests.filter(r=>r.method==='POST').length,previous+1);
    assert.equal(element('erp-chat-send-btn').disabled,true);
    assert.equal(element('erp-chat-recover-btn').hidden,false);
    mode='complete'; await element('erp-chat-recover-btn').listeners.click();
    assert.equal(element('erp-chat-send-btn').disabled,false);
    assert.equal(element('erp-chat-new-btn').disabled,false);
  }
  for (const flag of ['loading','creating','sending','recording']) {
    state[flag]=true;
    element('erp-chat-input').value='Consulta durante bloqueo';
    const before=requests.length;
    await sendMessage();
    assert.equal(requests.length,before);
    state[flag]=false;
  }
  mode='complete';element('erp-chat-input').value='Otra consulta';
  await sendMessage();
  assert.equal(element('erp-chat-status').textContent,'Respuesta lista.');
  element('erp-chat-photos').files=[new File(['fixture'],'fixture.png',{type:'image/png'})];
  element('erp-chat-photos').value='selected fixture';element('erp-chat-input').value='Analiza la imagen';
  await sendMessage();
  assert.equal(element('erp-chat-photos').value,'','Stored images must not remain staged for the next message');
  assert.equal(element('erp-chat-photo-status').textContent,'');
  assert.equal(element('erp-chat-status').textContent,'Respuesta lista.');
  element('erp-chat-photos').files=[];
  mode='photo-recovery';
  element('erp-chat-photos').files=[new File(['fixture'],'fixture.png',{type:'image/png'})];
  element('erp-chat-input').value='Imagen con propuesta';
  const posts=requests.filter(r=>r.method==='POST').length;
  await sendMessage();
  assert.equal(requests.filter(r=>r.method==='POST').length,posts+1,'Recovery must never replay evidence writes either');
  assert(element('erp-chat-photo-status').textContent.includes('Revisa su asociación'));
  element('erp-chat-photos').files=[];mode='complete';
  const creating=element('erp-chat-new-btn').listeners.click();
  const before=requests.length;
  await element('erp-chat-new-btn').listeners.click();
  element('erp-chat-input').value='Consulta durante creación';
  await sendMessage();
  assert.equal(requests.length,before,'Do not send or create twice during creation');
  releaseCreate();await creating;
  assert.equal(state.activeConversation.conversation.id,'conversation-2');
  assert.equal(element('erp-chat-new-btn').disabled,false);
  mode='offline';await controller.testController.refreshProcesses();
  assert(element('agent-process-list').textContent.includes('usa Actualizar'));
  assert(!element('agent-process-list').textContent.includes('Failed to fetch'));
  mode='complete'; controller.crypto={randomUUID(){throw new TypeError('UUID unavailable');}};
  element('erp-chat-input').value='Consulta sin UUID';
  const beforeUuid=requests.length;
  await sendMessage();
  assert.equal(requests.length,beforeUuid);
  assert.equal(element('erp-chat-send-btn').disabled,false);
  console.log('Agent UI checks passed: safe projections, empty results, workflow fields, incomplete SSE, no retries, navigation locks and mobile history.');
}
controllerChecks().catch(error=>{console.error(error);process.exitCode=1;});

const proposal = {draft_id:'server-id', status:'AWAITING_CONFIRMATION', version:2, payload_hash:'a'.repeat(64), missing_fields:[], fields:{titulo:hostile,descripcion:'No gira',justificacion_sin_foto:'Sin cámara'}};
assert(view.incident(proposal).includes('Confirmar y crear reporte'));
assert(view.incident(proposal).includes('&lt;img'));
assert(!view.incident(proposal).includes('<img'));
for(const status of ['WAITING_INFORMATION','EXPIRED','EXECUTED']) assert(!view.incident({...proposal,status}).includes('data-incident-id'));
assert(view.incident({...proposal,status:'EXECUTED',report_id:42}).includes('#42'));
const natural = view.response({role:'assistant', presentation:'natural', content:'Primero mi respuesta\n\n<img src=x onerror=alert(1)>',
  receipts:[{...proposal,status:'EXECUTED',report_id:42}], tool_calls:[{status:'complete',tool_key:'erp.search_assets',result:{result:{payload:{items:[]}}}}]});
assert(natural.indexOf('Primero mi respuesta') < natural.indexOf('Comprobante del ERP'));
assert(!natural.includes('<img'));
assert(natural.includes('&lt;img'));
assert(natural.includes('#42'));
assert(!natural.includes('data-incident-id'));
assert(!natural.includes('agent-full-response'));
const costHtml = view.response({role:'assistant',content:'Lista',cost:{estimated_usd:'0.059134',reserved_usd:'0.516148',unmeasured_calls:1}});
assert(costHtml.includes('Consumo estimado'));
assert(costHtml.includes('Reserva preventiva'));
assert(costHtml.includes('No es un cargo confirmado'));
assert(costHtml.includes('sin consumo informado'));
assert(view.response({role:'assistant',presentation:'technical',content:'datos',tool_calls:[{status:'error'}]}).includes('Detalle técnico'));
assert(!view.response({role:'assistant',presentation:'natural',content:'Confirmar <button data-incident-id="fake">',tool_calls:[]}).includes('<button'));

const followup = {...proposal,kind:'followup',fields:{comentario:hostile,estatus:'resuelto',fecha_trabajo_finalizado:'2026-10-09'},report:{id:114,titulo:'Filtración',sucursal:'El Tunel',proveedor:'Pedro',costo_estimado:'2500.00'},files:[]};
assert(view.incident(followup).includes('Confirmar actualización'));
assert(!view.incident(followup).includes('Confirmar y crear'));
assert(view.incident(followup).includes('&lt;img'));
assert(!view.incident(followup).includes('<img'));
assert(view.incident(followup).includes('9 oct 2026'));
assert(view.incident({...followup,missing_fields:['fotografías']}).includes(' disabled'));
assert(!view.incident({...followup,status:'REVIEW_REQUIRED'}).includes('data-incident-id'));
assert(!view.incident({...followup,status:'EXECUTED'}).includes('data-followup-upload'));
assert(view.incident({...followup,status:'EXECUTED'}).includes('#114'));
