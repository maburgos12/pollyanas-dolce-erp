(function () {
  'use strict';
  var activeRoot = null;
  var memoryFallback = {};
  // A fresh document owns a fresh durable key. Duplicated tabs may copy
  // sessionStorage pointers, but never write to the source document's draft.
  var tabToken=uuid(), tabStorage=true;
  try{sessionStorage.setItem('pollyanas.conteos.available','1');}catch(_){tabStorage=false;}
  function key(root) { return 'pollyanas.conteos.v1:u'+root.dataset.user+':c'+root.dataset.count+':r'+root.dataset.round+':t'+tabToken; }
  function uuid() {
    var bytes = new Uint8Array(16); window.crypto.getRandomValues(bytes);
    bytes[6]=(bytes[6]&15)|64; bytes[8]=(bytes[8]&63)|128;
    var h=Array.from(bytes,function(b){return b.toString(16).padStart(2,'0');}).join('');
    return h.slice(0,8)+'-'+h.slice(8,12)+'-'+h.slice(12,16)+'-'+h.slice(16,20)+'-'+h.slice(20);
  }
  function values(form) {
    var out={};
    form.querySelectorAll('input[name^="cantidad_"],input[name^="contado_en_"],textarea[name^="incidencia_"],textarea[name="observaciones"]').forEach(function(el){out[el.name]=el.value;});
    return out;
  }
  var inspected={};
  function read(storageKey) {
    if(memoryFallback[storageKey])return memoryFallback[storageKey];
    try{
      var current=JSON.parse(localStorage.getItem(storageKey)||'null');
      if(current)return current;
      if(inspected[storageKey])return null;
      inspected[storageKey]=true;
      var prefix=storageKey.split(':t')[0]+':t',pointer=tabStorage&&sessionStorage.getItem('pointer:'+prefix);
      var inherited=pointer&&JSON.parse(localStorage.getItem(pointer)||'null');
      if(!inherited){
        var choices=[],seen={};
        Object.keys(localStorage).filter(function(k){return k.indexOf(prefix)===0;}).forEach(function(k){
          var data;try{data=JSON.parse(localStorage.getItem(k));}catch(_){return;}
          if(data&&data.values&&!seen[data.request_id]){seen[data.request_id]=true;choices.push(data);}
        });
        if(choices.length>1)return {choices:choices};
        inherited=choices[0];
      }
      if(inherited){memoryFallback[storageKey]=inherited;localStorage.setItem(storageKey,JSON.stringify(inherited));if(tabStorage)sessionStorage.setItem('pointer:'+prefix,storageKey);return inherited;}
      return null;
    }catch(_){return memoryFallback[storageKey]||null;}
  }
  function write(root,data) {
    var k=key(root);data.updated_at=new Date().toISOString();memoryFallback[k]=data;
    try { if(!tabStorage)throw new Error('No tab storage');localStorage.setItem(k,JSON.stringify(data));sessionStorage.setItem('pointer:'+k.split(':t')[0]+':t',k); return true; }
    catch (_) { var warning=root.querySelector('[data-draft-warning]'); if(warning){warning.hidden=false;warning.textContent='Este navegador no permite conservar el borrador. Mantén esta página abierta y guarda en el servidor antes de salir.';} return false; }
  }
  function remove(k) {
    var saved=read(k);delete memoryFallback[k];inspected[k]=true;
    try{
      Object.keys(localStorage).filter(function(other){return other.indexOf(k.split(':t')[0]+':t')===0;}).forEach(function(other){
        var data;try{data=JSON.parse(localStorage.getItem(other));}catch(_){return;}
        if(other===k||(saved&&data&&data.request_id===saved.request_id&&String(data.version)===String(saved.version)))localStorage.removeItem(other);
      });
    }catch(_){}
  }
  var reported={};
  function report(root,code) {
    if(!root||!root.dataset.diagnosticUrl||reported[root.dataset.count+':'+code])return;
    var token=root.querySelector('[name="csrfmiddlewaretoken"]');if(!token)return;
    reported[root.dataset.count+':'+code]=true;
    var data=new FormData();data.set('csrfmiddlewaretoken',token.value);data.set('code',code);
    fetch(root.dataset.diagnosticUrl,{method:'POST',body:data,credentials:'same-origin'}).catch(function(){});
  }
  window.addEventListener('error',function(event){if(/\/(conteos|erp_actions)\.js/.test(event.filename||''))report(activeRoot,'error_javascript');});
  function state(root,text) {var el=root.querySelector('[data-save-state]');if(el)el.textContent=text;}
  function updateProgress(root) {
    var total=0, complete=0;
    root.querySelectorAll('[data-count-line]').forEach(function(row){total++;var q=row.querySelector('input[name^="cantidad_"]'),i=row.querySelector('textarea');if(q.value.trim()!==''||i.value.trim()!=='')complete++;});
    var text=root.querySelector('[data-count-progress]'),bar=root.querySelector('[role="progressbar"]');
    if(text)text.textContent=complete+' de '+total+' registrados';
    if(bar){bar.setAttribute('aria-valuenow',complete);bar.value=complete;}
  }
  function bind() {
    var root=document.querySelector('[data-count-root]');
    if(!root||root===activeRoot)return;
    var old=activeRoot;
    activeRoot=root;
    if(old&&old.dataset.round===root.dataset.round){
      old.querySelectorAll('form[data-async-action]:not([data-count-form])').forEach(function(previous){
        var action=previous.querySelector('input[name="action"]');
        if(!action||previous.elements.request_id.value===root.dataset.ack)return;
        var next=Array.from(root.querySelectorAll('form')).find(function(candidate){return candidate.elements.action&&candidate.elements.action.value===action.value;});
        if(!next)return;
        previous.querySelectorAll('textarea[name],input[type="checkbox"][name]').forEach(function(field){
          var matches=Array.from(next.querySelectorAll('[name]')).filter(function(el){return el.name===field.name&&(field.type!=='checkbox'||el.value===field.value);});
          matches.forEach(function(el){if(field.type==='checkbox')el.checked=field.checked;else el.value=field.value;});
        });
      });
    }
    if(old){try{var scroll=sessionStorage.getItem('conteos.scroll:'+key(root));if(scroll)window.scrollTo(0,Number(scroll));}catch(_) {}}
    var form=root.querySelector('[data-count-form]'),draft=read(key(root));
    var serverValues=form?JSON.stringify(values(form)):null;
    if(draft&&draft.choices){
      var picker=root.querySelector('[data-draft-warning]');picker.hidden=false;
      picker.textContent='Hay varios borradores de este conteo en el dispositivo. Elige cuál revisar antes de continuar.';
      draft.choices.forEach(function(choice){
        var detail=document.createElement('details'),title=document.createElement('summary');
        title.textContent='Borrador · versión '+choice.version+' · '+(choice.updated_at?new Date(choice.updated_at).toLocaleString():'fecha no disponible');detail.appendChild(title);
        var valuesList=document.createElement('ul');Object.keys(choice.values).forEach(function(name){var el=form&&form.elements.namedItem(name),item=document.createElement('li');item.textContent=(name==='observaciones'?'Observaciones':el&&el.getAttribute('aria-label')||name)+': '+(choice.values[name]||'(vacío)');valuesList.appendChild(item);});detail.appendChild(valuesList);
        var use=document.createElement('button');use.type='button';use.className='count-button secondary';use.textContent='Revisar este borrador';use.addEventListener('click',function(){write(root,choice);picker.replaceChildren();picker.hidden=true;activeRoot=null;bind();});detail.appendChild(use);picker.appendChild(detail);
      });
      if(form)form.addEventListener('submit',function(event){if(!form.dataset.draftChosen){event.preventDefault();event.stopImmediatePropagation();picker.scrollIntoView({block:'center'});}},true);
      // Rebinding replaces this guard by allowing the selected draft below.
      return;
    }
    if(form)form.dataset.draftChosen='true';
    if(root.dataset.ack && draft && draft.request_id===root.dataset.ack && String(draft.version)===root.dataset.ackVersion){remove(key(root));draft=null;}
    if(!form){
      if(draft){
        var retained=root.querySelector('[data-draft-warning]');retained.hidden=false;
        retained.textContent='Este dispositivo conserva cambios sin confirmar. Compáralos con la captura recibida que aparece abajo. Si faltan cambios, solicita un reconteo; no se enviarán automáticamente.';
        var pending=document.createElement('ul');
        Object.keys(draft.values||{}).forEach(function(name){
          var ident=name.split('_').pop(),row=root.querySelector('[data-line-id="'+ident+'"]');
          var label=name==='observaciones'?'Observaciones':(name.indexOf('incidencia_')===0?'Incidencia':name.indexOf('contado_en_')===0?'Hora del conteo':'Cantidad')+' · '+(row?row.querySelector('strong').textContent:ident);
          var item=document.createElement('li');item.textContent=label+': '+(draft.values[name]===''?'(vacío)':draft.values[name]);pending.appendChild(item);
        });retained.appendChild(pending);
        var dismiss=document.createElement('button');dismiss.type='button';dismiss.className='count-button secondary';dismiss.textContent='Ya comparé y resguardé estos cambios';dismiss.addEventListener('click',function(){remove(key(root));retained.hidden=true;});retained.appendChild(dismiss);
      }
      return;
    }
    var conflict=false,warning=root.querySelector('[data-draft-warning]');
    if(draft && String(draft.version)===root.dataset.version){
      Object.keys(draft.values||{}).forEach(function(name){var el=form.elements.namedItem(name);if(el)el.value=draft.values[name];});
      if(draft.request_id)form.elements.request_id.value=draft.request_id;
      state(root,'Borrador recuperado en este dispositivo. Guarda para confirmar.');
    }else if(draft){
      conflict=true;warning.hidden=false;warning.textContent='Hay un borrador de una versión anterior. El servidor cambió. Revisa estas cantidades y combina manualmente lo que corresponda antes de continuar.';
      var list=document.createElement('ul');
      Object.keys(draft.values||{}).forEach(function(name){
        var input=form.elements.namedItem(name),line=document.createElement('li');
        var label=input&&(input.getAttribute('aria-label')||(input.labels&&input.labels[0]&&input.labels[0].textContent));
        if(name==='observaciones')label='Observaciones';
        var product=input&&input.closest('[data-count-line]');
        if(name.indexOf('incidencia_')===0&&product)label='Incidencia de '+product.querySelector('.count-product label').textContent;
        line.textContent=(label||name)+': '+(draft.values[name]===''?'(vacío)':draft.values[name]);
        if(input){var recover=document.createElement('button');recover.type='button';recover.className='count-button secondary';recover.textContent='Usar este valor';recover.addEventListener('click',function(){input.value=draft.values[name];updateProgress(root);recover.textContent='Valor recuperado';});line.appendChild(recover);}
        list.appendChild(line);
      });
      warning.appendChild(list);
      var reconcile=document.createElement('button');reconcile.type='button';reconcile.className='count-button secondary';reconcile.textContent='He revisado y combinado los datos';
      reconcile.addEventListener('click',function(){conflict=false;warning.hidden=true;form.elements.request_id.value=uuid();persist();});warning.appendChild(reconcile);
    }
    var saveTimer, saving=false, stopped=Boolean(draft), pending=draft&&draft.pending;
    var confirmed=serverValues;
    form.noValidate=true;
    function notice(message) {
      state(root,message);
      window.ERPActionUI.showToast({type:'warning',message:message,persistent:true});
    }
    function reveal(input) {
      var row=input.closest('[data-count-line]');
      if(row){selectType(row.dataset.countType);search.value='';search.dispatchEvent(new Event('input'));}
      var details=input.closest('details');if(details)details.open=true;
      input.scrollIntoView({block:'center'});input.focus({preventScroll:true});
    }
    function valid(show) {
      var invalid=Array.from(form.elements).find(function(el){return el.willValidate&&!el.validity.valid;});
      if(!invalid)return true;
      if(show){reveal(invalid);invalid.reportValidity();notice('Revisa el campo señalado antes de guardar. Tu borrador se conserva.');}
      return false;
    }
    function scheduleSave() {
      window.clearTimeout(saveTimer);
      if(!stopped&&!conflict&&!saving&&navigator.onLine&&JSON.stringify(values(form))!==confirmed){
        saveTimer=window.setTimeout(function(){save('guardar',true);},2000);
      }
    }
    async function save(action,automatic) {
      window.clearTimeout(saveTimer);
      if(root!==activeRoot)return;
      if(saving){if(!automatic)notice('Hay un guardado en curso. Espera su confirmación antes de continuar.');return;}
      if(conflict){warning.scrollIntoView({block:'center'});return;}
      if(!pending&&!valid(!automatic))return;
      if((!('onformdata' in form)&&form.elements.length>900)||new Blob([JSON.stringify(values(form))]).size>1800000){
        stopped=true;notice('La captura es demasiado grande para este navegador. Conserva el borrador y reduce las notas extensas antes de guardar.');return;
      }
      try{sessionStorage.setItem('conteos.scroll:'+key(root),String(window.scrollY));}catch(_){}
      if(!navigator.onLine){persist();state(root,'Sin conexión. El borrador sigue pendiente de guardar.');return;}
      if(!pending){
        var body=new FormData(form);body.set('action',action);
        pending={entries:Array.from(body.entries()).filter(function(pair){return pair[0]!=='csrfmiddlewaretoken';}),values:values(form)};
      }
      var attempt=pending, data=new FormData();
      attempt.entries.forEach(function(pair){data.append(pair[0],pair[1]);});
      var csrfCookie=document.cookie.split('; ').find(function(item){return item.indexOf('csrftoken=')===0;});
      data.set('csrfmiddlewaretoken',csrfCookie?decodeURIComponent(csrfCookie.slice(10)):form.elements.csrfmiddlewaretoken.value);
      var sentAction=data.get('action'),sentId=data.get('request_id');
      persist();saving=true;state(root,'Guardando en el servidor…');
      // Manual actions can replace the form; prevent edits that replacement would discard.
      var locked=sentAction!=='guardar'?Array.from(form.elements).filter(function(el){return !el.disabled;}):[];
      locked.forEach(function(el){el.disabled=true;});
      var controller=new AbortController(),timeout=window.setTimeout(function(){controller.abort();},25000);
      try{
        var response=await fetch(form.getAttribute('action'),{method:'POST',body:data,credentials:'same-origin',
          headers:{Accept:'application/json','X-Requested-With':'XMLHttpRequest'},signal:controller.signal});
        if(response.redirected||!(response.headers.get('content-type')||'').includes('application/json')){
          throw new Error('No se confirmó el guardado. Si tu sesión venció, inicia sesión en otra pestaña y pulsa Guardar avance.');
        }
        var result=await response.json();
        if(!response.ok||!result.ok){
          if([400,403,404,409].includes(response.status))pending=null;
          throw new Error(result.toast&&result.toast.message||'No se confirmó el guardado. Revisa tu conexión y pulsa Guardar avance.');
        }
        var received=new DOMParser().parseFromString(result.html||'','text/html').querySelector('[data-count-root]');
        if(!received||received.dataset.ack!==sentId||received.dataset.count!==root.dataset.count){throw new Error('La confirmación no corresponde a esta captura. Conserva la página y pulsa Guardar avance.');}
        pending=null;stopped=false;
        if(sentAction==='guardar'){
          // The receipt may be replayed after another tab advanced: never adopt a newer version silently.
          if(String(result.result.version)!==received.dataset.version){throw new Error('El conteo cambió en otra pestaña. Vuelve a abrirlo para comparar tu borrador.');}
          root.dataset.version=received.dataset.version;
          root.querySelectorAll('input[name="version"]').forEach(function(el){el.value=received.dataset.version;});
          form.elements.request_id.value=uuid();confirmed=JSON.stringify(attempt.values);
          if(JSON.stringify(values(form))===confirmed){remove(key(root));state(root,'Guardado en servidor · '+new Date().toLocaleTimeString());}
          else persist();
        }else{
          if(JSON.stringify(values(form))===JSON.stringify(attempt.values))remove(key(root));
          else persist();
          root.outerHTML=result.html;window.ERPActionUI.bind(document);bind();
          window.ERPActionUI.showToast(result.toast||{type:'success',message:'Captura confirmada.'});
        }
      }catch(error){
        stopped=true;persist();notice(error.name==='AbortError'?'No llegó la confirmación a tiempo. El borrador se conserva; pulsa Guardar avance para verificar el mismo intento.':error.message||'No se confirmó el guardado. Conserva esta página y pulsa Guardar avance.');
        report(root,'guardado_no_confirmado');
      }finally{
        window.clearTimeout(timeout);saving=false;locked.forEach(function(el){el.disabled=false;});
        if(root===activeRoot)scheduleSave();
      }
    }
    function persist() {
      if(conflict)return;
      if(!pending&&JSON.stringify(values(form))===confirmed){remove(key(root));return;}
      write(root,{version:root.dataset.version,request_id:form.elements.request_id.value,values:values(form),pending:pending});
      scheduleSave();
      state(root,'Cambios pendientes de guardar en el servidor.');
    }
    form.addEventListener('formdata',function(event){
      var data=event.formData,readings={};
      form.querySelectorAll('input[name^="cantidad_"]').forEach(function(input){var id=input.name.slice(9);readings[id]={cantidad:input.value,incidencia:form.elements.namedItem('incidencia_'+id).value,contado_en:form.elements.namedItem('contado_en_'+id).value};});
      Array.from(data.keys()).forEach(function(name){if(name.indexOf('cantidad_')===0||name.indexOf('incidencia_')===0||name.indexOf('contado_en_')===0)data.delete(name);});
      data.set('lecturas_json',JSON.stringify(readings));
    });
    var serverClock=Date.parse(root.dataset.serverNow), wallStart=Date.now(), monoStart=performance.now();
    form.addEventListener('input',function(event){
      if(!event.target.name)return;
      if(event.target.name.indexOf('cantidad_')===0){
        var row=event.target.closest('[data-count-line]'), declared=row.querySelector('input[name^="contado_en_"]');
        var elapsed=Date.now()-wallStart;
        if(event.target.value==='')declared.value='';
        if(event.target.value!==''&&!declared.value&&Number.isFinite(serverClock)&&Math.abs(elapsed-(performance.now()-monoStart))<60000){
          declared.value=new Date(serverClock+elapsed+Number(root.dataset.serverOffset)*60000).toISOString().slice(0,19);
        }
        var incidence=row.querySelector('textarea');
        incidence.value=incidence.value.replace(/^No se maneja en esta sucursal\.\s*/,'');
      }
      // A changed body is a new intent; never reuse a receipt for different data.
      form.elements.request_id.value=uuid();persist();updateProgress(root);
      var preview=root.querySelector('[data-send-preview]');if(preview)preview.hidden=true;
    });
    root.querySelectorAll('[data-not-handled]').forEach(function(button){
      button.addEventListener('click',function(){
        var row=button.closest('[data-count-line]'),quantity=row.querySelector('input[name^="cantidad_"]'),incidence=row.querySelector('textarea');
        if(quantity.value!==''){window.ERPActionUI.showToast({type:'warning',message:'Este artículo ya tiene una cantidad. Revísala antes de marcar que no se maneja.'});return;}
        var marker='No se maneja en esta sucursal.';
        if(incidence.value.trim()===marker)incidence.value='';
        else if(incidence.value.indexOf(marker)<0){
          var next=marker+(incidence.value.trim()?' '+incidence.value:'');
          if(next.length>incidence.maxLength){window.ERPActionUI.showToast({type:'warning',message:'La incidencia ya ocupa el espacio disponible. Revísala para registrar que no se maneja.'});return;}
          incidence.value=next;
        }
        row.querySelector('details').open=Boolean(incidence.value);
        incidence.dispatchEvent(new Event('input',{bubbles:true}));
      });
    });
    form.addEventListener('submit',function(event){
      event.preventDefault();event.stopImmediatePropagation();
      save(event.submitter?event.submitter.value:'guardar',false);
    },true);
    form.addEventListener('change',function(){persist();updateProgress(root);});
    var search=root.querySelector('[data-count-search]');
    search.addEventListener('input',function(){var q=search.value.toLocaleLowerCase();root.querySelectorAll('[data-count-line]').forEach(function(row){row.hidden=row.dataset.search.toLocaleLowerCase().indexOf(q)<0;});});
    var activeType='producto';
    var typeTabs=Array.from(root.querySelectorAll('[data-count-type-tab]'));
    var catalogQuery=root.querySelector('[data-catalog-query]');
    var catalogResults=root.querySelector('[data-catalog-results]');
    var catalogSubmit=root.querySelector('[data-catalog-submit]');
    var searchTimer, searchVersion=0;
    function selectType(type){
      activeType=type;
      searchVersion++;window.clearTimeout(searchTimer);
      catalogSubmit.disabled=false;
      typeTabs.forEach(function(tab){tab.setAttribute('aria-selected',String(tab.dataset.countTypeTab===type));});
      root.querySelectorAll('[data-count-group]').forEach(function(group){group.hidden=group.dataset.countGroup!==type;});
      var label=root.querySelector('[data-catalog-type-label]');if(label)label.textContent=type==='producto'?'producto':'insumo';
      if(catalogResults)catalogResults.replaceChildren();
      search.dispatchEvent(new Event('input'));
      if(catalogQuery.value.trim())searchCatalog();
    }
    typeTabs.forEach(function(tab){tab.addEventListener('click',function(){selectType(tab.dataset.countTypeTab);});});
    function renderCatalog(items){
      catalogResults.replaceChildren();
      if(!items.length){var empty=document.createElement('p');empty.className='count-catalog-empty';empty.textContent='No se encontraron artículos disponibles con esa búsqueda.';catalogResults.appendChild(empty);return;}
      items.forEach(function(item){
        var row=document.createElement('div');row.className='count-catalog-result';
        var copy=document.createElement('span'),name=document.createElement('strong'),meta=document.createElement('small');
        name.textContent=item.nombre;meta.textContent=item.codigo+(item.unidad?' · '+item.unidad:' · Unidad pendiente');
        copy.append(name,meta);
        var add=document.createElement('button');add.type='button';add.className='count-button secondary';add.textContent='Agregar';
        if(!item.unidad){add.disabled=true;add.title='La unidad debe sincronizarse antes de agregar el artículo.';}
        add.addEventListener('click',function(){form.elements.articulo.value=item.key;form.elements.request_id.value=uuid();form.requestSubmit(form.querySelector('[data-add-submit]'));});
        row.append(copy,add);catalogResults.appendChild(row);
      });
    }
    function searchCatalog(){
      window.clearTimeout(searchTimer);
      var q=catalogQuery.value.trim(),version=++searchVersion;
      if(!q){catalogResults.replaceChildren();catalogSubmit.disabled=false;return;}
      catalogSubmit.disabled=true;catalogResults.textContent='Buscando…';
      var url=new URL(form.dataset.catalogUrl,window.location.origin);url.searchParams.set('tipo',activeType);url.searchParams.set('q',q);
      fetch(url.toString(),{headers:{'Accept':'application/json'},credentials:'same-origin'})
        .then(function(response){return response.json().then(function(data){if(!response.ok)throw new Error(data.error||'No fue posible buscar.');return data;});})
        .then(function(data){if(version===searchVersion&&root===activeRoot)renderCatalog(data.resultados||[]);})
        .catch(function(error){if(version===searchVersion&&root===activeRoot)catalogResults.textContent=error.message;})
        .finally(function(){if(version===searchVersion)catalogSubmit.disabled=false;});
    }
    catalogQuery.addEventListener('input',function(){searchVersion++;window.clearTimeout(searchTimer);catalogResults.textContent=catalogQuery.value.trim()?'Buscando…':'';searchTimer=window.setTimeout(searchCatalog,250);});
    catalogSubmit.addEventListener('click',searchCatalog);
    catalogQuery.addEventListener('keydown',function(event){if(event.key==='Enter'){event.preventDefault();searchCatalog();}});
    if(old&&old.dataset.round===root.dataset.round){
      var oldSearch=old.querySelector('[data-count-search]');
      if(oldSearch){search.value=oldSearch.value;search.dispatchEvent(new Event('input'));}
      var oldType=old.querySelector('[data-count-type-tab][aria-selected="true"]');
      if(oldType)selectType(oldType.dataset.countTypeTab);
      old.querySelectorAll('details[open] textarea[id]').forEach(function(el){var current=document.getElementById(el.id);if(current)current.closest('details').open=true;});
    }
    root.querySelector('[data-review-send]').addEventListener('click',function(){
      if(!valid(true))return;
      var preview=root.querySelector('[data-send-preview]'),lines=root.querySelector('[data-preview-lines]');lines.replaceChildren();var missing=null;
      root.querySelectorAll('[data-count-line]').forEach(function(row){var q=row.querySelector('input[name^="cantidad_"]'),i=row.querySelector('textarea');if(q.value===''&&!i.value.trim()&&!missing)missing=q;var entry=document.createElement('div');entry.className='count-preview-row';var name=document.createElement('span');name.textContent=row.querySelector('.count-product label').textContent;var amount=document.createElement('strong');amount.textContent=q.value!==''?q.value+' '+row.querySelector('.count-unit').textContent:(i.value.trim()?'Incidencia registrada':'Sin contar');entry.append(name,amount);lines.appendChild(entry);});
      if(missing){reveal(missing);notice('Falta registrar: '+missing.closest('[data-count-line]').querySelector('.count-product label').textContent+'. Captura su cantidad o marca «No se maneja».');report(root,'captura_incompleta');return;}
      preview.hidden=false;preview.scrollIntoView({block:'center',behavior:'auto'});
    });
    root.querySelector('[data-close-preview]').addEventListener('click',function(){root.querySelector('[data-send-preview]').hidden=true;});
    root.savePending=function(){persist();};
    root.resumeCapture=function(){if(read(key(root)))state(root,'Revisa tu captura pendiente. Pulsa Guardar avance para confirmar.');};
    if(draft)state(root,'Borrador recuperado. Revisa los datos y pulsa Guardar avance.');
    updateProgress(root);
  }
  bind();
  new MutationObserver(function(records){
    bind();
    records.forEach(function(record){if(record.type==='attributes'&&record.target.hasAttribute('data-count-form')&&record.target.dataset.actionPending==='false'&&activeRoot&&read(key(activeRoot)))state(activeRoot,'Cambios sin confirmar. Revisa el mensaje y vuelve a guardar cuando puedas.');});
  }).observe(document.body,{childList:true,subtree:true,attributes:true,attributeFilter:['data-action-pending']});
  document.addEventListener('input',function(event){
    var form=event.target.closest('form[data-async-action]');
    if(form && !form.hasAttribute('data-count-form') && form.elements.request_id)form.elements.request_id.value=uuid();
  });
  document.addEventListener('submit',function(event){
    var form=event.target;
    if(activeRoot&&activeRoot.contains(form)){try{sessionStorage.setItem('conteos.scroll:'+key(activeRoot),String(window.scrollY));}catch(_) {}}
    if(!form.querySelector('input[type="file"]'))return;
    var capture=document.querySelector('[data-count-form]');
    if(capture && read(key(activeRoot))){event.preventDefault();event.stopImmediatePropagation();window.ERPActionUI.showToast({type:'warning',message:'Guarda primero las cantidades pendientes antes de adjuntar evidencia.',persistent:true});}
  },true);
  window.addEventListener('offline',function(){if(activeRoot)state(activeRoot,'Sin conexión. La captura no se ha enviado.');});
  window.addEventListener('online',function(){if(activeRoot&&activeRoot.resumeCapture)activeRoot.resumeCapture();});
  document.addEventListener('visibilitychange',function(){
    if(!activeRoot)return;
    if(document.visibilityState==='hidden'&&activeRoot.savePending)activeRoot.savePending();
    else if(activeRoot.resumeCapture)activeRoot.resumeCapture();
  });
  window.addEventListener('pagehide',function(){if(activeRoot&&activeRoot.savePending)activeRoot.savePending();});
  window.addEventListener('pageshow',function(event){if(event.persisted&&activeRoot&&activeRoot.resumeCapture)activeRoot.resumeCapture();});
  window.addEventListener('beforeunload',function(event){
    if(activeRoot && document.querySelector('[data-count-form]') && read(key(activeRoot))){event.preventDefault();event.returnValue='';}
  });
})();
