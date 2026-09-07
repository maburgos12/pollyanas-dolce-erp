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
    form.querySelectorAll('input[name^="cantidad_"],textarea[name^="incidencia_"],textarea[name="observaciones"]').forEach(function(el){out[el.name]=el.value;});
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
  function state(root,text) {var el=root.querySelector('[data-save-state]');if(el)el.textContent=text;}
  function updateProgress(root) {
    var total=0, complete=0;
    root.querySelectorAll('[data-count-line]').forEach(function(row){total++;var q=row.querySelector('input[name^="cantidad_"]'),i=row.querySelector('textarea');if(q.value.trim()!==''||i.value.trim()!=='')complete++;});
    var text=root.querySelector('[data-count-progress]'),bar=root.querySelector('[role="progressbar"]');
    if(text)text.textContent=complete+' de '+total+' contados';
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
          var label=name==='observaciones'?'Observaciones':(name.indexOf('incidencia_')===0?'Incidencia':'Cantidad')+' · '+(row?row.querySelector('strong').textContent:ident);
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
    function persist() {
      if(conflict)return;
      write(root,{version:root.dataset.version,request_id:form.elements.request_id.value,values:values(form)});
      state(root,'Cambios pendientes de guardar en el servidor.');
    }
    form.addEventListener('formdata',function(event){
      var data=event.formData,readings={};
      form.querySelectorAll('input[name^="cantidad_"]').forEach(function(input){var id=input.name.slice(9);readings[id]={cantidad:input.value,incidencia:form.elements.namedItem('incidencia_'+id).value};});
      Array.from(data.keys()).forEach(function(name){if(name.indexOf('cantidad_')===0||name.indexOf('incidencia_')===0)data.delete(name);});
      data.set('lecturas_json',JSON.stringify(readings));
    });
    form.addEventListener('input',function(event){
      if(!event.target.name)return;
      // A changed body is a new intent; never reuse a receipt for different data.
      form.elements.request_id.value=uuid();persist();updateProgress(root);
      var preview=root.querySelector('[data-send-preview]');if(preview)preview.hidden=true;
    });
    form.addEventListener('submit',function(event){
      if((!('onformdata' in form)&&form.elements.length>900)||new Blob([JSON.stringify(values(form))]).size>1800000){
        event.preventDefault();event.stopImmediatePropagation();
        window.ERPActionUI.showToast({type:'error',message:!('onformdata' in form)?'Actualiza el navegador para enviar un conteo de este tamaño. El borrador se conserva.':'La captura contiene demasiadas notas para un envío. Conserva las notas extensas como evidencia y reduce el texto antes de guardar. El borrador se conserva.',persistent:true});
        return;
      }
      if(conflict){event.preventDefault();event.stopImmediatePropagation();warning.scrollIntoView({block:'center'});return;}
      var button=event.submitter;
      var data={version:root.dataset.version,request_id:form.elements.request_id.value,values:values(form),action:button?button.value:'guardar'};
      var prior=read(key(root));
      // Retrying the same body/action retains its UUID. Changing action uses a new one.
      if(prior && prior.action && prior.action!==data.action){data.request_id=uuid();form.elements.request_id.value=data.request_id;}
      write(root,data);
      try{sessionStorage.setItem('conteos.scroll:'+key(root),String(window.scrollY));}catch(_){}
      state(root,'Esperando confirmación del servidor…');
    },true);
    var search=root.querySelector('[data-count-search]');
    search.addEventListener('input',function(){var q=search.value.toLocaleLowerCase();root.querySelectorAll('[data-count-line]').forEach(function(row){row.hidden=row.dataset.search.toLocaleLowerCase().indexOf(q)<0;});});
    if(old&&old.dataset.round===root.dataset.round){
      var oldSearch=old.querySelector('[data-count-search]');
      if(oldSearch){search.value=oldSearch.value;search.dispatchEvent(new Event('input'));}
      old.querySelectorAll('details[open] textarea[id]').forEach(function(el){var current=document.getElementById(el.id);if(current)current.closest('details').open=true;});
    }
    root.querySelector('[data-review-send]').addEventListener('click',function(){
      if(!form.reportValidity())return;
      var preview=root.querySelector('[data-send-preview]'),lines=root.querySelector('[data-preview-lines]');lines.replaceChildren();var missing=false;
      root.querySelectorAll('[data-count-line]').forEach(function(row){var q=row.querySelector('input[name^="cantidad_"]'),i=row.querySelector('textarea');if(q.value===''&&!i.value.trim())missing=true;var entry=document.createElement('div');entry.className='count-preview-row';var name=document.createElement('span');name.textContent=row.querySelector('.count-product label').textContent;var amount=document.createElement('strong');amount.textContent=q.value!==''?q.value+' '+row.querySelector('.count-unit').textContent:(i.value.trim()?'Incidencia registrada':'Sin contar');entry.append(name,amount);lines.appendChild(entry);});
      if(missing){window.ERPActionUI.showToast({type:'error',message:'Completa las cantidades o registra una incidencia en cada artículo.',persistent:true});return;}
      preview.hidden=false;preview.scrollIntoView({block:'center',behavior:'auto'});
    });
    root.querySelector('[data-close-preview]').addEventListener('click',function(){root.querySelector('[data-send-preview]').hidden=true;});
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
  window.addEventListener('online',function(){if(activeRoot && read(key(activeRoot)))state(activeRoot,'Conexión recuperada. Pulsa Guardar para enviar tu borrador.');});
  window.addEventListener('beforeunload',function(event){
    if(activeRoot && document.querySelector('[data-count-form]') && read(key(activeRoot))){event.preventDefault();event.returnValue='';}
  });
})();
