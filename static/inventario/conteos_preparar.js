(function () {
  'use strict';
  var search=document.querySelector('[data-catalog-search]');
  var form=document.getElementById('preparar-conteo');
  if(!search||!form)return;
  var busy=false;
  form.addEventListener('submit',function(event){if(!('onformdata' in form)&&form.elements.length>900){event.preventDefault();event.stopImmediatePropagation();window.ERPActionUI.showToast({type:'error',message:'Actualiza el navegador para preparar un conteo de este tamaño. Tus selecciones permanecen en pantalla.',persistent:true});}},true);
  form.addEventListener('formdata',function(event){
    var items=[];
    form.querySelectorAll('input[name="articulos"]:checked').forEach(function(box){var item={unidad:form.elements.namedItem('unidad_'+box.value).value,fuente_unidad:form.elements.namedItem('fuente_'+box.value).value};item[box.value[0]==='p'?'producto_id':'insumo_id']=Number(box.value.slice(1));items.push(item);});
    Array.from(event.formData.keys()).forEach(function(name){if(name==='articulos'||name.indexOf('unidad_')===0||name.indexOf('fuente_')===0)event.formData.delete(name);});
    event.formData.set('articulos_json',JSON.stringify(items));
  });
  function uuid(){return window.crypto.randomUUID();}
  function summary(){var n=form.querySelectorAll('input[name="articulos"]:checked').length;document.querySelector('[data-selection-status]').textContent=n+' artículos seleccionados. Se conservan al buscar en otro catálogo.';}
  form.addEventListener('input',function(){form.elements.request_id.value=uuid();summary();});
  search.addEventListener('submit',async function(event){
    event.preventDefault();if(busy)return;busy=true;
    var button=event.submitter||search.querySelector('button');button.disabled=true;button.textContent='Buscando…';
    try{
      var url=new URL(window.location.href);url.search=new URLSearchParams(new FormData(search)).toString();
      var controller=new AbortController();var timeout=window.setTimeout(function(){controller.abort();},20000);
      var response;
      try{response=await fetch(url,{credentials:'same-origin',signal:controller.signal});}finally{window.clearTimeout(timeout);}
      if(!response.ok||response.redirected)throw new Error('No fue posible buscar. Revisa tu sesión y vuelve a intentar.');
      var html=new DOMParser().parseFromString(await response.text(),'text/html');
      var fresh=html.querySelector('#preparar-conteo tbody');
      if(!fresh)throw new Error('No se recibió el catálogo. Tus selecciones se conservan.');
      var body=form.querySelector('tbody'),selected=new Map();
      body.querySelectorAll('input[name="articulos"]:checked').forEach(function(box){selected.set(box.value,box.closest('tr'));});
      var fragment=document.createDocumentFragment();
      selected.forEach(function(row){fragment.appendChild(row);});
      fresh.querySelectorAll('tr').forEach(function(row){var box=row.querySelector('input[name="articulos"]');if(box&&!selected.has(box.value))fragment.appendChild(row);});
      body.replaceChildren(fragment);summary();
      if(!body.children.length){var row=document.createElement('tr'),cell=document.createElement('td');cell.colSpan=4;cell.textContent='No hay coincidencias. Cambia la búsqueda.';row.appendChild(cell);body.appendChild(row);}
      window.ERPActionUI.showToast({type:'info',message:'Catálogo actualizado. Los artículos seleccionados permanecen al principio.'});
    }catch(error){window.ERPActionUI.showToast({type:'error',message:error.name==='AbortError'?'La búsqueda tardó demasiado. Tus selecciones se conservan.':error.message,persistent:true});}
    finally{busy=false;button.disabled=false;button.textContent='Buscar artículos';}
  });
  summary();
})();
