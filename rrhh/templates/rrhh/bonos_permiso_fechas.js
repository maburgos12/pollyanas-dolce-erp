function fechaPermiso(valor){
  const partes=new Intl.DateTimeFormat('en-CA',{
    timeZone:'America/Mazatlan',year:'numeric',month:'2-digit',day:'2-digit'
  }).formatToParts(new Date(valor));
  const fecha=Object.fromEntries(partes.map(({type,value})=>[type,value]));
  return `${fecha.year}-${fecha.month}-${fecha.day}`;
}
function horaPermiso(valor){
  return new Date(valor).toLocaleTimeString('es-MX',{
    timeZone:'America/Mazatlan',hour:'2-digit',minute:'2-digit',hourCycle:'h23'
  });
}
function permisoDate(p){
  const formato=new Intl.DateTimeFormat('es-MX',{
    timeZone:'America/Mazatlan',day:'2-digit',month:'long',year:'numeric'
  });
  const inicio=formato.format(new Date(p.fecha_inicio));
  const fin=p.fecha_fin&&formato.format(new Date(p.fecha_fin));
  return p.tipo==='permiso_dia'&&fin&&fin!==inicio?`${inicio} a ${fin}`:inicio;
}
function permisoTime(p){
  if(p.tipo==='permiso_dia')return 'Día completo';
  return `${horaPermiso(p.fecha_inicio)} - ${p.fecha_fin?horaPermiso(p.fecha_fin):''}`;
}
function validarRangoPermiso({tipo,fecha,modalidad,fechaFin}){
  if(!fecha)return 'Selecciona la fecha del permiso.';
  if(tipo==='permiso_dia'&&modalidad==='rango'){
    if(!fechaFin)return 'Selecciona el último día del rango.';
    if(fechaFin<fecha)return 'La fecha final no puede ser anterior al inicio.';
  }
  return '';
}
function fechasPermiso({tipo,fecha,desde,hasta,modalidad,fechaFin}){
  if(tipo==='permiso_dia'){
    return {
      modalidad_dias:modalidad,
      fecha_inicio:`${fecha}T00:00:00`,
      fecha_fin:modalidad==='rango'?`${fechaFin}T00:00:00`:null
    };
  }
  return {fecha_inicio:`${fecha}T${desde}:00`,fecha_fin:`${fecha}T${hasta}:00`};
}
function DiasPermiso({fecha,fechaFin,modalidad,actualizar}){
  const rango=modalidad==='rango';
  const dias=rango&&fecha&&fechaFin>=fecha?(Date.parse(fechaFin)-Date.parse(fecha))/86400000+1:0;
  return h('div',{className:'form-grid'},
    h('label',{className:'field'},'Duración',
      h('select',{className:'form-control',value:modalidad,onChange:e=>actualizar({modalidad:e.target.value,fechaFin:''})},
        h('option',{value:'dia'},'Un día'),
        h('option',{value:'rango'},'Rango de días')
      )
    ),
    h('label',{className:'field'},rango?'Desde':'Fecha',
      h('input',{className:'form-control',type:'date',value:fecha,onChange:e=>actualizar({fecha:e.target.value})})
    ),
    rango&&h('label',{className:'field'},'Hasta',
      h('input',{className:'form-control',type:'date',value:fechaFin,min:fecha,onChange:e=>actualizar({fechaFin:e.target.value})})
    ),
    dias>0&&h('output',null,`${dias} ${dias===1?'día calendario':'días calendario'}`)
  );
}
