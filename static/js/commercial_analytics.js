(() => {
  const el = document.getElementById('commercial-analytics-data');
  if (!el) return;
  const data = JSON.parse(el.textContent);
  if (!data || !Array.isArray(data.monthly)) return;
  document.querySelectorAll('.ca-legacy').forEach(details=>details.addEventListener('toggle',()=>{
    if (details.open && window.Chart) Object.values(Chart.instances).forEach(chart=>chart.resize());
  }));
  const money = v => new Intl.NumberFormat('es-MX', {style:'currency', currency:'MXN', maximumFractionDigits:0}).format(v);
  const number = v => v === null || v === undefined ? null : Number(v);
  const wine = '#8B2252', gold = '#C9A84C';
  function chart(id, labels, datasets, horizontal=false) {
    const canvas = document.getElementById(id);
    if (!canvas) return;
    if (!labels.length || !window.Chart) {
      const fallback = document.createElement('p');
      fallback.className = 'ca-chart-unavailable';
      fallback.textContent = !labels.length ? 'Sin datos en ambos periodos para descomponer el cambio.' : 'Gráfico no disponible. Consulta los mismos importes en las tablas.';
      canvas.replaceWith(fallback); return;
    }
    new Chart(canvas, {type:'bar', data:{labels, datasets}, options:{responsive:true, maintainAspectRatio:false,
      animation:window.matchMedia('(prefers-reduced-motion: reduce)').matches ? false : {duration:250},
      indexAxis:horizontal?'y':'x', plugins:{legend:{position:'bottom'}, tooltip:{callbacks:{label:ctx=>`${ctx.dataset.label}: ${money(ctx.raw)}`}}},
      scales:horizontal?{x:{ticks:{callback:v=>money(v)}},y:{ticks:{autoSkip:false}}}:{y:{ticks:{callback:v=>money(v)}}}
    }});
  }
  const pair = rows => [
    {label:String(data.previous_year),data:rows.map(r=>number(r.previous)),backgroundColor:gold},
    {label:String(data.year),data:rows.map(r=>number(r.current)),backgroundColor:wine}
  ].filter(dataset=>dataset.data.some(value=>value!==null));
  chart('ca-monthly', data.monthly.map(r=>r.label), pair(data.monthly));
  chart('ca-branches', data.branches.map(r=>r.name), [{label:data.delta!==null?'Diferencia con IVA':'Venta con IVA',data:data.branches.map(r=>number(data.delta!==null?r.delta:(data.current!==null?r.current:r.previous))),backgroundColor:data.branches.map(r=>data.delta!==null&&Number(r.delta)<0?'#a82c3c':'#267244')}], true);
  chart('ca-category', data.categories.slice(0,10).map(r=>r.name), pair(data.categories.slice(0,10)), true);
  chart('ca-products-chart', data.products.slice(0,10).map(r=>r.name), pair(data.products.slice(0,10)), true);
  chart('ca-components', data.components.map(r=>r.label), [{label:'Contribución al cambio',data:data.components.map(r=>number(r.amount)),backgroundColor:data.components.map(r=>Number(r.amount)<0?'#a82c3c':'#267244')}], true);
})();
