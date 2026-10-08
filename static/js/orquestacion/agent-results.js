/* Present only server-projected DTOs. Text supplied by the model is never an action. */
(() => {
  const escape = (value) => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#039;'}[c]));
  const statuses = {
    WAITING_INFORMATION:'Falta información', WAITING_SELECTION:'Elegir equipo', READY:'Lista para consultar',
    RUNNING:'En curso', REVALIDATION_REQUIRED:'Requiere revisión', COMPLETED:'Consulta completada',
    CANCELLED:'Cancelada', EXPIRED:'Vencida', complete:'Completada', error:'No completada',
    running:'En curso', pending:'Pendiente', approval_requested:'Requiere autorización',
  };
  const fields = {query:'Consulta', asset:'Equipo', query_or_asset:'Nombre o código del equipo', asset_selection:'Elegir uno de los equipos encontrados'};
  function date(value) {
    if (!value) return 'Sin fecha';
    const parsed = new Date(value.length === 10 ? `${value}T12:00:00` : value);
    return Number.isNaN(parsed.getTime()) ? escape(value) : escape(parsed.toLocaleString('es-MX', {dateStyle:'medium', ...(value.length === 10 ? {} : {timeStyle:'short'})}));
  }
  function badge(status) { return `<span class="agent-state">${escape(statuses[status] || 'Estado no reconocido')}</span>`; }
  function source(result) {
    return `<p class="agent-source">Fuente: ${escape((result.sources || []).join(', ') || 'No indicada')} · Consultado: ${date(result.as_of)}</p>`;
  }
  function asset(row) {
    if (!row || typeof row !== 'object') return '';
    return `<div class="agent-asset"><strong>${escape(row.nombre || 'Equipo sin nombre')}</strong><span>${escape(row.codigo)} · ${escape(row.sucursal || 'Sin sucursal indicada')}</span>${row.estado ? `<span>${escape(row.estado)}</span>` : ''}</div>`;
  }
  function workflow(row, interactive = false) {
    if (!row || typeof row.public_id !== 'string') return '';
    const actionable = ['WAITING_INFORMATION','WAITING_SELECTION','READY'].includes(row.status);
    const options = (row.options || []).map(option => `<li>${escape(option.position)}. ${option.available ? `${escape(option.asset?.nombre)} · ${escape(option.asset?.codigo)} · ${escape(option.asset?.sucursal || 'Sin sucursal indicada')}` : 'Opción ya no disponible'}</li>`).join('');
    return `<article class="agent-workflow">
      <div class="agent-result-head"><strong>${escape(row.asset?.nombre || row.query || 'Consulta de mantenimiento')}</strong>${badge(row.status)}</div>
      ${row.asset ? asset(row.asset) : ''}
      ${row.missing_fields?.length ? `<p>Falta: ${row.missing_fields.map(key => escape(fields[key] || 'Información adicional')).join(', ')}.</p>` : ''}
      ${options && row.status === 'WAITING_SELECTION' ? `<ol class="agent-options">${options}</ol>` : ''}
      ${source(row)}
      <p class="agent-source">Proceso actualizado: ${date(row.updated_at)} · Versión ${escape(row.version)}</p>
      <details class="agent-reference"><summary>Referencia del proceso</summary><code>${escape(row.public_id)}</code></details>
      ${interactive && actionable ? `<button type="button" class="agent-continue" data-workflow-id="${escape(row.public_id)}">Continuar en la conversación</button>` : ''}
      ${row.status === 'REVALIDATION_REQUIRED' ? '<p>La consulta requiere revisión antes de continuar.</p>' : ''}
    </article>`;
  }
  function table(title, rows, columns) {
    if (!Array.isArray(rows) || !rows.length) return `<p class="agent-no-data">${escape(title)}: sin registros en esta consulta.</p>`;
    return `<div class="agent-table-scroll" tabindex="0" role="region" aria-label="${escape(title)}"><table><caption>${escape(title)}</caption><thead><tr>${columns.map(([label]) => `<th scope="col">${escape(label)}</th>`).join('')}</tr></thead><tbody>${rows.map(row => `<tr>${columns.map(([,render]) => `<td>${render(row)}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
  }
  function data(key, result) {
    const p = result.payload || {};
    if (result.status === 'no_data' && key !== 'erp.get_pending_maintenance') return '<p>No se encontraron registros con los filtros de esta consulta.</p>';
    if (key === 'erp.search_assets') return `${result.status === 'ambiguous' ? '<p>Hay varias coincidencias. Indica cuál equipo necesitas.</p>' : ''}${(p.items || []).map(asset).join('')}${p.truncated ? '<p>Hay más coincidencias; precisa la búsqueda.</p>' : ''}`;
    if (key === 'erp.get_asset_context') {
      const i = p.identidad || {};
      return `${asset(p.activo)}<dl class="agent-facts">${[['Ubicación',i.ubicacion],['Marca',i.marca],['Modelo',i.modelo],['Serie',i.numero_serie]].filter(([,v]) => v).map(([k,v]) => `<div><dt>${k}</dt><dd>${escape(v)}</dd></div>`).join('')}</dl>
        ${p.proximo_plan ? `<p><strong>Próximo plan:</strong> ${escape(p.proximo_plan.nombre)} · ${date(p.proximo_plan.proxima_ejecucion)}</p>` : '<p>Sin próximo plan registrado. Esto no demuestra ausencia de servicio.</p>'}
        ${table('Fallas abiertas',p.fallas_abiertas,[['Falla',r=>escape(r.titulo)],['Estado',r=>escape(r.estatus_label || r.estatus)],['Prioridad',r=>escape(r.prioridad_label || r.prioridad)]])}
        ${table('Órdenes recientes',p.ordenes_recientes,[['Folio',r=>escape(r.folio)],['Estado',r=>escape(r.estatus_label || r.estatus)],['Fecha',r=>date(r.fecha)]])}
        ${p.puede_ver_costos === true && p.costos ? `<dl class="agent-facts">${[['Adquisición','adquisicion'],['Mantenimiento acumulado','mantenimiento_total'],['Reposición','valor_reposicion']].filter(([,k]) => p.costos[k] != null).map(([label,k]) => `<div><dt>${label} (MXN)</dt><dd>${escape(p.costos[k])}</dd></div>`).join('')}</dl>` : ''}
        <p class="agent-source">Historial parcial: hasta ${escape(p.history?.event_limit ?? 10)} eventos por grupo.${p.history?.orders_truncated || p.history?.failures_truncated ? ' Hay más eventos fuera de esta consulta.' : ''}</p>`;
    }
    if (key === 'erp.get_pending_maintenance') return `${[['Vencidos','overdue'],['Próximos','upcoming'],['Sin fecha','missing_schedule'],['Inactivos o pausados','inactive_paused']].map(([label,bucket]) => `${table(label,p[bucket],[['Equipo',r=>escape(r.activo?.nombre)],['Plan',r=>escape(r.nombre)],['Fecha',r=>date(r.proxima_ejecucion)]])}${p.truncated?.[bucket] ? `<p>Más planes en ${escape(label.toLowerCase())}; consulta acotada.</p>` : ''}`).join('')}<p class="agent-source">Hasta ${date(p.fecha_hasta)}. La ausencia de un plan no demuestra ausencia de servicio.</p>`;
    if (key === 'workflow.list_pending') return (p.items || []).length ? p.items.map(row => workflow(row)).join('') : '<p>No hay consultas pendientes en este resultado.</p>';
    if (['workflow.prepare_asset_maintenance','workflow.resume_asset_maintenance'].includes(key)) return `${workflow(p.workflow)}${p.consultation ? data('erp.get_asset_context',{payload:p.consultation, status:'ok'}) : ''}`;
    return '';
  }
  function tool(call) {
    const wrapper = call.result || call.payload || {};
    const result = wrapper.result || {};
    const key = call.tool_key || ({erp_search_assets:'erp.search_assets',erp_get_asset_context:'erp.get_asset_context',erp_get_pending_maintenance:'erp.get_pending_maintenance',erp_list_pending_workflows:'workflow.list_pending',erp_prepare_asset_maintenance:'workflow.prepare_asset_maintenance',erp_resume_asset_maintenance:'workflow.resume_asset_maintenance'}[call.tool_name]);
    const error = call.status === 'error' || Boolean(wrapper.error);
    const approval = call.status === 'approval_requested' || call.requires_approval === true;
    const complete = call.status === 'complete' && !error && !approval;
    const html = complete ? data(key,result) : '';
    return `<section class="agent-tool${error ? ' is-error' : ''}"><div class="agent-result-head"><strong>${escape(call.tool_display_name || 'Consulta del ERP')}</strong>${badge(error ? 'error' : approval ? 'approval_requested' : call.status)}</div>
      ${error ? '<p>No se pudo completar esta consulta. Revisa tu acceso o intenta consultar nuevamente.</p>' : approval ? '<p>La acción requiere autorización; no se presenta como ejecutada.</p>' : html || `<p>${escape(call.summary || (complete ? 'El servidor registró el resultado.' : 'Esperando resultado del servidor.'))}</p>`}
      ${html ? source(result) : ''}</section>`;
  }
  globalThis.ERPAgentView = {escape, date, badge, workflow, tool};
})();
