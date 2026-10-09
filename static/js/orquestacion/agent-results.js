/* Present only server-projected DTOs. Text supplied by the model is never an action. */
(() => {
  const escape = (value) => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#039;'}[c]));
  const statuses = {
    WAITING_INFORMATION:'Falta información', WAITING_SELECTION:'Elegir equipo', READY:'Lista para consultar',
    RUNNING:'En curso', REVALIDATION_REQUIRED:'Requiere revisión', COMPLETED:'Consulta completada',
    CANCELLED:'Cancelada', EXPIRED:'Vencida', complete:'Completada', error:'No completada',
    running:'En curso', pending:'Pendiente', approval_requested:'Requiere autorización',
    out_of_scope:'Fuera del alcance', AWAITING_CONFIRMATION:'Pendiente de confirmación', EXECUTED:'Acción ejecutada', REVIEW_REQUIRED:'Revisar falla existente',
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
    if (key === 'incident.prepare') return incident(p.incident);
    if (key === 'incident.requirements') return `<p>Se necesita equipo, categoría, descripción y evidencia o justificación.</p><ul>${(p.categories || []).map(row => `<li>${escape(row.nombre)}</li>`).join('')}</ul>`;
    if (key === 'read.explain_limit') return `<p>${escape(p.message)}</p>`;
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
  function incident(row) {
    if (!row || typeof row.draft_id !== 'string') return '';
    if (row.status === 'EXECUTED') return `${asset(row.asset)}<p>Reporte creado · Folio de falla: <strong>#${escape(row.report_id)}</strong>.</p>${row.confirmed_at ? `<p class="agent-source">Confirmado: ${date(row.confirmed_at)}</p>` : ''}`;
    const fields = {activo_id:'Equipo', categoria_id:'Categoría', titulo:'Título', descripcion:'Qué ocurrió', prioridad:'Prioridad', justificacion_sin_foto:'Motivo de no adjuntar foto'};
    const missing = (row.missing_fields || []).map(key => escape(fields[key] || key)).join(', ');
    const labels = {WAITING_INFORMATION:'Falta información', AWAITING_CONFIRMATION:'Pendiente de tu confirmación', EXPIRED:'Propuesta vencida', REVIEW_REQUIRED:'Revisar falla existente'};
    return `${asset(row.asset)}<p><strong>${escape(labels[row.status] || 'Revisar propuesta')}</strong></p>
      <dl class="agent-facts">${Object.entries(row.fields || {}).filter(([key]) => !['activo_id','categoria_id'].includes(key)).map(([key,value]) => `<div><dt>${escape(fields[key] || key)}</dt><dd>${escape(value)}</dd></div>`).join('')}${row.categoria ? `<div><dt>Categoría</dt><dd>${escape(row.categoria)}</dd></div>` : ''}</dl>
      ${row.status === 'REVIEW_REQUIRED' ? `<p>Este equipo ya tiene fallas abiertas. Revisa su seguimiento antes de crear otra.</p><ul>${(row.existing_reports || []).map(report => `<li>#${escape(report.id)} · ${escape(report.titulo)}</li>`).join('')}</ul>` : ''}
      ${missing ? `<p>Falta: ${missing}. Puedes completar el reporte en esta conversación.</p>` : ''}
      ${row.status === 'AWAITING_CONFIRMATION' ? `<p>Al confirmar se creará el reporte en Fallas y quedará disponible para seguimiento en Mantenimiento.</p><button type="button" class="agent-continue" data-incident-id="${escape(row.draft_id)}" data-version="${escape(row.version)}" data-hash="${escape(row.payload_hash)}">Confirmar y crear reporte</button>` : ''}`;
  }
  function tool(call) {
    const wrapper = call.result || call.payload || {};
    const result = wrapper.result || {};
    const key = call.tool_key || ({erp_explain_read_limit:'read.explain_limit',erp_search_assets:'erp.search_assets',erp_get_asset_context:'erp.get_asset_context',erp_get_pending_maintenance:'erp.get_pending_maintenance',erp_list_pending_workflows:'workflow.list_pending',erp_prepare_asset_maintenance:'workflow.prepare_asset_maintenance',erp_resume_asset_maintenance:'workflow.resume_asset_maintenance'}[call.tool_name]);
    const error = call.status === 'error' || Boolean(wrapper.error);
    const approval = call.status === 'approval_requested' || call.requires_approval === true;
    const complete = call.status === 'complete' && !error && !approval;
    const html = complete ? data(key,result) : '';
    const incidentStatus = key === 'incident.prepare' ? result.payload?.incident?.status : null;
    return `<section class="agent-tool${error ? ' is-error' : ''}"><div class="agent-result-head"><strong>${escape(call.tool_display_name || 'Consulta del ERP')}</strong>${badge(error ? 'error' : incidentStatus || (approval ? 'approval_requested' : complete && result.status === 'out_of_scope' ? 'out_of_scope' : call.status))}</div>
      ${error ? '<p>No se pudo completar esta consulta. Revisa tu acceso o intenta consultar nuevamente.</p>' : approval ? '<p>La acción requiere autorización; no se presenta como ejecutada.</p>' : html || `<p>${escape(call.summary || (complete ? 'El servidor registró el resultado.' : 'Esperando resultado del servidor.'))}</p>`}
      ${html ? source(result) : ''}</section>`;
  }
  function response(message) {
    const content = escape(message.content || (message.status === 'streaming' ? 'Consultando el ERP…' : ''));
    const receipts = (message.receipts || []).map(row => `<section class="agent-tool"><div class="agent-result-head"><strong>Comprobante del ERP</strong>${badge(row.status)}</div>${incident(row)}</section>`).join('');
    const tools = (message.tool_calls || []).map(tool).join('');
    const prose = message.presentation === 'natural' || message.role === 'user' || (!tools && !receipts)
      ? `<p class="agent-answer">${content}</p>`
      : `<details class="agent-full-response"><summary>Detalle técnico de la consulta</summary><p>${content}</p></details>`;
    return `${prose}${receipts}${tools}`;
  }
  globalThis.ERPAgentView = {escape, date, badge, workflow, tool, incident, response};
})();
