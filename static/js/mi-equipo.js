(() => {
  'use strict';
  const root = document.getElementById('mi-equipo');
  if (!root) return;
  const form = document.getElementById('equipo-form');
  const editor = document.getElementById('equipo-editor');
  const records = document.getElementById('equipo-registros');
  const status = document.getElementById('equipo-status');
  const nuevo = document.getElementById('equipo-nuevo');
  let recurso = 'permisos';
  let editing = null;
  let employees = [];
  let busy = false;
  const esPermiso = () => recurso === 'permisos';
  const endpoint = () => `/rrhh/api/mi-equipo/${recurso}/`;
  const announce = (message, error = false) => {
    status.textContent = message;
    status.dataset.error = String(error);
  };
  const lock = (value) => {
    busy = value;
    root.querySelectorAll('button').forEach(button => { button.disabled = value; });
    nuevo.disabled = value || !employees.length;
  };
  async function api(url, data) {
    const response = await fetch(url, {
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json', 'X-CSRFToken': form.elements.csrfmiddlewaretoken.value },
      ...(data ? { method: 'POST', body: JSON.stringify(data) } : {})
    });
    if (response.redirected) throw new Error('Tu sesión terminó. Vuelve a iniciar sesión y recarga esta pantalla.');
    const body = await response.json();
    if (!response.ok) throw new Error(typeof body === 'object' ? Object.values(body).flat().join(' · ') : String(body));
    return body;
  }
  const text = (parent, tag, value) => {
    const element = document.createElement(tag);
    element.textContent = value;
    parent.append(element);
    return element;
  };
  const dateText = value => value ? new Date(value).toLocaleString('es-MX', { timeZone: 'America/Mazatlan' }) : 'Sin fecha final';
  const inputDate = value => {
    if (!value) return '';
    const parts = new Intl.DateTimeFormat('sv-SE', { timeZone: 'America/Mazatlan', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' }).formatToParts(new Date(value));
    const p = Object.fromEntries(parts.map(part => [part.type, part.value]));
    return `${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}`;
  };
  function closeEditor() { editor.hidden = true; editing = null; }
  function openEditor(item = null) {
    editing = item;
    form.reset();
    editor.hidden = false;
    const permiso = document.getElementById('equipo-permiso');
    const extra = document.getElementById('equipo-extra');
    permiso.hidden = permiso.disabled = !esPermiso();
    extra.hidden = extra.disabled = esPermiso();
    form.elements.empleado.replaceChildren(...employees.map(employee => new Option(employee.empleado_nombre, employee.id)));
    form.elements.empleado.disabled = Boolean(item);
    document.getElementById('equipo-correccion').hidden = !item;
    form.elements.motivo_cambio.disabled = !item;
    form.elements.motivo_cambio.required = Boolean(item);
    document.getElementById('equipo-form-title').textContent = item ? `Corregir ${item.folio}` : esPermiso() ? 'Crear permiso' : 'Registrar horas extra';
    if (item) {
      for (const name of ['empleado', 'tipo', 'motivo', 'goce_sueldo', 'fecha', 'horas', 'notas']) {
        if (item[name] !== undefined) form.elements[name].value = String(item[name]);
      }
      form.elements.fecha_inicio.value = inputDate(item.fecha_inicio);
      form.elements.fecha_fin.value = inputDate(item.fecha_fin);
    }
    document.getElementById('equipo-form-title').focus();
  }
  async function resolve(item, action) {
    if (busy || !window.confirm(`${action === 'rechazar' ? 'Rechazar' : 'Autorizar'} ${item.folio} de ${item.empleado_nombre}?`)) return;
    lock(true);
    try {
      await api(`${endpoint()}${item.id}/${action}/`, {});
      closeEditor();
      await load('Resolución guardada.');
    } catch (error) { announce(error.message, true); }
    finally { lock(false); }
  }
  async function load(message = '') {
    lock(true);
    records.setAttribute('aria-busy', 'true');
    announce('Cargando registros…');
    try {
      const data = await api(endpoint());
      employees = data.empleados;
      const items = data[esPermiso() ? 'permisos' : 'horas_extra'];
      records.replaceChildren();
      if (!items.length) text(records, 'p', 'Todavía no hay registros. Usa el botón de captura para registrar la primera solicitud.');
      for (const item of items) {
        const row = document.createElement('article');
        row.className = 'equipo-registro';
        text(row, 'h3', `${item.empleado_nombre} · ${item.folio}`);
        text(row, 'p', `${item.estado_label || item.estado}${esPermiso() ? ` · ${item.goce_label}` : ''}`);
        text(row, 'p', esPermiso() ? `${item.tipo_label} · ${dateText(item.fecha_inicio)} — ${dateText(item.fecha_fin)}` : `${item.fecha} · ${item.horas} horas`);
        text(row, 'p', esPermiso() ? item.motivo : item.notas);
        if (item.revision_extra) text(row, 'p', item.revision_extra);
        const actions = document.createElement('div');
        actions.className = 'equipo-actions';
        const button = (label, handler) => { const b = text(actions, 'button', label); b.type = 'button'; b.className = 'btn'; b.addEventListener('click', handler); };
        if (item.puede_editar) button('Corregir', () => openEditor(item));
        if (item.puede_preautorizar || item.puede_autorizar) button('Autorizar', () => resolve(item, esPermiso() ? 'preautorizar' : 'autorizar'));
        if (item.puede_preautorizar || item.puede_rechazar) button('Rechazar', () => resolve(item, 'rechazar'));
        row.append(actions);
        records.append(row);
      }
      announce(`${message} ${items.length} registros · ${employees.length} colaboradores.`.trim());
    } catch (error) {
      records.replaceChildren();
      employees = [];
      announce(`${error.message} Puedes volver a intentar con Actualizar.`, true);
    } finally { records.setAttribute('aria-busy', 'false'); lock(false); }
  }
  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (busy || !form.reportValidity()) return;
    const data = Object.fromEntries(new FormData(form));
    delete data.csrfmiddlewaretoken;
    if (esPermiso()) {
      // Las capturas se interpretan en la zona operativa, independientemente del dispositivo.
      data.fecha_inicio += ':00-07:00';
      data.fecha_fin = data.fecha_fin ? `${data.fecha_fin}:00-07:00` : null;
    }
    lock(true);
    try {
      await api(editing ? `${endpoint()}${editing.id}/editar/` : endpoint(), data);
      closeEditor();
      await load('Solicitud guardada.');
    } catch (error) { announce(error.message, true); }
    finally { lock(false); }
  });
  nuevo.addEventListener('click', () => openEditor());
  document.getElementById('equipo-cancelar').addEventListener('click', closeEditor);
  document.getElementById('equipo-recargar').addEventListener('click', () => load());
  root.querySelectorAll('[data-recurso]').forEach(button => button.addEventListener('click', () => {
    if (busy) return;
    recurso = button.dataset.recurso;
    closeEditor();
    root.querySelectorAll('[data-recurso]').forEach(tab => { tab.classList.toggle('active', tab === button); tab.setAttribute('aria-pressed', String(tab === button)); });
    document.getElementById('equipo-list-title').textContent = esPermiso() ? 'Permisos de mi equipo' : 'Horas extra de mi equipo';
    nuevo.textContent = esPermiso() ? 'Crear permiso' : 'Registrar horas extra';
    load();
  }));
  load();
})();
