/* Recuperación privada de capturas: el servidor sigue siendo la autoridad del intento. */
(function () {
  'use strict';
  const form = document.querySelector('form[data-captura-activos]');
  if (!form) return;
  const feedback = form.querySelector('[data-captura-feedback]');
  const contextNode = document.getElementById('captura-contexto');
  const context = contextNode ? JSON.parse(contextNode.textContent) : {};
  const key = context.actor && context.superficie ? 'activos:captura:v1:' + context.actor + ':' + context.superficie : null;
  const endpoint = new URL(form.getAttribute('action') || window.location.href, window.location.href).pathname;
  let record = null, preparing = false, ready = false, recoveryBlocked = false, storageWarning = '';
  const controls = () => Array.from(form.querySelectorAll('input,select,textarea'));
  const safeName = name => name && name !== 'csrfmiddlewaretoken';
  function message(text) {
    const solicitudes = record && record.status === 'incierto' ? record.entries.filter(entry => entry[0] === 'solicitud_id').map(entry => entry[1]) : [];
    feedback.textContent = text + (solicitudes.length ? ' Solicitudes del intento: ' + solicitudes.join(', ') + '.' : '') + (storageWarning && text !== storageWarning ? ' ' + storageWarning : '');
  }
  function storageFailure() {
    storageWarning = 'Este navegador no permite conservar la captura al recargar. Mantén esta pantalla abierta para recuperar el intento.';
  }
  function save() {
    if (!key || !record) { storageFailure(); return; }
    try { sessionStorage.setItem(key, JSON.stringify(record)); } catch (_) { storageFailure(); }
  }
  function identities() {
    const occurrences = Object.create(null);
    return controls().map(control => {
      const group = control.name + ':' + control.type;
      const occurrence = occurrences[group] || 0; occurrences[group] = occurrence + 1;
      return control.id ? group + ':id:' + control.id : group + ':occurrence:' + occurrence;
    });
  }
  function lock(locked, filesOnly) {
    controls().forEach(function (control) {
      const shouldLock = locked && !(filesOnly && control.type === 'file' && record.files.some(file => file.control === identities()[controls().indexOf(control)]));
      if (shouldLock && !control.disabled) { control.dataset.captureLocked = 'true'; control.disabled = true; }
      else if (!shouldLock && control.dataset.captureLocked) { control.disabled = false; delete control.dataset.captureLocked; }
    });
  }
  function controlState() {
    const ids = identities();
    return controls().map(function (control, index) {
      if (!safeName(control.name) || control.type === 'file') return null;
      return {identity: ids[index], name: control.name, value: control.value, checked: !!control.checked,
        selected: control.multiple ? Array.from(control.options).map(option => option.selected) : null};
    });
  }
  function restoreControls(saved) {
    controls().forEach(function (control, index) {
      const item = saved.find(item => item && item.identity === identities()[index]);
      if (!item || item.name !== control.name || !safeName(control.name) || control.type === 'file') return;
      control.value = item.value;
      if (control.type === 'checkbox' || control.type === 'radio') control.checked = item.checked;
      if (control.multiple && item.selected) Array.from(control.options).forEach((option, i) => option.selected = !!item.selected[i]);
    });
    const details = form.closest('details'); if (details) details.open = true;
  }
  function newRecord(status) {
    return {version: 1, endpoint, actor: String(context.actor), superficie: context.superficie, status,
      controls: controlState(), entries: [], files: [], result: null};
  }
  function safeLink(value) {
    try { const url = new URL(value, window.location.href); return url.origin === window.location.origin && !url.username && !url.password && /^https?:$/.test(url.protocol) ? url.href : null; } catch (_) { return null; }
  }
  function nextCapture(result) {
    lock(false); form.reset(); delete form._captureSnapshot;
    form.querySelectorAll('[data-solicitud-borrador]').forEach(el => el.remove());
    form.elements.clave_captura.value = result && result.siguiente_clave || crypto.randomUUID();
    form.dataset.captureCompleted = 'false'; recoveryBlocked = false;
    record = newRecord('editable');
    form.dispatchEvent(new CustomEvent('activos:nueva-captura'));
    record.controls = controlState(); save();
    message('Nuevo intento. Revisa el equipo y los datos antes de guardar.');
    const submit = form.querySelector('[type="submit"]'); if (submit) { submit.disabled = false; submit.focus(); }
  }
  function showNext(result) {
    const next = document.createElement('button'); next.type = 'button'; next.className = 'btn btn-secondary';
    next.textContent = 'Registrar otro mantenimiento'; next.dataset.nuevaCaptura = 'true';
    next.onclick = () => nextCapture(result);
    feedback.appendChild(document.createTextNode(' ')); feedback.appendChild(next);
  }
  function confirmed(result) {
    form.dataset.captureCompleted = 'true'; lock(true);
    const submit = form.querySelector('[type="submit"]'); if (submit) submit.disabled = true;
    message(result.toast && result.toast.message || 'Mantenimiento registrado.');
    const href = safeLink(result.enlace);
    if (href) { const link = document.createElement('a'); link.href = href; link.textContent = 'Abrir orden y evidencias'; feedback.appendChild(document.createTextNode(' ')); feedback.appendChild(link); }
    showNext(result);
  }
  function draftRestore(draft) {
    const offsets = Object.create(null);
    controls().forEach(function (control) {
      const values = draft[control.name]; if (!values || control.type === 'file' || !safeName(control.name)) return;
      if (control.type === 'checkbox' || control.type === 'radio') control.checked = values.includes(control.value);
      else if (control.multiple) Array.from(control.options).forEach(option => option.selected = values.includes(option.value));
      else { const index = offsets[control.name] || 0; control.value = values[index] || ''; offsets[control.name] = index + 1; }
    });
    const details = form.closest('details'); if (details) details.open = true;
  }
  const draftNode = document.getElementById('captura-borrador');
  if (draftNode) { try { const draft = JSON.parse(draftNode.textContent); if (draft) draftRestore(draft); } catch (_) { message('No pudimos recuperar los datos de la captura.'); } }
  if (key) {
    let saved = null;
    let readUnknown = false;
    try { saved = sessionStorage.getItem(key); } catch (_) { readUnknown = true; }
    try {
      if (readUnknown) throw Error('unknown storage');
      if (saved) {
        const value = JSON.parse(saved);
        const topFields = ['version', 'endpoint', 'actor', 'superficie', 'status', 'controls', 'entries', 'files', 'result'];
        if (!value || typeof value !== 'object' || Array.isArray(value) || topFields.some(name => !Object.prototype.hasOwnProperty.call(value, name)) || Object.keys(value).some(name => !topFields.includes(name)) ||
            (value.result !== null && (typeof value.result !== 'object' || Array.isArray(value.result))) ||
            (value.status !== 'confirmado' && value.result !== null)) throw Error('invalid');
        const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
        const allowedNames = new Set(controls().map(control => control.name).filter(safeName));
        allowedNames.add('solicitud_id'); // Una falla resuelta puede desaparecer del selector tras el commit.
        const entriesValid = Array.isArray(value.entries) && value.entries.every(entry => Array.isArray(entry) && entry.length === 2 && entry.every(part => typeof part === 'string') && safeName(entry[0]) && allowedNames.has(entry[0]));
        const clave = entriesValid && value.entries.filter(entry => entry[0] === 'clave_captura');
        const controlsValid = Array.isArray(value.controls) && value.controls.every(item => item === null ||
          item && typeof item.identity === 'string' && typeof item.name === 'string' && safeName(item.name) && allowedNames.has(item.name) && typeof item.value === 'string' && typeof item.checked === 'boolean' &&
          (item.selected === null || Array.isArray(item.selected) && item.selected.every(selected => typeof selected === 'boolean')));
        const controlClave = controlsValid && value.controls.find(item => item && item.name === 'clave_captura');
        const filesValid = Array.isArray(value.files) && value.files.every(file => file && typeof file.control === 'string' && typeof file.field === 'string' && safeName(file.field) && controls().some((control, index) => control.type === 'file' && control.name === file.field && identities()[index] === file.control) &&
          Number.isInteger(file.position) && file.position >= 0 && typeof file.name === 'string' && Number.isInteger(file.size) && file.size >= 0 && typeof file.type === 'string' && /^[a-f0-9]{64}$/.test(file.sha256));
        const action = controls().find(control => control.name === 'action');
        const actions = entriesValid && value.entries.filter(entry => entry[0] === 'action');
        const controlActions = controlsValid && value.controls.filter(item => item && item.name === 'action');
        const frozen = value.status !== 'editable';
        const actionValid = context.superficie === 'ordenes' ? action && action.value === 'create_orden' && controlActions.length === 1 && controlActions[0].value === action.value && (!frozen || actions.length === 1 && actions[0][1] === action.value) : !action && !actions.length && !controlActions.length;
        const countsValid = entriesValid && Array.from(allowedNames).every(name => name === 'solicitud_id' || value.entries.filter(entry => entry[0] === name).length <= controls().filter(control => control.name === name).reduce((count, control) => count + (control.multiple ? control.options.length : 1), 0));
        const completeEntries = !frozen || controlsValid && entriesValid && controls().every(control => !safeName(control.name) || ['file', 'checkbox', 'radio'].includes(control.type) || control.disabled || value.entries.some(entry => entry[0] === control.name));
        if (value.version !== 1 || value.endpoint !== endpoint || !actionValid || !completeEntries || !countsValid || value.actor !== String(context.actor) || value.superficie !== context.superficie ||
            !['editable', 'incierto', 'confirmado'].includes(value.status) || !controlsValid || !entriesValid || !filesValid ||
            !controlClave || !uuid.test(controlClave.value) || (value.status !== 'editable' && (clave.length !== 1 || clave[0][1] !== controlClave.value)) ||
            (value.status === 'confirmado' && (!value.result || !safeLink(value.result.enlace) || !uuid.test(value.result.siguiente_clave)))) throw Error('invalid');
        record = value; restoreControls(record.controls);
        if (record.status === 'confirmado') confirmed(record.result || {});
        else if (record.status === 'incierto') {
          lock(true, true);
          message(record.files.length ? 'Recuperamos el intento. Vuelve a seleccionar exactamente los mismos adjuntos antes de reintentar.' : 'Recuperamos el intento sin confirmación. Reintenta Guardar para recuperar la misma orden.');
        } else message('Recuperamos tu captura pendiente. Revisa los datos antes de guardar.');
      }
    } catch (_) {
      // Una entrada ilegible puede ser un envío sin confirmar: nunca sustituir su clave automáticamente.
      recoveryBlocked = true;
      const details = form.closest('details'); if (details) details.open = true;
      message('No pudimos leer la captura conservada. No enviaremos otro intento automáticamente. Revisa la orden antes de elegir Registrar otro mantenimiento.');
      const review = document.createElement('a'); review.href = '/activos/ordenes/'; review.textContent = 'Revisar órdenes';
      feedback.appendChild(document.createTextNode(' ')); feedback.appendChild(review);
      showNext(null);
    }
  } else storageFailure();
  function saveEditable() {
    if (recoveryBlocked || preparing || record && record.status !== 'editable') return;
    record = newRecord('editable'); save();
    if (storageWarning) message(storageWarning);
  }
  form.addEventListener('input', saveEditable);
  form.addEventListener('change', saveEditable);
  async function attachmentList() {
    const files = [];
    for (const [controlIndex, control] of controls().entries()) {
      if (control.type !== 'file' || (control.disabled && !control.dataset.captureLocked)) continue;
      for (const [position, file] of Array.from(control.files || []).entries()) {
        const hash = await crypto.subtle.digest('SHA-256', await file.arrayBuffer());
        files.push({control: identities()[controlIndex], field: control.name, position, name: file.name, size: file.size,
          type: file.type, lastModified: file.lastModified, sha256: Array.from(new Uint8Array(hash)).map(byte => byte.toString(16).padStart(2, '0')).join('')});
      }
    }
    return files;
  }
  function currentSnapshot(entries) {
    const data = new FormData();
    entries.forEach(([name, value]) => data.append(name, value));
    // Sólo el token de esta página. Nunca se guarda el CSRF en sessionStorage.
    controls().filter(control => control.name === 'csrfmiddlewaretoken').forEach(control => data.append(control.name, control.value));
    controls().filter(control => control.type === 'file').forEach(control => {
      Array.from(control.files || []).forEach((file, position) => {
        if (record.files.some(expected => expected.control === identities()[controls().indexOf(control)] && expected.position === position)) data.append(control.name, file);
      });
    });
    return data;
  }
  form.addEventListener('submit', async function (event) {
    if (ready) { ready = false; return; }
    event.preventDefault(); event.stopImmediatePropagation();
    if (preparing || recoveryBlocked || form.dataset.captureCompleted === 'true' || form.dataset.actionPending === 'true') return;
    if (!form.reportValidity()) return;
    const submitter = event.submitter;
    preparing = true;
    const uncertain = record && record.status === 'incierto';
    // Leer los controles antes de congelarlos; los disabled se excluyen de FormData.
    const data = uncertain ? null : new FormData(form);
    const savedControls = uncertain ? record.controls : controlState();
    lock(true);
    message('Verificando los adjuntos antes de guardar…');
    try {
      const files = await attachmentList();
      if (uncertain && (files.length !== record.files.length || files.some((file, i) =>
          ['control', 'field', 'position', 'name', 'size', 'type', 'sha256'].some(name => file[name] !== record.files[i][name])))) {
        lock(true, true); message('Faltan adjuntos o no son los mismos del intento original. Selecciona los archivos originales para reintentar.'); return;
      }
      if (!uncertain) {
        record = newRecord('editable'); record.controls = savedControls; record.files = files;
        record.entries = Array.from(data.entries()).filter(([name, value]) => safeName(name) && typeof value === 'string');
      }
      form._captureSnapshot = currentSnapshot(record.entries);
      // El motor compartido ejecuta fetch inmediatamente después de action-start.
      // requestSubmit dentro del mismo despacho puede ser ignorado por el navegador.
      // Continuar en otra tarea conserva el guard de doble clic durante el digest.
      await new Promise(resolve => window.setTimeout(resolve, 0));
      if (!document.contains(form)) { save(); return; }
      ready = true; preparing = false;
      form.requestSubmit(submitter);
    } catch (_) {
      lock(!!uncertain, !!uncertain); message('No pudimos verificar los adjuntos. Mantén esta pantalla abierta y vuelve a intentar.');
    } finally { preparing = false; }
  }, true);
  form.addEventListener('erp:action-start', function (event) {
    if (!record) record = newRecord('editable');
    // Guardado síncrono antes del fetch del motor compartido.
    record.status = 'incierto';
    record.entries = Array.from(event.detail.formData.entries()).filter(([name, value]) => safeName(name) && typeof value === 'string');
    save(); lock(true); message('Guardando el intento. Si se pierde la respuesta, reintenta para recuperar la misma orden.');
  });
  form.addEventListener('erp:action-error', function (event) {
    if ([400, 403, 404].includes(event.detail.statusCode)) { lock(false); delete form._captureSnapshot; if (record) { record.status = 'editable'; record.controls = controlState(); save(); } }
    message(event.detail.message);
  });
  form.addEventListener('erp:action-success', function (event) {
    record.status = 'confirmado';
    const data = event.detail;
    record.result = {enlace: data.enlace, siguiente_clave: data.siguiente_clave, toast: {message: data.toast && data.toast.message}};
    save(); confirmed(record.result);
  });
}());
