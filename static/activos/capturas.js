/* Una confirmación incierta conserva el FormData y su UUID hasta recuperar la orden. */
(function () {
  'use strict';
  const form = document.querySelector('form[data-captura-activos]');
  if (!form) return;
  const feedback = form.querySelector('[data-captura-feedback]');
  const draftNode = document.getElementById('captura-borrador');
  const draft = draftNode ? JSON.parse(draftNode.textContent) : null;
  if (draft) {
    const offsets = Object.create(null);
    form.querySelectorAll('input,select,textarea').forEach(function (control) {
      const values = draft[control.name];
      if (!values || control.type === 'file') return;
      if (control.type === 'checkbox') control.checked = values.includes(control.value);
      else {
        const index = offsets[control.name] || 0;
        control.value = values[index] || '';
        offsets[control.name] = index + 1;
      }
    });
    const details = form.closest('details');
    if (details) details.open = true;

  }
  function lock(locked) {
    form.querySelectorAll('input,select,textarea').forEach(function (el) {
      if (locked && !el.disabled) { el.dataset.captureLocked = 'true'; el.disabled = true; }
      else if (!locked && el.dataset.captureLocked) { el.disabled = false; delete el.dataset.captureLocked; }
    });
  }
  form.addEventListener('erp:action-start', function () {
    lock(true); feedback.textContent = 'Guardando el intento. Si se pierde la respuesta, reintenta para recuperar la misma orden.';
  });
  form.addEventListener('erp:action-error', function (event) {
    feedback.textContent = event.detail.message;
    if ([400, 403, 404].includes(event.detail.statusCode)) { lock(false); delete form._captureSnapshot; }
  });
  form.addEventListener('erp:action-success', function (event) {
    const data = event.detail;
    form.dataset.captureCompleted = "true";
    feedback.textContent = data.toast.message + ' ';
    const link = document.createElement('a'); link.href = data.enlace; link.textContent = 'Abrir orden y evidencias'; feedback.appendChild(link);
    const submit = form.querySelector('[type="submit"]');
    // El reintento sigue recuperando esta orden hasta que se elija otra captura.
    const next = document.createElement('button'); next.type = 'button'; next.className = 'btn btn-secondary'; next.textContent = 'Registrar otro mantenimiento';
    next.onclick = function () {
      lock(false); form.reset(); delete form._captureSnapshot;
      form.querySelectorAll("[data-solicitud-borrador]").forEach(function (el) { el.remove(); });
      form.elements.clave_captura.value = data.siguiente_clave;
      feedback.textContent = 'Nuevo intento. Revisa el equipo y los datos antes de guardar.';
      next.remove();
      form.dispatchEvent(new CustomEvent('activos:nueva-captura'));
      form.dataset.captureCompleted = "false";
      if (submit) { submit.disabled = false; submit.focus(); }
    };
    const old = form.querySelector('[data-nueva-captura]'); if (old) old.remove();
    next.dataset.nuevaCaptura = 'true'; feedback.appendChild(document.createTextNode(' ')); feedback.appendChild(next);
  });
}());
