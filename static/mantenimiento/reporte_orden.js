/* El formulario conserva el UUID y el cuerpo ante una respuesta incierta. */
(function () {
  window.bindReporteOrden = function (root) {
    root.querySelectorAll('form[data-report-order]').forEach(function (form) {
      if (form.dataset.reportBound) return;
      form.dataset.reportBound = 'true';
      var controls = Array.from(form.querySelectorAll('textarea,select,input:not([type="hidden"])'));
      var feedback = form.querySelector('[role="status"]');
      var scrollPosition = 0;
      var panel = form.closest('[data-report-order-panel]');
      var resultId = 'reporteOrdenResultado-' + form.dataset.reporteId;
      form.addEventListener('erp:action-start', function () {
        scrollPosition = window.scrollY;
        controls.forEach(function (control) { control.disabled = true; });
        feedback.textContent = 'Creando orden…';
      });
      form.addEventListener('erp:action-success', function () {
        var updated = document.getElementById(resultId);
        if (updated) window.bindReporteOrden(updated);
        window.requestAnimationFrame(function () {
          var result = document.getElementById(resultId);
          var drawer = panel?.closest('#mantDrawer');
          if (!result || (panel && (panel.hidden || !document.contains(panel) || panel.parentElement.hidden)) || (drawer && !drawer.classList.contains('is-open'))) return;
          result.focus({preventScroll:true});
          window.scrollTo(0, scrollPosition);
        });
      });
      form.addEventListener('erp:action-error', function (event) {
        var editable = [400, 403, 404].includes(event.detail.statusCode);
        controls.forEach(function (control) { control.disabled = !editable; });
        feedback.textContent = event.detail.message + (editable || [409, 410].includes(event.detail.statusCode) ? '' : ' Conservamos este intento. Reintenta sin cambiar los datos.');
        feedback.setAttribute('role', 'alert');
      });
    });
    window.ERPActionUI?.bind(root);
  };
  window.showReporteOrdenPanel = function (container, reportId) {
    Array.from(container.children).forEach(function (panel) { panel.hidden = true; });
    var panel = Array.from(container.children).find(function (item) { return item.dataset.reportId === reportId; });
    if (panel) { panel.hidden = false; return; }
    panel = document.createElement('div');
    panel.dataset.reportOrderPanel = 'true';
    panel.dataset.reportId = reportId;
    container.appendChild(panel);
    panel.innerHTML = '<p role="status">Cargando trabajos vinculados…</p>';
    fetch('/mantenimiento/reportes/' + reportId + '/orden/', {headers:{Accept:'application/json'}})
      .then(function (response) { if (!response.ok) throw new Error(); return response.json(); })
      .then(function (data) { panel.innerHTML = data.html; window.bindReporteOrden(panel); })
      .catch(function () {
        panel.innerHTML = '<p role="alert">No se pudieron consultar los trabajos.</p><button type="button" class="btn btn-secondary">Reintentar consulta</button>';
        panel.querySelector('button').onclick = function () { panel.remove(); window.showReporteOrdenPanel(container, reportId); };
      });
  };
  document.querySelector('[data-report-order-back]')?.addEventListener('click', function (event) {
    if (!document.referrer || window.history.length < 2) return;
    try {
      var previous = new URL(document.referrer);
      if (previous.origin === window.location.origin && previous.pathname === '/mantenimiento/') {
        event.preventDefault();
        window.history.back();
      }
    } catch (error) {}
  });
  window.bindReporteOrden(document);
})();
