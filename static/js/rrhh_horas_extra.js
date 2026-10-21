(function () {
  "use strict";
  // Una resolución actualiza su propia fila. No navega, reordena ni desplaza al operador.
  document.addEventListener("submit", async function (event) {
    var form = event.target.closest("form[data-extra-action]");
    if (!form) return;
    event.preventDefault();
    var row = form.closest("[data-extra-row]");
    if (row.dataset.pending === "true" || !form.reportValidity()) return;
    var submitter = event.submitter;
    if (!submitter) return;
    var data = new FormData(form);
    data.set(submitter.name, submitter.value);
    var buttons = Array.from(row.querySelectorAll('button[type="submit"]'));
    var previous = buttons.map(function (button) { return button.disabled; });
    var label = submitter.textContent;
    var feedback = row.querySelector("[data-extra-feedback]");
    feedback.textContent = "";
    row.dataset.pending = "true";
    buttons.forEach(function (button) { button.disabled = true; });
    submitter.textContent = submitter.dataset.pendingLabel || "Guardando…";
    var controller = new AbortController();
    var timeout = setTimeout(function () { controller.abort(); }, 30000);
    try {
      var response = await fetch(form.getAttribute("action") || window.location.href, {
        method: "POST", body: data, credentials: "same-origin", signal: controller.signal,
        headers: { "Accept": "application/json", "X-Requested-With": "XMLHttpRequest", "X-Horas-Extra-Inline": "1" }
      });
      if (!(response.headers.get("content-type") || "").includes("application/json")) {
        throw new Error("No se recibió confirmación. Actualiza la página para comprobar el estado y tu sesión antes de reintentar.");
      }
      var payload = await response.json();
      if (!response.ok || !payload.ok) throw new Error(payload.toast && payload.toast.message || "No se pudo guardar la solicitud.");
      var template = document.createElement("template");
      template.innerHTML = payload.html.trim();
      var replacement = template.content.firstElementChild;
      // Mantener el espacio y la posición de la fila evita saltos incluso en la última solicitud.
      replacement.style.minHeight = row.getBoundingClientRect().height + "px";
      row.replaceWith(replacement);
      var status = replacement.querySelector("[data-extra-feedback]");
      status.textContent = payload.message + (payload.estado !== "pendiente" ? " Puedes continuar con la siguiente solicitud; esta ya está en su historial." : "");
      status.focus({ preventScroll: true });
      Object.keys(payload.conteos).forEach(function (state) {
        document.querySelectorAll('[data-extra-count="' + state + '"]').forEach(function (count) { count.textContent = payload.conteos[state]; });
      });
      // Mismos enlaces de navegación existentes; no modificar permisos ni destinos.
      document.querySelectorAll('a[href="/rrhh/horas-extra/"]').forEach(function (link) {
        link.querySelectorAll(".nav-badge, .tab-badge").forEach(function (badge) {
          badge.textContent = payload.nav_pendientes;
          badge.setAttribute("aria-label", payload.nav_pendientes + " solicitudes pendientes");
          badge.hidden = !payload.nav_pendientes;
        });
      });
      var workGroup = document.getElementById("group-mi_trabajo");
      var workBadge = workGroup && workGroup.previousElementSibling.querySelector(".nav-badge");
      if (workBadge) {
        workBadge.textContent = payload.notificaciones_pendientes;
        workBadge.setAttribute("aria-label", payload.notificaciones_pendientes + " notificaciones pendientes");
        workBadge.hidden = !payload.notificaciones_pendientes;
      }
      document.querySelectorAll('a[href="/notificaciones/"]').forEach(function (link) {
        link.querySelectorAll(".nav-badge, .tab-badge").forEach(function (badge) {
          badge.textContent = payload.notificaciones_pendientes;
          badge.setAttribute("aria-label", payload.notificaciones_pendientes + " notificaciones pendientes");
          badge.hidden = !payload.notificaciones_pendientes;
        });
      });
    } catch (error) {
      feedback.textContent = error.name === "AbortError" ? "No se recibió confirmación a tiempo. Actualiza para comprobar el estado antes de reintentar." : error.message || "No se recibió confirmación. Revisa tu conexión y actualiza el estado antes de reintentar.";
      feedback.setAttribute("role", "alert");
      feedback.focus({ preventScroll: true });
    } finally {
      clearTimeout(timeout);
      row.dataset.pending = "false";
      buttons.forEach(function (button, index) { button.disabled = previous[index]; });
      submitter.textContent = label;
    }
  });
}());
