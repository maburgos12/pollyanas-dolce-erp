(function () {
  "use strict";

  var region = document.getElementById("erp-toast-region");
  var confirmDialog = document.getElementById("erp-confirm-dialog");
  var confirmMessage = document.getElementById("erp-confirm-message");
  var confirmTrigger = null;
  var confirmForm = null;
  var pendingToastKey = "pollyanas.erpActions.pendingToast.v1";

  function safeNavigationUrl(value) {
    try {
      var url = new URL(value, window.location.href);
      if (url.protocol !== "http:" && url.protocol !== "https:") return null;
      if (url.origin !== window.location.origin) return null;
      if (url.username || url.password) return null;
      return url;
    } catch (_error) {
      return null;
    }
  }

  function normalizedToast(toast, fallbackType) {
    var allowedTypes = ["success", "info", "warning", "error"];
    var type = toast && allowedTypes.indexOf(toast.type) !== -1 ? toast.type : fallbackType || "info";
    var message = toast && typeof toast.message === "string" ? toast.message.slice(0, 500) : "Acción completada.";
    return { type: type, message: message, persistent: Boolean(toast && toast.persistent) };
  }

  function storePendingToast(toast) {
    try {
      window.sessionStorage.setItem(pendingToastKey, JSON.stringify(normalizedToast(toast, "success")));
      return true;
    } catch (_error) {
      return false;
    }
  }

  function consumePendingToast() {
    try {
      var raw = window.sessionStorage.getItem(pendingToastKey);
      if (!raw) return;
      window.sessionStorage.removeItem(pendingToastKey);
      showToast(normalizedToast(JSON.parse(raw), "info"));
    } catch (_error) {
      try { window.sessionStorage.removeItem(pendingToastKey); } catch (_ignored) {}
    }
  }

  function closeConfirm() {
    if (!confirmDialog) return;
    confirmDialog.hidden = true;
    if (confirmTrigger) confirmTrigger.focus();
    confirmTrigger = null;
    confirmForm = null;
  }

  function openConfirm(form, submitter) {
    if (!confirmDialog) return false;
    confirmForm = form;
    confirmTrigger = submitter;
    confirmMessage.textContent = form.dataset.confirmMessage;
    confirmDialog.hidden = false;
    confirmDialog.querySelector("[data-confirm-cancel]").focus();
    return true;
  }

  function showToast(options) {
    if (!region) return;
    var type = options.type || "info";
    var toast = document.createElement("div");
    toast.className = "erp-toast erp-toast--" + type;
    toast.setAttribute("role", type === "error" ? "alert" : "status");

    var message = document.createElement("span");
    message.textContent = options.message || "Acción completada.";
    toast.appendChild(message);

    var close = document.createElement("button");
    close.type = "button";
    close.className = "erp-toast__close";
    close.setAttribute("aria-label", "Cerrar notificación");
    close.textContent = "×";
    close.addEventListener("click", function () { toast.remove(); });
    toast.appendChild(close);
    region.appendChild(toast);

    if (!options.persistent) {
      window.setTimeout(function () { toast.remove(); }, 4500);
    }
  }

  function submitLabel(button) {
    return button.dataset.pendingLabel || (button.textContent.trim() === "Guardar" ? "Guardando…" : "Procesando…");
  }

  async function handleAction(event) {
    var form = event.currentTarget;
    if (form.dataset.actionPending === "true") {
      event.preventDefault();
      return;
    }
    if (!form.reportValidity()) return;
    event.preventDefault();

    var submitter = event.submitter || form.querySelector('[type="submit"]');
    if (form.dataset.confirmMessage && form.dataset.confirmed !== "true" && openConfirm(form, submitter)) return;
    form.dataset.confirmed = "false";
    var originalLabel = submitter ? submitter.textContent : "";
    var navigating = false;
    form.dataset.actionPending = "true";
    if (submitter) {
      submitter.disabled = true;
      submitter.textContent = submitLabel(submitter);
    }

    try {
      var formData = new FormData(form);
      if (submitter && submitter.name) formData.set(submitter.name, submitter.value);
      var response = await fetch(form.getAttribute("action") || window.location.href, {
        method: (form.method || "POST").toUpperCase(),
        body: formData,
        headers: { "Accept": "application/json", "X-Requested-With": "XMLHttpRequest" },
        credentials: "same-origin"
      });
      var contentType = response.headers && response.headers.get("content-type") || "";
      if (contentType.indexOf("application/json") === -1) {
        var responseUrl = response.redirected ? safeNavigationUrl(response.url) : null;
        if (responseUrl) {
          navigating = true;
          window.location.assign(responseUrl.href);
          return;
        }
        throw { toast: { type: "error", message: "El servidor devolvió una respuesta inesperada. Recarga la página e inténtalo de nuevo.", persistent: true } };
      }
      var payload = await response.json();
      if (!response.ok || !payload.ok) throw payload;

      var target = payload.target ? document.querySelector(payload.target) : null;
      if (target && payload.html) {
        target.outerHTML = payload.html;
        bind(document);
      }
      if (payload.redirect) {
        var redirectUrl = safeNavigationUrl(payload.redirect);
        if (!redirectUrl) {
          throw { toast: { type: "error", message: "La acción terminó, pero se rechazó un destino de navegación inseguro.", persistent: true } };
        }
        navigating = true;
        var redirectToast = normalizedToast(payload.toast, "success");
        if (storePendingToast(redirectToast)) {
          window.location.assign(redirectUrl.href);
        } else {
          showToast(redirectToast);
          window.setTimeout(function () { window.location.assign(redirectUrl.href); }, 900);
        }
        return;
      }
      showToast(payload.toast || { type: "success", message: "Acción completada." });
    } catch (error) {
      var toast = error && error.toast ? error.toast : {
        type: "error",
        message: "No se pudo completar la acción. Revisa tu conexión e inténtalo de nuevo.",
        persistent: true
      };
      showToast(toast);
    } finally {
      if (!navigating) {
        form.dataset.actionPending = "false";
        if (submitter && document.contains(submitter)) {
          submitter.disabled = false;
          submitter.textContent = originalLabel;
        }
      }
    }
  }

  function bind(root) {
    (root || document).querySelectorAll("form[data-async-action]").forEach(function (form) {
      if (form.dataset.actionBound === "true") return;
      form.dataset.actionBound = "true";
      form.addEventListener("submit", handleAction);
    });
  }

  document.querySelectorAll("#erp-toast-region .erp-toast").forEach(function (toast) {
    if (!toast.classList.contains("erp-toast--error")) {
      window.setTimeout(function () { toast.remove(); }, 4500);
    }
  });
  consumePendingToast();
  if (confirmDialog) {
    confirmDialog.querySelectorAll("[data-confirm-cancel]").forEach(function (button) {
      button.addEventListener("click", closeConfirm);
    });
    confirmDialog.querySelector("[data-confirm-accept]").addEventListener("click", function () {
      if (!confirmForm) return;
      var form = confirmForm;
      var trigger = confirmTrigger;
      confirmDialog.hidden = true;
      confirmForm = null;
      confirmTrigger = null;
      form.dataset.confirmed = "true";
      form.requestSubmit(trigger);
    });
    document.addEventListener("keydown", function (event) {
      if (confirmDialog.hidden) return;
      if (event.key === "Escape") {
        event.preventDefault();
        closeConfirm();
      }
      if (event.key === "Tab") {
        var focusable = Array.from(confirmDialog.querySelectorAll("button:not([disabled])"));
        var index = focusable.indexOf(document.activeElement);
        var next = event.shiftKey ? index - 1 : index + 1;
        if (next < 0 || next >= focusable.length) {
          event.preventDefault();
          focusable[event.shiftKey ? focusable.length - 1 : 0].focus();
        }
      }
    });
  }
  bind(document);
  window.ERPActionUI = { bind: bind, showToast: showToast };
})();
