(() => {
  const panels = [...document.querySelectorAll("[data-tab-panel]")];
  const tabs = [...document.querySelectorAll("[data-tab-target]")];
  const toast = document.querySelector(".toast");
  let toastTimer;

  function showToast(message, tone = "success") {
    if (!toast) return;
    toast.textContent = message;
    toast.dataset.tone = tone;
    toast.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { toast.hidden = true; }, 5000);
  }

  function activateTab(name) {
    tabs.forEach((tab) => tab.setAttribute("aria-selected", String(tab.dataset.tabTarget === name)));
    panels.forEach((panel) => { panel.hidden = panel.dataset.tabPanel !== name; });
    const url = new URL(window.location.href);
    url.searchParams.set("tab", name);
    history.replaceState(null, "", url);
  }
  tabs.forEach((tab) => tab.addEventListener("click", () => activateTab(tab.dataset.tabTarget)));

  const objectiveInputs = [...document.querySelectorAll('input[name="tipo_objetivo"]')];
  function syncFailureTarget(clearCategory = false) {
    const equipment = document.querySelector('input[name="tipo_objetivo"]:checked')?.value === "EQUIPO";
    document.querySelector("[data-equipment-fields]").hidden = !equipment;
    document.querySelector("[data-installation-fields]").hidden = equipment;
    document.querySelector("[data-equipment-options]").disabled = !equipment;
    document.querySelector("[data-installation-options]").disabled = equipment;
    document.querySelector("#activo_id").required = equipment;
    document.querySelector("#area_instalacion").required = !equipment;
    if (clearCategory) document.querySelector("#categoria_falla").value = "";
  }
  objectiveInputs.forEach((input) => input.addEventListener("change", () => syncFailureTarget(true)));
  if (objectiveInputs.length) syncFailureTarget(false);

  const supply = document.querySelector("#codigo_point");
  const mermaForm = document.querySelector("#merma-form");
  function captureUuid() {
    const bytes = new Uint8Array(16); window.crypto.getRandomValues(bytes);
    bytes[6] = (bytes[6] & 15) | 64; bytes[8] = (bytes[8] & 63) | 128;
    const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }
  let captureId = mermaForm ? captureUuid() : null;
  let stockRequest = 0, stockReady = false, draftPhoto = null, restoredCode = "";
  const draftStatus = mermaForm?.querySelector("[data-draft-status]");
  let draftDb;
  const draftReady = (async () => {
    if (!mermaForm?.dataset.draftKey) return;
    try {
      draftDb = await new Promise((resolve, reject) => {
        const request = indexedDB.open("pollyanas-mermas", 1);
        request.onupgradeneeded = () => request.result.createObjectStore("drafts");
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      });
      const saved = await draftOperation("get");
      if (saved) {
        captureId = saved.requestId || captureId;
        Object.entries(saved.fields).forEach(([name, value]) => {
          const field = mermaForm.elements.namedItem(name);
          if (field) field.value = value;
        });
        restoredCode = saved.fields.codigo_point || "";
        draftPhoto = saved.photo;
        draftStatus.textContent = "Borrador recuperado en este dispositivo" + (draftPhoto ? "; incluye tu foto." : ".");
      }
    } catch (_) {
      draftStatus.textContent = "No se pudo conservar el borrador en este dispositivo. Mantén la página abierta hasta confirmar el envío.";
    }
  })();
  function draftOperation(action, value) {
    return new Promise((resolve, reject) => {
      const transaction = draftDb.transaction("drafts", action === "get" ? "readonly" : "readwrite");
      const store = transaction.objectStore("drafts");
      // shortcut: un borrador por usuario y sucursal; separar por captura si se habilitan formularios simultáneos.
      const request = action === "put" ? store.put(value, mermaForm.dataset.draftKey) : store[action](mermaForm.dataset.draftKey);
      transaction.oncomplete = () => resolve(request.result);
      transaction.onerror = transaction.onabort = () => reject(transaction.error);
    });
  }
  async function saveDraft() {
    await draftReady;
    if (!draftDb) return;
    const fields = {};
    ["codigo_point", "cantidad", "motivo", "comentario", "justificacion_sin_foto"].forEach((name) => {
      fields[name] = mermaForm.elements.namedItem(name).value;
    });
    try {
      await draftOperation("put", {requestId: captureId, fields, photo: draftPhoto});
      draftStatus.textContent = "Borrador guardado en este dispositivo. Aún no se ha enviado.";
    } catch (_) {
      draftStatus.textContent = "No se pudo guardar el borrador. Mantén esta página abierta.";
    }
  }
  mermaForm?.addEventListener("input", saveDraft);
  mermaForm?.addEventListener("change", (event) => {
    if (event.target.name === "foto_evidencia") draftPhoto = event.target.files[0] || null;
    saveDraft();
  });
  async function recoverSupplyCatalog() {
    const status = document.querySelector("[data-catalog-status]");
    if (!supply || !mermaForm?.dataset.stockUrl) {
      if (status) status.hidden = true;
      return;
    }
    if (status) {
      status.hidden = false;
      status.textContent = "Actualizando los insumos recibidos por esta sucursal…";
    }
    try {
      const response = await fetch(mermaForm.dataset.stockUrl, {
        headers: { "X-Requested-With": "XMLHttpRequest" },
        credentials: "same-origin",
        cache: "no-store",
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || response.redirected || !Array.isArray(payload.insumos)) throw new Error(payload.error || "No fue posible actualizar los insumos. Revisa tu sesión y reintenta.");
      const items = Array.isArray(payload.insumos) ? payload.insumos : [];
      const selectedCode = supply.value || restoredCode;
      const placeholder = supply.querySelector('option[value=""]') || document.createElement("option");
      placeholder.value = "";
      placeholder.textContent = "Selecciona un insumo";
      const fragment = document.createDocumentFragment();
      items.forEach((item) => {
        const option = document.createElement("option");
        option.value = item.codigo_point;
        option.dataset.unit = item.unidad || "";
        option.textContent = item.nombre;
        fragment.appendChild(option);
      });
      supply.replaceChildren(placeholder, fragment);
      if (items.some((item) => item.codigo_point === selectedCode)) {
        supply.value = selectedCode;
      }
      restoredCode = "";
      if (status) {
        status.hidden = items.length > 0;
        status.textContent = items.length
          ? ""
          : "No hay insumos recibidos disponibles para esta sucursal.";
      }
    } catch (error) {
      if (status) {
        status.hidden = false;
        status.textContent = error.message;
      }
      showToast(error.message, "error");
    }
  }
  async function syncSupply() {
    if (!supply || !mermaForm) return;
    const requestId = ++stockRequest;
    const selected = supply?.selectedOptions[0];
    const unit = selected?.dataset.unit || "";
    const code = selected?.value || "";
    const unitLabel = document.querySelector("[data-unit-label]");
    if (unitLabel) unitLabel.textContent = unit ? `(${unit})` : "";
    const note = document.querySelector("[data-stock-note]");
    const quantity = document.querySelector("#cantidad_merma");
    const submit = mermaForm?.querySelector('button[type="submit"]');
    if (quantity) quantity.removeAttribute("max");
    stockReady = false;
    if (submit) submit.disabled = true;
    if (!code || !mermaForm?.dataset.stockUrl) {
      if (note) note.hidden = true;
      return;
    }
    if (note) {
      note.hidden = false;
      note.textContent = "Consultando existencia vigente en Point…";
    }
    try {
      const url = new URL(mermaForm.dataset.stockUrl, window.location.origin);
      url.searchParams.set("codigo_point", code);
      let response, payload;
      for (let attempt = 0; attempt < 20; attempt++) {
        if (requestId !== stockRequest) return;
        response = await fetch(url, {
          headers: { "X-Requested-With": "XMLHttpRequest" },
          credentials: "same-origin", cache: "no-store",
        });
        payload = await response.json().catch(() => ({}));
        if (requestId !== stockRequest) return;
        if (response.status !== 503 || payload.code !== "point_busy") break;
        if (note) note.textContent = "En espera de que Point termine la sincronización. Tu captura se conserva.";
        if (attempt < 19) await new Promise((resolve) => setTimeout(resolve, 3000));
      }
      if (!response.ok) throw new Error(payload.code === "point_busy"
        ? "Point sigue ocupado. Tu captura se conserva; vuelve a seleccionar el insumo para reintentar."
        : payload.error || "No fue posible consultar Point.");
      if (requestId !== stockRequest) return;
      if (response.redirected || payload.insumo?.existencia == null) throw new Error("No recibimos la existencia de Point. Revisa tu sesión; tu captura se conserva.");
      const stock = payload.insumo.existencia;
      const liveUnit = payload.insumo?.unidad || unit;
      if (unitLabel) unitLabel.textContent = liveUnit ? `(${liveUnit})` : "";
      if (quantity) quantity.max = stock;
      if (note) note.textContent = `Existencia disponible en Point: ${stock} ${liveUnit}`;
      stockReady = true;
      if (submit) submit.disabled = false;
    } catch (error) {
      if (requestId !== stockRequest) return;
      if (note) note.textContent = error.message;
      showToast(error.message, "error");
    }
  }
  supply?.addEventListener("change", syncSupply);
  draftReady.then(recoverSupplyCatalog).then(syncSupply);

  // Freno de duplicados. El servidor decide (409); esto sólo le da forma a la
  // decisión: abrir lo que ya existe o afirmar que es otro problema.
  const dupModal = document.querySelector("[data-dup-modal]");
  const dupLista = dupModal?.querySelector("[data-dup-lista]");
  const dupCheckbox = dupModal?.querySelector("[data-dup-checkbox]");
  const dupContinuar = dupModal?.querySelector("[data-dup-continuar]");
  const dupAbrir = dupModal?.querySelector("[data-dup-abrir]");
  const dupCerrar = dupModal?.querySelector("[data-dup-cerrar]");
  let dupDisparador = null;
  let dupResolver = null;

  function dupFocusables() {
    return [...dupModal.querySelectorAll("button, input, a[href]")].filter((el) => !el.disabled);
  }

  function cerrarDup(resultado) {
    if (!dupModal || dupModal.hidden) return;
    dupModal.hidden = true;
    document.removeEventListener("keydown", dupTeclado, true);
    const resolver = dupResolver;
    dupResolver = null;
    dupDisparador?.focus();
    dupDisparador = null;
    resolver?.(resultado);
  }

  function dupTeclado(event) {
    if (event.key === "Escape") {
      event.preventDefault();
      cerrarDup(false);
      return;
    }
    if (event.key !== "Tab") return;
    const focusables = dupFocusables();
    if (!focusables.length) return;
    const primero = focusables[0];
    const ultimo = focusables[focusables.length - 1];
    if (event.shiftKey && document.activeElement === primero) {
      event.preventDefault();
      ultimo.focus();
    } else if (!event.shiftKey && document.activeElement === ultimo) {
      event.preventDefault();
      primero.focus();
    }
  }

  function pedirConfirmacionDuplicado(reportes, disparador) {
    if (!dupModal || !dupLista) return Promise.resolve(false);
    dupLista.replaceChildren();
    reportes.forEach((reporte) => {
      const item = document.createElement("li");
      const enlace = document.createElement("a");
      enlace.href = `/fallas/app/?reporte=${encodeURIComponent(reporte.id)}`;
      enlace.textContent = reporte.titulo;
      const detalle = document.createElement("small");
      detalle.textContent = `${reporte.estatus_label || reporte.estatus} · Prioridad ${reporte.prioridad_label || reporte.prioridad}`;
      item.append(enlace, detalle);
      dupLista.appendChild(item);
    });
    if (dupAbrir) {
      dupAbrir.dataset.href = reportes.length
        ? `/fallas/app/?reporte=${encodeURIComponent(reportes[0].id)}`
        : "/fallas/app/";
    }
    if (dupCheckbox) dupCheckbox.checked = false;
    if (dupContinuar) dupContinuar.disabled = true;
    dupDisparador = disparador || document.activeElement;
    dupModal.hidden = false;
    document.addEventListener("keydown", dupTeclado, true);
    (dupAbrir || dupFocusables()[0])?.focus();
    return new Promise((resolve) => { dupResolver = resolve; });
  }

  dupCheckbox?.addEventListener("change", () => {
    if (dupContinuar) dupContinuar.disabled = !dupCheckbox.checked;
  });
  dupContinuar?.addEventListener("click", () => cerrarDup(true));
  dupCerrar?.addEventListener("click", () => cerrarDup(false));
  dupAbrir?.addEventListener("click", () => {
    const destino = dupAbrir.dataset.href || "/fallas/app/";
    cerrarDup(false);
    window.location.href = destino;
  });
  dupModal?.addEventListener("click", (event) => {
    if (event.target === dupModal) cerrarDup(false);
  });

  document.querySelectorAll("form[data-async-action]").forEach((form) => {
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!form.reportValidity()) return;
      const button = event.submitter || form.querySelector('button[type="submit"]');
      if (!button || button.disabled) return;
      const original = button.textContent;
      button.disabled = true;
      button.textContent = "Procesando…";
      let controls = [];
      try {
        if (form.id === "merma-form") await saveDraft();
        const body = new FormData(form);
        if (form.id === "merma-form" && draftPhoto) body.set("foto_evidencia", draftPhoto);
        if (form.id === "merma-form") {
          body.set("request_id", captureId);
          controls = Array.from(form.elements).filter((field) => !field.disabled);
          controls.forEach((field) => { field.disabled = true; });
        }
        const token = document.cookie.split("; ").find((row) => row.startsWith("csrftoken="))?.slice(10);
        if (token) body.set("csrfmiddlewaretoken", decodeURIComponent(token));
        if (button.name) body.set(button.name, button.value);
        let response = await fetch(form.action, {
          method: "POST",
          body,
          headers: { "X-Requested-With": "XMLHttpRequest" },
          credentials: "same-origin",
        });
        let payload = await response.json().catch(() => ({}));
        if (response.status === 409 && Array.isArray(payload.existing_reports)) {
          const continuar = await pedirConfirmacionDuplicado(payload.existing_reports, button);
          if (!continuar) {
            showToast("No se envió nada; tu captura sigue aquí.", "warning");
            return;
          }
          body.set("confirmar_problema_distinto", "1");
          response = await fetch(form.action, {
            method: "POST",
            body,
            headers: { "X-Requested-With": "XMLHttpRequest" },
            credentials: "same-origin",
          });
          payload = await response.json().catch(() => ({}));
        }
        if (form.id === "merma-form" && response.status === 503 && payload.code === "point_busy") {
          showToast("Point está sincronizando. Tu captura se conserva; validaremos la existencia antes de volver a enviar.", "warning");
          await syncSupply();
          return;
        }
        if (!response.ok || response.redirected || (form.id === "merma-form" && (!payload.id || payload.request_id !== captureId))) throw new Error(payload.error || "No fue posible guardar. Revisa tu sesión; la captura se conserva.");
        if (form.id === "merma-form") {
          draftPhoto = null;
          captureId = captureUuid();
          if (draftDb) {
            try { await draftOperation("delete"); }
            catch (_) {
              draftStatus.textContent = "Merma enviada, pero el borrador sigue en el dispositivo. No vuelvas a enviarlo.";
              form.reset();
              await syncSupply();
              return;
            }
          }
          draftStatus.textContent = "Merma enviada. Borrador retirado.";
        }
        showToast(form.id === "falla-form" ? "Reporte enviado a Mantenimiento." : "Merma enviada correctamente.");
        if (form.dataset.resetOnSuccess !== "false") form.reset();
        if (form.id === "falla-form") syncFailureTarget();
        if (form.id === "merma-form") await syncSupply();
        document.dispatchEvent(new CustomEvent("operacion:action-complete", { detail: payload }));
      } catch (error) {
        const sinRed = error instanceof TypeError || !navigator.onLine;
        showToast(sinRed ? "Se perdió la conexión. La captura se conserva; verifica el historial antes de reenviar, porque el servidor pudo haberla recibido." : error.message, "error");
      } finally {
        controls.forEach((field) => { field.disabled = false; });
        button.disabled = form.id === "merma-form" && !stockReady;
        button.textContent = original;
      }
    });
  });
})();
