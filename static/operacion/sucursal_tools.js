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
  let stockRequest = 0;
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
      if (!response.ok) throw new Error(payload.error || "No fue posible actualizar los insumos.");
      const items = Array.isArray(payload.insumos) ? payload.insumos : [];
      const selectedCode = supply.value;
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
    const requestId = ++stockRequest;
    const selected = supply?.selectedOptions[0];
    const unit = selected?.dataset.unit || "";
    const code = selected?.value || "";
    document.querySelector("[data-unit-label]").textContent = unit ? `(${unit})` : "";
    const note = document.querySelector("[data-stock-note]");
    const quantity = document.querySelector("#cantidad_merma");
    const submit = mermaForm?.querySelector('button[type="submit"]');
    if (quantity) quantity.removeAttribute("max");
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
      const response = await fetch(url, {
        headers: { "X-Requested-With": "XMLHttpRequest" },
        credentials: "same-origin",
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload.error || "No fue posible consultar Point.");
      if (requestId !== stockRequest) return;
      const stock = payload.insumo?.existencia ?? "";
      const liveUnit = payload.insumo?.unidad || unit;
      document.querySelector("[data-unit-label]").textContent = liveUnit ? `(${liveUnit})` : "";
      if (quantity) quantity.max = stock;
      if (note) note.textContent = `Existencia disponible en Point: ${stock} ${liveUnit}`;
      if (submit) submit.disabled = false;
    } catch (error) {
      if (requestId !== stockRequest) return;
      if (note) note.textContent = error.message;
      showToast(error.message, "error");
    }
  }
  supply?.addEventListener("change", syncSupply);
  recoverSupplyCatalog().then(syncSupply);

  document.querySelectorAll("form[data-async-action]").forEach((form) => {
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!form.reportValidity()) return;
      const button = event.submitter || form.querySelector('button[type="submit"]');
      if (!button || button.disabled) return;
      const original = button.textContent;
      button.disabled = true;
      button.textContent = "Procesando…";
      try {
        const body = new FormData(form);
        if (button.name) body.set(button.name, button.value);
        const response = await fetch(form.action, {
          method: "POST",
          body,
          headers: { "X-Requested-With": "XMLHttpRequest" },
          credentials: "same-origin",
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(payload.error || "No fue posible guardar la captura.");
        showToast(form.id === "falla-form" ? "Reporte enviado a Mantenimiento." : "Merma enviada correctamente.");
        if (form.dataset.resetOnSuccess !== "false") form.reset();
        if (form.id === "falla-form") syncFailureTarget();
        if (form.id === "merma-form") await syncSupply();
        document.dispatchEvent(new CustomEvent("operacion:action-complete", { detail: payload }));
      } catch (error) {
        showToast(error.message, "error");
      } finally {
        button.disabled = form.id === "merma-form" && !supply?.value;
        button.textContent = original;
      }
    });
  });
})();
