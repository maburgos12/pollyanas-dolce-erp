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
  function syncFailureTarget() {
    const equipment = document.querySelector('input[name="tipo_objetivo"]:checked')?.value === "EQUIPO";
    document.querySelector("[data-equipment-fields]").hidden = !equipment;
    document.querySelector("[data-installation-fields]").hidden = equipment;
    document.querySelector("[data-equipment-options]").disabled = !equipment;
    document.querySelector("[data-installation-options]").disabled = equipment;
    document.querySelector("#activo_id").required = equipment;
    document.querySelector("#area_instalacion").required = !equipment;
    document.querySelector("#categoria_falla").value = "";
  }
  objectiveInputs.forEach((input) => input.addEventListener("change", syncFailureTarget));
  if (objectiveInputs.length) syncFailureTarget();

  const supply = document.querySelector("#codigo_point");
  function syncSupply() {
    const selected = supply?.selectedOptions[0];
    const unit = selected?.dataset.unit || "";
    const stock = selected?.dataset.stock || "";
    document.querySelector("[data-unit-label]").textContent = unit ? `(${unit})` : "";
    const note = document.querySelector("[data-stock-note]");
    const quantity = document.querySelector("#cantidad_merma");
    if (quantity) quantity.max = stock || "";
    if (note) {
      note.hidden = !unit;
      note.textContent = unit ? `Existencia disponible en Point: ${stock} ${unit}` : "";
    }
  }
  supply?.addEventListener("change", syncSupply);
  syncSupply();

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
        if (form.id === "merma-form") syncSupply();
        document.dispatchEvent(new CustomEvent("operacion:action-complete", { detail: payload }));
      } catch (error) {
        showToast(error.message, "error");
      } finally {
        button.disabled = false;
        button.textContent = original;
      }
    });
  });
})();
