(function () {
  "use strict";

  const toast = document.querySelector(".higiene-toast");
  let toastTimer;
  function showToast(message, tone) {
    if (!toast) return;
    toast.textContent = message;
    toast.dataset.tone = tone || "success";
    toast.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(function () { toast.hidden = true; }, 5000);
  }

  function openPanel(type) {
    document.querySelectorAll("[data-panel]").forEach(function (panel) {
      panel.hidden = panel.dataset.panel !== type;
    });
    document.querySelector(".today-block").hidden = true;
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  document.querySelectorAll("[data-open-panel]").forEach(function (button) {
    button.addEventListener("click", function () { openPanel(button.dataset.openPanel); });
  });
  document.querySelectorAll("[data-close-panel]").forEach(function (button) {
    button.addEventListener("click", function () {
      button.closest("[data-panel]").hidden = true;
      document.querySelector(".today-block").hidden = false;
    });
  });

  document.querySelectorAll("[data-review-point]").forEach(function (point) {
    point.querySelectorAll("[data-status]").forEach(function (input) {
      input.addEventListener("change", function () {
        const finding = input.value === "NO_CUMPLE";
        point.classList.toggle("has-finding", finding);
        const detail = point.querySelector("[data-finding-detail]");
        if (detail) detail.hidden = !finding;
        updateProgress(point.closest("form"));
      });
    });
    point.querySelectorAll("[data-resolution]").forEach(function (input) {
      input.addEventListener("change", function () {
        const fields = point.querySelector("[data-failure-fields]");
        if (fields) fields.hidden = input.value !== "SEGUIMIENTO";
      });
    });
    const target = point.querySelector("[data-target-type]");
    if (target) {
      target.addEventListener("change", function () {
        const type = target.value;
        point.querySelector("[data-installation-field]").hidden = type === "EQUIPO";
        point.querySelector("[data-asset-field]").hidden = type !== "EQUIPO";
        const category = point.querySelector("[data-category]");
        category.value = "";
        category.querySelectorAll("[data-category-type]").forEach(function (option) {
          option.hidden = option.dataset.categoryType !== type;
        });
      });
    }
    const numeric = point.querySelector("[data-numeric]");
    if (numeric) numeric.addEventListener("change", function () { updateProgress(point.closest("form")); });
  });

  function updateProgress(form) {
    if (!form) return;
    const points = Array.from(form.querySelectorAll("[data-review-point]"));
    const complete = points.filter(function (point) {
      if (point.dataset.kind === "NUMERICA") return Boolean(point.querySelector("[data-numeric]").value);
      return Boolean(point.querySelector("[data-status]:checked"));
    }).length;
    const progress = form.closest("[data-panel]").querySelector("[data-progress]");
    if (progress) progress.textContent = complete + " de " + points.length + " revisados";
  }

  document.querySelectorAll("[data-mark-section]").forEach(function (button) {
    button.addEventListener("click", function () {
      const fieldset = button.closest("fieldset");
      fieldset.querySelectorAll("[data-review-point]").forEach(function (point) {
        const compliant = point.querySelector('[data-status][value="CUMPLE"]');
        if (!compliant) return;
        compliant.checked = true;
        point.classList.remove("has-finding");
        const detail = point.querySelector("[data-finding-detail]");
        if (detail) detail.hidden = true;
      });
      updateProgress(fieldset.closest("form"));
    });
  });

  function buildAnswers(form) {
    const answers = [];
    form.querySelectorAll("[data-review-point]").forEach(function (point) {
      const answer = { key: point.dataset.key };
      if (point.dataset.kind === "NUMERICA") {
        answer.valor_numerico = point.querySelector("[data-numeric]").value;
      } else {
        const status = point.querySelector("[data-status]:checked");
        answer.respuesta = status ? status.value : "";
        if (answer.respuesta === "NO_CUMPLE") {
          const resolution = point.querySelector("[data-resolution]:checked");
          answer.observacion = point.querySelector("[data-observation]").value.trim();
          answer.corregido = Boolean(resolution && resolution.value === "CORREGIDO");
          answer.requiere_seguimiento = Boolean(resolution && resolution.value === "SEGUIMIENTO");
          if (answer.requiere_seguimiento) {
            answer.tipo_objetivo = point.querySelector("[data-target-type]").value;
            answer.area_instalacion = point.querySelector("[data-area]").value.trim();
            answer.activo_id = point.querySelector("[data-asset]").value;
            answer.categoria_id = point.querySelector("[data-category]").value;
            answer.prioridad = point.querySelector("[data-priority]").value;
          }
        }
      }
      answers.push(answer);
    });
    return answers;
  }

  document.querySelectorAll("form[data-higiene-form]").forEach(function (form) {
    form.addEventListener("submit", async function (event) {
      event.preventDefault();
      if (!form.reportValidity()) return;
      const button = event.submitter || form.querySelector("[type=submit]");
      const original = button.innerHTML;
      const type = form.querySelector("[name=tipo]").value;
      if (type === "CLORO_PH") {
        form.querySelector("[name=clave_instancia]").value = form.querySelector("[data-instance-source]").value;
      } else if (type === "BANOS") {
        form.querySelector("[name=clave_instancia]").value =
          form.querySelector("[data-bathroom]").value + "-" + form.querySelector("[data-round]").value;
      }
      form.querySelector("[name=respuestas]").value = JSON.stringify(buildAnswers(form));
      button.disabled = true;
      button.textContent = "Guardando…";
      try {
        const response = await fetch(form.action, {
          method: "POST",
          body: new FormData(form),
          headers: { "X-Requested-With": "XMLHttpRequest" },
          credentials: "same-origin"
        });
        const payload = await response.json();
        if (!response.ok) {
          const details = payload.fields ? Object.values(payload.fields).flat().join(" ") : "";
          throw new Error(details || payload.error || "No fue posible guardar.");
        }
        const failures = payload.reporte_falla_ids || [];
        showToast(
          payload.mensaje + (failures.length ? " Falla #" + failures.join(", #") + " enviada a Mantenimiento." : ""),
          "success"
        );
        setTimeout(function () {
          window.location.href = "/app/higiene/historial/#registro-" + payload.id;
        }, 900);
      } catch (error) {
        showToast(error.message, "error");
        button.disabled = false;
        button.innerHTML = original;
      }
    });
  });
})();
