(function () {
  "use strict";

  const toast = document.querySelector(".higiene-toast");
  const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  let toastTimer;

  function showToast(message, tone) {
    if (!toast) return;
    toast.textContent = message;
    toast.dataset.tone = tone || "success";
    toast.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(function () {
      toast.hidden = true;
    }, 5000);
  }

  function scrollToWorkflow(form) {
    const hero = form.closest("[data-panel]").querySelector(".workflow-hero");
    if (hero) {
      hero.scrollIntoView({ block: "start", behavior: reduceMotion ? "auto" : "smooth" });
    }
  }

  function setDefaultTime(form) {
    const field = form.querySelector('input[name="hora"]');
    if (!field || field.value) return;
    const now = new Date();
    field.value = String(now.getHours()).padStart(2, "0") + ":" +
      String(now.getMinutes()).padStart(2, "0");
  }

  function selectedValue(point, selector) {
    const selected = point.querySelector(selector + ":checked");
    return selected ? selected.value : "";
  }

  function pointIsComplete(point, includeFindingDetail) {
    if (point.dataset.kind === "NUMERICA") {
      return Boolean(point.querySelector("[data-numeric]").value);
    }
    const status = selectedValue(point, "[data-status]");
    if (!status) return false;
    if (status !== "NO_CUMPLE" || !includeFindingDetail) return true;

    const observation = point.querySelector("[data-observation]").value.trim();
    const resolution = selectedValue(point, "[data-resolution]");
    if (!observation || !resolution) return false;
    if (resolution !== "SEGUIMIENTO") return true;

    const target = point.querySelector("[data-target-type]").value;
    const category = point.querySelector("[data-category]").value;
    if (!target || !category) return false;
    if (target === "EQUIPO") return Boolean(point.querySelector("[data-asset]").value);
    return Boolean(point.querySelector("[data-area]").value.trim());
  }

  function updatePointState(point) {
    const label = point.querySelector("[data-point-state]");
    let text = "Sin revisar";
    let complete = false;

    if (point.dataset.kind === "NUMERICA") {
      complete = Boolean(point.querySelector("[data-numeric]").value);
      if (complete) text = "Medición registrada";
    } else {
      const status = selectedValue(point, "[data-status]");
      complete = Boolean(status);
      if (status === "CUMPLE") text = "Cumple";
      if (status === "NO_CUMPLE") text = "Hallazgo detectado";
      if (status === "NA") text = "No aplica";
    }

    point.classList.toggle("is-complete", complete);
    if (label) label.textContent = text;
  }

  function sectionCompletion(section) {
    const points = Array.from(section.querySelectorAll("[data-review-point]"));
    return {
      complete: points.filter(function (point) {
        return pointIsComplete(point, false);
      }).length,
      ready: points.filter(function (point) {
        return pointIsComplete(point, true);
      }).length,
      total: points.length
    };
  }

  function updateWorkflow(form) {
    if (!form) return;
    const points = Array.from(form.querySelectorAll("[data-review-point]"));
    const completed = points.filter(function (point) {
      return pointIsComplete(point, false);
    }).length;
    const percentage = points.length ? Math.round((completed / points.length) * 100) : 0;
    const panel = form.closest("[data-panel]");

    points.forEach(updatePointState);
    const progress = panel.querySelector("[data-progress]");
    const percent = panel.querySelector("[data-progress-percent]");
    const bar = panel.querySelector("[data-progress-bar]");
    if (progress) progress.textContent = completed + " de " + points.length + " revisados";
    if (percent) percent.textContent = percentage + "%";
    if (bar) bar.style.width = percentage + "%";

    form.querySelectorAll("[data-review-section]").forEach(function (section) {
      const result = sectionCompletion(section);
      const counter = section.querySelector("[data-section-complete]");
      const step = form.querySelector('[data-section-step="' + section.dataset.sectionIndex + '"]');
      if (counter) counter.textContent = result.complete;
      if (step) {
        const isComplete = result.ready === result.total;
        step.classList.toggle("is-complete", isComplete);
        const state = step.querySelector("[data-step-state]");
        if (state && step.getAttribute("aria-current") !== "step") {
          state.textContent = isComplete ? "Completa" : result.complete + " de " + result.total;
        }
      }
    });

    const findings = points.filter(function (point) {
      return selectedValue(point, "[data-status]") === "NO_CUMPLE";
    });
    const failures = findings.filter(function (point) {
      return selectedValue(point, "[data-resolution]") === "SEGUIMIENTO";
    });
    const finishComplete = form.querySelector("[data-finish-complete]");
    const finishFindings = form.querySelector("[data-finish-findings]");
    const finishFailures = form.querySelector("[data-finish-failures]");
    const finishMessage = form.querySelector("[data-finish-message]");
    if (finishComplete) finishComplete.textContent = completed;
    if (finishFindings) finishFindings.textContent = findings.length;
    if (finishFailures) finishFailures.textContent = failures.length;
    if (finishMessage) {
      finishMessage.textContent = completed === points.length
        ? "La revisión está completa y lista para guardarse."
        : "Faltan " + (points.length - completed) + " puntos por revisar.";
    }

    const finishStep = form.querySelector('[data-section-step="finish"]');
    if (finishStep) {
      const ready = points.every(function (point) {
        return pointIsComplete(point, true);
      });
      finishStep.classList.toggle("is-complete", ready);
      const state = finishStep.querySelector("[data-step-state]");
      if (state && finishStep.getAttribute("aria-current") !== "step") {
        state.textContent = ready ? "Lista" : "Pendiente";
      }
    }
  }

  function showSection(form, target, shouldScroll) {
    const sections = Array.from(form.querySelectorAll("[data-review-section]"));
    const finish = form.querySelector("[data-finish-step]");
    const isFinish = target === "finish";

    sections.forEach(function (section) {
      section.hidden = isFinish || section.dataset.sectionIndex !== String(target);
    });
    if (finish) finish.hidden = !isFinish;

    form.querySelectorAll("[data-section-step]").forEach(function (step) {
      const active = step.dataset.sectionStep === String(target);
      if (active) {
        step.setAttribute("aria-current", "step");
        const state = step.querySelector("[data-step-state]");
        if (state) state.textContent = "En curso";
      } else {
        step.removeAttribute("aria-current");
      }
    });
    form.dataset.activeSection = String(target);
    updateWorkflow(form);
    if (shouldScroll) scrollToWorkflow(form);
  }

  function firstIncompletePoint(section) {
    return Array.from(section.querySelectorAll("[data-review-point]")).find(function (point) {
      return !pointIsComplete(point, true);
    });
  }

  function explainIncompletePoint(point) {
    if (point.dataset.kind === "NUMERICA") return "Selecciona la medición antes de continuar.";
    const status = selectedValue(point, "[data-status]");
    if (!status) return "Indica si el punto cumple, no cumple o no aplica.";
    if (status === "NO_CUMPLE") {
      if (!point.querySelector("[data-observation]").value.trim()) {
        return "Describe el hallazgo antes de continuar.";
      }
      const resolution = selectedValue(point, "[data-resolution]");
      if (!resolution) return "Indica si se corrigió o debe enviarse a Fallas.";
      if (resolution === "SEGUIMIENTO") {
        return "Completa los datos necesarios para enviar el reporte a Fallas.";
      }
    }
    return "Completa este punto antes de continuar.";
  }

  function goToFinish(form) {
    const sections = Array.from(form.querySelectorAll("[data-review-section]"));
    const incompleteSection = sections.find(function (section) {
      return Boolean(firstIncompletePoint(section));
    });
    if (incompleteSection) {
      const point = firstIncompletePoint(incompleteSection);
      showSection(form, Number(incompleteSection.dataset.sectionIndex), true);
      showToast(explainIncompletePoint(point), "error");
      point.scrollIntoView({ block: "center", behavior: reduceMotion ? "auto" : "smooth" });
      return;
    }
    showSection(form, "finish", true);
  }

  function openPanel(type) {
    document.querySelectorAll("[data-panel]").forEach(function (panel) {
      panel.hidden = panel.dataset.panel !== type;
    });
    const overview = document.querySelector("[data-capture-overview]");
    if (overview) overview.hidden = true;
    const panel = document.querySelector('[data-panel="' + type + '"]');
    const form = panel ? panel.querySelector("[data-higiene-form]") : null;
    if (form) {
      setDefaultTime(form);
      showSection(form, 0, false);
    }
    window.scrollTo({ top: 0, behavior: reduceMotion ? "auto" : "smooth" });
  }

  document.querySelectorAll("[data-open-panel]").forEach(function (button) {
    button.addEventListener("click", function () {
      openPanel(button.dataset.openPanel);
    });
  });

  document.querySelectorAll("[data-close-panel]").forEach(function (button) {
    button.addEventListener("click", function () {
      button.closest("[data-panel]").hidden = true;
      const overview = document.querySelector("[data-capture-overview]");
      if (overview) overview.hidden = false;
      window.scrollTo({ top: 0, behavior: reduceMotion ? "auto" : "smooth" });
    });
  });

  document.querySelectorAll("form[data-higiene-form]").forEach(function (form) {
    showSection(form, 0, false);

    form.querySelectorAll("[data-section-step]").forEach(function (step) {
      step.addEventListener("click", function () {
        if (step.dataset.sectionStep === "finish") {
          goToFinish(form);
          return;
        }
        showSection(form, Number(step.dataset.sectionStep), true);
      });
    });

    form.querySelectorAll("[data-section-next]").forEach(function (button) {
      button.addEventListener("click", function () {
        const section = button.closest("[data-review-section]");
        const incomplete = firstIncompletePoint(section);
        if (incomplete) {
          showToast(explainIncompletePoint(incomplete), "error");
          incomplete.scrollIntoView({ block: "center", behavior: reduceMotion ? "auto" : "smooth" });
          return;
        }
        const sections = Array.from(form.querySelectorAll("[data-review-section]"));
        const current = Number(section.dataset.sectionIndex);
        if (current === sections.length - 1) {
          goToFinish(form);
        } else {
          showSection(form, current + 1, true);
        }
      });
    });

    form.querySelectorAll("[data-section-previous]").forEach(function (button) {
      button.addEventListener("click", function () {
        const section = button.closest("[data-review-section]");
        const current = section
          ? Number(section.dataset.sectionIndex)
          : form.querySelectorAll("[data-review-section]").length;
        showSection(form, Math.max(0, current - 1), true);
      });
    });
  });

  document.querySelectorAll("[data-review-point]").forEach(function (point) {
    point.querySelectorAll("[data-status]").forEach(function (input) {
      input.addEventListener("change", function () {
        const finding = input.value === "NO_CUMPLE";
        point.classList.toggle("has-finding", finding);
        const detail = point.querySelector("[data-finding-detail]");
        if (detail) detail.hidden = !finding;
        updateWorkflow(point.closest("form"));
      });
    });

    point.querySelectorAll("[data-resolution]").forEach(function (input) {
      input.addEventListener("change", function () {
        const fields = point.querySelector("[data-failure-fields]");
        if (fields) fields.hidden = input.value !== "SEGUIMIENTO";
        updateWorkflow(point.closest("form"));
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
        updateWorkflow(point.closest("form"));
      });
    }

    point.querySelectorAll("input, select, textarea").forEach(function (control) {
      control.addEventListener("input", function () {
        updateWorkflow(point.closest("form"));
      });
      control.addEventListener("change", function () {
        updateWorkflow(point.closest("form"));
      });
    });
  });

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
      updateWorkflow(fieldset.closest("form"));
      showToast("Área marcada como cumple.", "success");
    });
  });

  function buildAnswers(form) {
    const answers = [];
    form.querySelectorAll("[data-review-point]").forEach(function (point) {
      const answer = { key: point.dataset.key };
      if (point.dataset.kind === "NUMERICA") {
        answer.valor_numerico = point.querySelector("[data-numeric]").value;
      } else {
        answer.respuesta = selectedValue(point, "[data-status]");
        if (answer.respuesta === "NO_CUMPLE") {
          const resolution = selectedValue(point, "[data-resolution]");
          answer.observacion = point.querySelector("[data-observation]").value.trim();
          answer.corregido = resolution === "CORREGIDO";
          answer.requiere_seguimiento = resolution === "SEGUIMIENTO";
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
      const points = Array.from(form.querySelectorAll("[data-review-point]"));
      const incomplete = points.find(function (point) {
        return !pointIsComplete(point, true);
      });
      if (incomplete) {
        const section = incomplete.closest("[data-review-section]");
        showSection(form, Number(section.dataset.sectionIndex), true);
        showToast(explainIncompletePoint(incomplete), "error");
        return;
      }

      const button = event.submitter || form.querySelector("[type=submit]");
      const original = button.innerHTML;
      const type = form.querySelector("[name=tipo]").value;
      if (type === "CLORO_PH") {
        form.querySelector("[name=clave_instancia]").value =
          form.querySelector("[data-instance-source]").value;
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
          payload.mensaje +
            (failures.length ? " Falla #" + failures.join(", #") + " enviada a Mantenimiento." : ""),
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
