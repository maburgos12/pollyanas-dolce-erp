(function () {
  "use strict";

  const toast = document.querySelector(".higiene-toast");
  const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const failureRequests = new WeakMap();
  const failureTimers = new WeakMap();
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

  function isFailureFollowUp(point) {
    return selectedValue(point, "[data-resolution]") === "SEGUIMIENTO";
  }

  function failureClassificationComplete(point) {
    if (!isFailureFollowUp(point)) return false;
    const target = point.querySelector("[data-target-type]").value;
    const category = point.querySelector("[data-category]").value;
    if (!target || !category) return false;
    if (target === "EQUIPO") return Boolean(point.querySelector("[data-asset]").value);
    return Boolean(point.querySelector("[data-area]").value.trim());
  }

  function currentFailureIdentity(point) {
    const form = point.closest("form");
    return {
      tipo: form.querySelector("[name=tipo]").value,
      punto_clave: point.dataset.key,
      tipo_objetivo: point.querySelector("[data-target-type]").value,
      categoria_id: point.querySelector("[data-category]").value,
      activo_id: point.querySelector("[data-asset]").value,
      area_instalacion: point.querySelector("[data-area]").value.trim()
    };
  }

  function clearFailureChoices(point) {
    const reports = point.querySelector("[data-match-report-list]");
    if (reports) reports.replaceChildren();
    point.querySelectorAll("[data-failure-decision]").forEach(function (radio) {
      radio.checked = false;
    });
  }

  function setFailureMatchState(point, status, message) {
    const panel = point.querySelector("[data-failure-match]");
    if (!panel) return;
    panel.hidden = !isFailureFollowUp(point);
    panel.dataset.matchStatus = status;
    panel.setAttribute("aria-busy", status === "loading" ? "true" : "false");
    const state = panel.querySelector("[data-match-state]");
    const results = panel.querySelector("[data-match-results]");
    const retry = panel.querySelector("[data-match-retry]");
    if (state) state.textContent = message;
    if (results) results.hidden = status !== "results";
    if (retry) retry.hidden = status !== "error";
  }

  function invalidateFailureMatches(point) {
    const request = failureRequests.get(point);
    if (request && request.controller) request.controller.abort();
    failureRequests.set(point, {
      requestId: request ? request.requestId + 1 : 1,
      controller: null
    });
    const timer = failureTimers.get(point);
    if (timer) clearTimeout(timer);
    failureTimers.delete(point);
    clearFailureChoices(point);
    setFailureMatchState(
      point,
      "idle",
      isFailureFollowUp(point)
        ? "Completa la clasificación para buscar coincidencias."
        : ""
    );
  }

  function formatFailureDate(value) {
    if (!value) return "Sin confirmaciones previas";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return "Fecha no disponible";
    return new Intl.DateTimeFormat("es-MX", {
      dateStyle: "medium",
      timeStyle: "short"
    }).format(date);
  }

  function renderFailureReports(point, reports) {
    const container = point.querySelector("[data-match-report-list]");
    if (!container) return;
    const identity = currentFailureIdentity(point);
    const groupName = "failure_report_" + identity.tipo + "_" + point.dataset.key;
    reports.forEach(function (report) {
      const label = document.createElement("label");
      label.className = "failure-report-option";
      const input = document.createElement("input");
      input.type = "radio";
      input.name = groupName;
      input.value = String(report.id);
      input.dataset.failureReport = "";
      const copy = document.createElement("span");
      const title = document.createElement("b");
      title.textContent = "Falla #" + report.id + " · " + report.titulo;
      const detail = document.createElement("small");
      detail.textContent = report.estatus + " · Última confirmación: " +
        formatFailureDate(report.ultima_confirmacion || report.fecha_reporte);
      copy.append(title, detail);
      label.append(input, copy);
      container.append(label);
    });
  }

  function selectedFailureContext(point) {
    return {
      reporteId: selectedValue(point, "[data-failure-report]"),
      decision: selectedValue(point, "[data-failure-decision]")
    };
  }

  function restoreFailureContext(point, context) {
    if (!context) return;
    const reports = Array.from(point.querySelectorAll("[data-failure-report]"));
    const previousReport = reports.find(function (radio) {
      return radio.value === context.reporteId;
    });
    if (previousReport) previousReport.checked = true;
    const decisions = Array.from(point.querySelectorAll("[data-failure-decision]"));
    const previousDecision = decisions.find(function (radio) {
      return radio.value === context.decision;
    });
    if (previousDecision && (context.decision === "DISTINTA" || previousReport)) {
      previousDecision.checked = true;
    }
  }

  async function loadFailureMatches(point, options) {
    if (!failureClassificationComplete(point)) {
      invalidateFailureMatches(point);
      updateWorkflow(point.closest("form"));
      return [];
    }

    const preserveSelection = Boolean(options && options.preserveSelection);
    const previousSelection = preserveSelection ? selectedFailureContext(point) : null;
    const previous = failureRequests.get(point);
    if (previous && previous.controller) previous.controller.abort();
    const requestId = previous ? previous.requestId + 1 : 1;
    const controller = new AbortController();
    failureRequests.set(point, { requestId: requestId, controller: controller });
    if (!preserveSelection) clearFailureChoices(point);
    setFailureMatchState(point, "loading", "Buscando fallas activas relacionadas…");
    updateWorkflow(point.closest("form"));

    const form = point.closest("form");
    const params = new URLSearchParams(currentFailureIdentity(point));
    try {
      const response = await fetch(form.dataset.failureMatchesUrl + "?" + params.toString(), {
        method: "GET",
        credentials: "same-origin",
        headers: { "X-Requested-With": "XMLHttpRequest" },
        signal: controller.signal
      });
      const payload = await response.json();
      const current = failureRequests.get(point);
      if (!current || current.requestId !== requestId) return [];
      if (!response.ok) {
        const details = payload.fields ? Object.values(payload.fields).flat().join(" ") : "";
        throw new Error(details || payload.error || "No fue posible buscar fallas relacionadas.");
      }
      const reports = Array.isArray(payload.reportes) ? payload.reportes : [];
      if (!reports.length) {
        if (preserveSelection) clearFailureChoices(point);
        setFailureMatchState(
          point,
          "empty",
          "No hay una falla activa igual. Al guardar se abrirá un reporte nuevo."
        );
      } else {
        if (preserveSelection) clearFailureChoices(point);
        renderFailureReports(point, reports);
        restoreFailureContext(point, previousSelection);
        setFailureMatchState(
          point,
          "results",
          reports.length === 1
            ? "Encontramos una falla activa. Elige qué ocurre hoy."
            : "Encontramos " + reports.length + " fallas activas. Elige una y confirma qué ocurre hoy."
        );
      }
      updateWorkflow(form);
      return reports;
    } catch (error) {
      if (error.name === "AbortError") return [];
      const current = failureRequests.get(point);
      if (!current || current.requestId !== requestId) return [];
      setFailureMatchState(
        point,
        "error",
        "No pudimos buscar coincidencias. Revisa tu conexión y usa Reintentar búsqueda."
      );
      updateWorkflow(form);
      return [];
    }
  }

  function scheduleFailureMatches(point) {
    const timer = failureTimers.get(point);
    if (timer) clearTimeout(timer);
    failureTimers.set(point, setTimeout(function () {
      failureTimers.delete(point);
      loadFailureMatches(point);
    }, 180));
  }

  function failureDecision(point) {
    if (!isFailureFollowUp(point)) return null;
    const panel = point.querySelector("[data-failure-match]");
    const status = panel ? panel.dataset.matchStatus : "idle";
    if (status === "empty") return { falla_decision: "AUTO", reporte_falla_id: "" };
    if (status !== "results") return null;
    const decision = {
      falla_decision: selectedValue(point, "[data-failure-decision]"),
      reporte_falla_id: selectedValue(point, "[data-failure-report]")
    };
    if (decision.falla_decision === "DISTINTA") decision.reporte_falla_id = "";
    return decision;
  }

  function pointHasEvidence(point) {
    const input = point.querySelector('input[type="file"]');
    return Boolean(input && input.files && input.files.length);
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

    if (!failureClassificationComplete(point)) return false;
    const panel = point.querySelector("[data-failure-match]");
    const matchStatus = panel ? panel.dataset.matchStatus : "idle";
    if (matchStatus === "empty") return pointHasEvidence(point);
    if (matchStatus !== "results") return false;
    const decision = failureDecision(point);
    if (!decision || !decision.falla_decision) return false;
    if (decision.falla_decision === "DISTINTA") return pointHasEvidence(point);
    if (!decision.reporte_falla_id) return false;
    return decision.falla_decision === "MISMA" || pointHasEvidence(point);
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
        if (!failureClassificationComplete(point)) {
          return "Completa el tipo, la categoría y el equipo o área de la falla.";
        }
        const panel = point.querySelector("[data-failure-match]");
        const matchStatus = panel ? panel.dataset.matchStatus : "idle";
        if (matchStatus === "loading") return "Espera a que termine la búsqueda de fallas activas.";
        if (matchStatus === "error") return "No se pudo buscar fallas activas. Usa Reintentar búsqueda.";
        if (matchStatus === "idle") return "Espera a que se busquen fallas activas relacionadas.";
        if (matchStatus === "empty" && !pointHasEvidence(point)) {
          return "Agrega una foto para abrir la nueva falla.";
        }
        const decision = failureDecision(point);
        if (matchStatus === "results" && (!decision || !decision.falla_decision)) {
          return "Elige qué ocurre hoy con este hallazgo.";
        }
        if (
          matchStatus === "results" &&
          decision.falla_decision !== "DISTINTA" &&
          !decision.reporte_falla_id
        ) {
          return "Elige la falla activa que corresponde a este hallazgo.";
        }
        if (decision && decision.falla_decision !== "MISMA" && !pointHasEvidence(point)) {
          return "Agrega una foto para documentar el cambio, el problema distinto o la corrección.";
        }
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
        if (!finding) invalidateFailureMatches(point);
        updateWorkflow(point.closest("form"));
      });
    });

    point.querySelectorAll("[data-resolution]").forEach(function (input) {
      input.addEventListener("change", function () {
        const fields = point.querySelector("[data-failure-fields]");
        if (fields) fields.hidden = input.value !== "SEGUIMIENTO";
        if (input.value === "SEGUIMIENTO") {
          loadFailureMatches(point);
        } else {
          invalidateFailureMatches(point);
        }
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
        invalidateFailureMatches(point);
        updateWorkflow(point.closest("form"));
      });
    }

    const category = point.querySelector("[data-category]");
    const asset = point.querySelector("[data-asset]");
    const area = point.querySelector("[data-area]");
    if (category) category.addEventListener("change", function () { loadFailureMatches(point); });
    if (asset) asset.addEventListener("change", function () { loadFailureMatches(point); });
    if (area) {
      area.addEventListener("input", function () {
        invalidateFailureMatches(point);
        scheduleFailureMatches(point);
      });
    }
    const retry = point.querySelector("[data-match-retry]");
    if (retry) {
      retry.addEventListener("click", function () {
        loadFailureMatches(point, { preserveSelection: true });
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
        invalidateFailureMatches(point);
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
            const decision = failureDecision(point);
            if (decision) {
              answer.falla_decision = decision.falla_decision;
              if (decision.reporte_falla_id) {
                answer.reporte_falla_id = decision.reporte_falla_id;
              }
            }
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
      if (form.dataset.submitting === "true" || form.dataset.saved === "true") return;
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
      form.dataset.submitting = "true";
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
        if (response.status === 409) {
          const followUpPoints = points.filter(isFailureFollowUp);
          let conflicted = payload.punto_clave
            ? form.querySelector(
              '[data-review-point][data-key="' + CSS.escape(payload.punto_clave) + '"]'
            )
            : null;
          if (conflicted && isFailureFollowUp(conflicted)) {
            await loadFailureMatches(conflicted);
          } else {
            conflicted = null;
            const conflictIds = new Set((payload.existing_reports || []).map(function (report) {
              return String(report.id);
            }));
            for (const point of followUpPoints) {
              const reports = await loadFailureMatches(point, { preserveSelection: true });
              if (!conflicted && reports.some(function (report) {
                return conflictIds.has(String(report.id));
              })) {
                conflicted = point;
              }
            }
            if (!conflicted) {
              conflicted = followUpPoints.find(function (point) {
                const panel = point.querySelector("[data-failure-match]");
                return panel && panel.dataset.matchStatus === "results";
              }) || followUpPoints[0];
            }
          }
          if (conflicted) {
            const section = conflicted.closest("[data-review-section]");
            showSection(form, Number(section.dataset.sectionIndex), true);
            conflicted.scrollIntoView({ block: "center", behavior: reduceMotion ? "auto" : "smooth" });
            const usefulControl = conflicted.querySelector("[data-failure-report]") ||
              conflicted.querySelector("[data-failure-decision]") ||
              conflicted.querySelector("[data-category]");
            if (usefulControl) usefulControl.focus({ preventScroll: true });
          }
          showToast(
            payload.error || "La falla cambió mientras guardabas. Revisa la coincidencia y vuelve a guardar.",
            "warning"
          );
          return;
        }
        if (!response.ok) {
          const details = payload.fields ? Object.values(payload.fields).flat().join(" ") : "";
          throw new Error(details || payload.error || "No fue posible guardar.");
        }
        const failures = payload.reporte_falla_ids || [];
        form.dataset.saved = "true";
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
      } finally {
        form.dataset.submitting = "false";
        button.disabled = false;
        button.innerHTML = original;
      }
    });
  });
})();
