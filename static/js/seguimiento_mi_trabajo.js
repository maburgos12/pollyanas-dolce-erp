(function () {
  "use strict";

  const root = document.querySelector(".seguimiento-shell");
  if (!root) return;

  const cards = Array.from(root.querySelectorAll(".seg-work-card"));
  const bucketButtons = Array.from(root.querySelectorAll("[data-work-bucket]"));
  const callout = root.querySelector("[data-priority-callout]");
  const calloutCopy = root.querySelector("[data-priority-copy]");

  function asNumber(card, name) {
    return Number(card.dataset[name] || 0);
  }

  function bucketFor(card) {
    if (card.dataset.isClosed === "true") return "history";
    if (card.dataset.status === "EN_REVISION") return "review";
    const total = asNumber(card, "checklistTotal");
    const done = asNumber(card, "checklistDone");
    if (total > 0 && done >= total) return "ready";
    return "attention";
  }

  function priorityFor(card) {
    if (card.dataset.overdue === "true") return 0;
    if (card.dataset.newDgResponse === "true") return 1;
    if (!card.dataset.due) return 4;
    const hours = (new Date(card.dataset.due).getTime() - Date.now()) / 3600000;
    return hours <= 24 ? 1 : 2;
  }

  function closeCard(card) {
    const button = card.querySelector(".seg-work-card-toggle");
    const detail = card.querySelector(".seg-work-card-detail");
    if (!button || !detail) return;
    button.setAttribute("aria-expanded", "false");
    detail.hidden = true;
    card.classList.remove("is-open");
  }

  function openCard(card) {
    cards.forEach((candidate) => {
      if (candidate !== card) closeCard(candidate);
    });
    const button = card.querySelector(".seg-work-card-toggle");
    const detail = card.querySelector(".seg-work-card-detail");
    if (!button || !detail) return;
    button.setAttribute("aria-expanded", "true");
    detail.hidden = false;
    card.classList.add("is-open");
  }

  cards.forEach((card) => {
    card.dataset.bucket = bucketFor(card);
    card.dataset.priority = String(priorityFor(card));
    const button = card.querySelector(".seg-work-card-toggle");
    if (button) {
      button.addEventListener("click", () => {
        if (card.classList.contains("is-open")) closeCard(card);
        else openCard(card);
      });
    }
  });

  function selectBucket(bucket) {
    bucketButtons.forEach((button) => {
      const active = button.dataset.workBucket === bucket;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });
    const visibleCards = cards
      .filter((card) => card.dataset.bucket === bucket)
      .sort((a, b) => Number(a.dataset.priority) - Number(b.dataset.priority));
    cards.forEach((card) => {
      const visible = card.dataset.bucket === bucket;
      card.hidden = !visible;
      if (!visible) closeCard(card);
    });
    visibleCards.forEach((card) => card.parentElement.appendChild(card));
    if (callout) {
      const first = visibleCards[0];
      const urgent = bucket === "attention" && first && Number(first.dataset.priority) <= 1;
      callout.hidden = !urgent;
      if (urgent && calloutCopy) {
        calloutCopy.textContent = first.dataset.overdue === "true"
          ? "Está vencido y requiere tu atención"
          : "Vence pronto o tiene una respuesta nueva";
      }
    }
    if (visibleCards[0]) openCard(visibleCards[0]);
  }

  ["attention", "ready", "review", "history"].forEach((bucket) => {
    const count = cards.filter((card) => card.dataset.bucket === bucket).length;
    const target = root.querySelector(`[data-work-count="${bucket}"]`);
    if (target) target.textContent = String(count);
  });

  bucketButtons.forEach((button) => {
    button.addEventListener("click", () => selectBucket(button.dataset.workBucket));
  });

  selectBucket("attention");
})();
