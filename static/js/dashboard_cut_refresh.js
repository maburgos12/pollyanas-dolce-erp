(function () {
  "use strict";

  var form = document.querySelector("form[data-cut-refresh-poll]");
  if (!form || !form.querySelector('[aria-busy="true"]')) {
    return;
  }

  var refreshAfterMs = Number(form.dataset.refreshAfterMs || 10000);
  if (!Number.isFinite(refreshAfterMs) || refreshAfterMs < 3000) {
    refreshAfterMs = 10000;
  }

  window.setTimeout(function () {
    window.location.reload();
  }, refreshAfterMs);
})();
