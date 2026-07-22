(function () {
  "use strict";

  const dialog = document.querySelector("[data-panel-dialog]");
  const openButton = document.querySelector("[data-panel-dialog-open]");

  if (dialog && openButton) {
    openButton.addEventListener("click", function () {
      if (typeof dialog.showModal === "function") dialog.showModal();
      else dialog.setAttribute("open", "");
    });

    dialog.addEventListener("click", function (event) {
      if (event.target === dialog) dialog.close();
    });
  }

  const personRows = Array.from(document.querySelectorAll(".panel-dg-person-row"));
  personRows.forEach(function (row) {
    row.addEventListener("toggle", function () {
      if (!row.open) return;
      personRows.forEach(function (candidate) {
        if (candidate !== row) candidate.open = false;
      });
    });
  });
})();
