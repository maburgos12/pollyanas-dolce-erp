(function () {
  function applyDataStyles(root) {
    var scope = root && root.querySelectorAll ? root : document;
    var widthNodes = scope.querySelectorAll("[data-pd-width]");
    widthNodes.forEach(function (node) {
      var value = node.getAttribute("data-pd-width");
      if (!value || node.getAttribute("data-pd-width-applied") === value) return;
      var cssVar = node.getAttribute("data-pd-css-var");
      if (cssVar) {
        node.style.setProperty(cssVar, value);
      } else {
        node.style.width = value;
      }
      node.setAttribute("data-pd-width-applied", value);
    });
  }

  window.PollyanaOpsUI = window.PollyanaOpsUI || {};
  window.PollyanaOpsUI.applyDataStyles = applyDataStyles;

  document.addEventListener("DOMContentLoaded", function () {
    applyDataStyles(document);

    if (!("MutationObserver" in window)) return;
    var observer = new MutationObserver(function (mutations) {
      mutations.forEach(function (mutation) {
        mutation.addedNodes.forEach(function (node) {
          if (node.nodeType !== 1) return;
          if (node.matches && node.matches("[data-pd-width]")) {
            applyDataStyles(node.parentElement || document);
          } else {
            applyDataStyles(node);
          }
        });
      });
    });
    observer.observe(document.body, { childList: true, subtree: true });
  });
})();
