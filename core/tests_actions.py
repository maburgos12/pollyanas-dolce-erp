from pathlib import Path

from django.test import SimpleTestCase


ROOT = Path(__file__).resolve().parent.parent


class ERPActionContractTests(SimpleTestCase):
    def test_base_carga_toast_y_helper_global_accesible(self):
        html = (ROOT / "templates" / "base.html").read_text(encoding="utf-8")

        self.assertIn('id="erp-toast-region"', html)
        self.assertIn('aria-live="polite"', html)
        self.assertIn('aria-atomic="false"', html)
        self.assertIn("js/erp_actions.js", html)
        self.assertIn("data-toast-type", html)

    def test_helper_bloquea_solo_submitter_y_previene_doble_envio(self):
        js = (ROOT / "static" / "js" / "erp_actions.js").read_text(encoding="utf-8")

        self.assertIn('form.dataset.actionPending === "true"', js)
        self.assertIn('form.dataset.actionPending = "true"', js)
        self.assertIn("submitter.disabled = true", js)
        self.assertNotIn('querySelectorAll("button")', js)
        self.assertIn('headers: { "Accept": "application/json"', js)
        self.assertIn('form.getAttribute("action") || window.location.href', js)
        self.assertIn("target.outerHTML = payload.html", js)
        self.assertIn("showToast", js)

    def test_toast_respeta_safe_area_y_movimiento_reducido(self):
        css = (ROOT / "static" / "css" / "styles.css").read_text(encoding="utf-8")

        self.assertIn(".erp-toast-region", css)
        self.assertIn("env(safe-area-inset-bottom", css)
        self.assertIn("@media (prefers-reduced-motion: reduce)", css)
        self.assertIn(".erp-toast", css)

    def test_modal_compartido_es_opt_in_y_restaura_foco(self):
        html = (ROOT / "templates" / "base.html").read_text(encoding="utf-8")
        js = (ROOT / "static" / "js" / "erp_actions.js").read_text(encoding="utf-8")

        self.assertIn('id="erp-confirm-dialog"', html)
        self.assertIn('aria-modal="true"', html)
        self.assertIn('data-confirm-cancel', html)
        self.assertIn('event.key === "Escape"', js)
        self.assertIn("confirmTrigger.focus()", js)
        self.assertIn('form.dataset.confirmMessage', js)
        self.assertIn("bind(document)", js)
