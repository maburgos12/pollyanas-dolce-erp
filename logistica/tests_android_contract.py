from pathlib import Path

from django.test import SimpleTestCase


PROJECT_DIR = Path(__file__).resolve().parents[1]
PWA_TEMPLATE = PROJECT_DIR / "logistica" / "templates" / "logistica" / "pwa.html"
PWA_SERVICE_WORKER = PROJECT_DIR / "logistica" / "static" / "logistica" / "pwa" / "sw.js"
ANDROID_APP = PROJECT_DIR / "android" / "logistics-corporate" / "app"
ANDROID_MAIN = (
    ANDROID_APP
    / "src"
    / "main"
    / "java"
    / "com"
    / "pollyanas"
    / "logistics"
    / "MainActivity.kt"
)
ANDROID_BRIDGE = (
    ANDROID_APP
    / "src"
    / "main"
    / "java"
    / "com"
    / "pollyanas"
    / "logistics"
    / "web"
    / "NativeTrackingBridge.kt"
)
ANDROID_BUILD = ANDROID_APP / "build.gradle.kts"


class AndroidPwaContractTests(SimpleTestCase):
    def test_pwa_invoca_el_puente_nativo_sin_romper_el_fallback_web(self):
        pwa_html = PWA_TEMPLATE.read_text(encoding="utf-8")

        self.assertIn("window.PollyanasNative?.storeTokens", pwa_html)
        self.assertIn("window.PollyanasNative?.startRouteTracking", pwa_html)
        self.assertIn("window.PollyanasNative?.stopRouteTracking", pwa_html)
        self.assertIn("window.PollyanasNative?.clearSession", pwa_html)
        self.assertIn("function stopRutaAutoTracking({ stopNative = true } = {})", pwa_html)
        self.assertIn(
            'window.addEventListener("pagehide", () => stopRutaAutoTracking({ stopNative: false }));',
            pwa_html,
        )
        self.assertIn("navigator.geolocation.getCurrentPosition", pwa_html)
        self.assertIn('tracking_origen: "automatico_pwa"', pwa_html)

    def test_android_expone_el_contrato_y_soporta_camara_y_telefono(self):
        main_activity = ANDROID_MAIN.read_text(encoding="utf-8")
        native_bridge = ANDROID_BRIDGE.read_text(encoding="utf-8")

        for method in ("storeTokens", "startRouteTracking", "stopRouteTracking", "clearSession"):
            self.assertIn(f"fun {method}", native_bridge)
        self.assertIn("onShowFileChooser", main_activity)
        self.assertIn("FileChooserParams", main_activity)
        self.assertIn("Intent.ACTION_DIAL", main_activity)
        self.assertIn('uri.scheme == "tel"', main_activity)
        self.assertIn("onGeolocationPermissionsShowPrompt", main_activity)

    def test_android_y_service_worker_tienen_version_nueva_coherente(self):
        build_gradle = ANDROID_BUILD.read_text(encoding="utf-8")
        pwa_html = PWA_TEMPLATE.read_text(encoding="utf-8")
        service_worker = PWA_SERVICE_WORKER.read_text(encoding="utf-8")

        self.assertIn("versionCode = 3", build_gradle)
        self.assertIn('versionName = "1.0.3"', build_gradle)
        self.assertIn("route-control-v88-flota-gastos-completos", pwa_html)
        self.assertIn("pollyanas-logistica-pwa-v88-flota-gastos-completos", service_worker)
