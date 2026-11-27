from pathlib import Path
import subprocess

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
    def test_consulta_point_no_store_no_reutiliza_respuesta_202_cacheada(self):
        script = r"""
const fs = require('node:fs'), vm = require('node:vm'), assert = require('node:assert/strict');
let handler, networkCalls = 0, cacheReads = 0;
const sandbox = {
  URL, self: {addEventListener: (name, fn) => {if (name === 'fetch') handler = fn;}},
  caches: {match: async () => {cacheReads++; return {status: 202};}},
  fetch: async () => ({status: ++networkCalls === 1 ? 202 : 200})
};
vm.runInNewContext(fs.readFileSync(process.argv[1], 'utf8'), sandbox);
async function consult(url) {
  let result;
  handler({request: {url, method: 'GET', mode: 'cors', cache: 'no-store'}, respondWith: p => result = p});
  return (await result).status;
}
(async () => {
  const url = 'https://erp.example/logistica/rutas/1/?carga_point_estado=7';
  assert.equal(await consult(url), 202);
  assert.equal(await consult(url), 200);
  assert.equal(await consult('https://erp.example/logistica/rutas/1/'), 200);
  assert.equal(cacheReads, 0);
  assert.equal(networkCalls, 3);
})().catch(error => {console.error(error); process.exitCode = 1;});
"""
        result = subprocess.run(
            ["node", "-e", script, str(PWA_SERVICE_WORKER)],
            capture_output=True, text=True, timeout=20,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

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
        self.assertIn("route-control-v105-combustible-sin-turno", pwa_html)
        self.assertIn("pollyanas-logistica-pwa-v105-combustible-sin-turno", service_worker)
