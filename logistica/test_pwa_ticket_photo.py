from pathlib import Path

from django.conf import settings
from django.test import SimpleTestCase


class LogisticaPwaTicketPhotoTemplateTests(SimpleTestCase):
    def _source(self):
        template_path = Path(settings.BASE_DIR) / "logistica" / "templates" / "logistica" / "pwa.html"
        return template_path.read_text(encoding="utf-8")

    def test_combustible_ticket_photo_can_be_changed_before_upload(self):
        source = self._source()

        self.assertIn("function setFotoCargaCombustible(input)", source)
        self.assertIn("function limpiarFotoCargaCombustible()", source)
        self.assertIn('draft.carga_foto_ticket = null;', source)
        self.assertIn("Cambiar foto", source)
        self.assertIn("Quitar foto", source)
        self.assertIn('payload.append("foto_ticket", draft.carga_foto_ticket);', source)

    def test_combustible_no_bloquea_guardado_por_falta_de_gps(self):
        source = self._source()

        self.assertIn("function faltantesCargaCombustible(draft)", source)
        self.assertNotIn('draft.carga_geoStatus === "ready" &&', source)
        self.assertIn("Se guardará sin ubicación", source)
        self.assertIn(
            'if (draft.carga_latitud && draft.carga_longitud) {',
            source,
        )

    def test_acciones_criticas_validan_al_pulsar_en_vez_de_quedar_inertes(self):
        source = self._source()

        self.assertIn('faltantes.join(", ")', source)
        self.assertIn(
            '<button id="guardar_carga_combustible_btn" class="primary-btn" '
            'type="button" onclick="guardarCargaCombustible()"',
            source,
        )
        self.assertNotIn(
            '${puedeGuardarCargaCombustible() ? "" : "disabled"}',
            source,
        )
        self.assertNotIn(
            ".toggleAttribute('disabled', !puedeGuardarCargaCombustible())",
            source,
        )
        self.assertIn(
            '<button class="primary-btn" type="submit">Cerrar turno</button>',
            source,
        )

    def test_cierre_conserva_km_gas_foto_y_carga_como_requisitos(self):
        source = self._source()

        self.assertIn('faltantes.push("nivel de gasolina de llegada")', source)
        self.assertIn('faltantes.push("foto del tablero de llegada")', source)
        self.assertIn('faltantes.push("registrar la carga de gasolina del recorrido")', source)
        self.assertIn("if (!kmLlegada)", source)

    def test_version_de_cache_publica_el_flujo_corregido(self):
        source = self._source()
        sw_path = Path(settings.BASE_DIR) / "logistica" / "static" / "logistica" / "pwa" / "sw.js"
        sw_source = sw_path.read_text(encoding="utf-8")

        self.assertIn("pollyanas-logistica-pwa-v89-turno-combustible-seguro", sw_source)
        self.assertIn("route-control-v89-turno-combustible-seguro", source)
