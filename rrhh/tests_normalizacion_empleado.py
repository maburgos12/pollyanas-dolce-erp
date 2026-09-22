"""El área y el nivel entran limpios, sin importar cómo se tecleen."""
from django.test import TestCase

from reportes.clasificacion_nomina import (
    DESTINO_CEDIS, DESTINO_ADMINISTRACION, DESTINO_PRODUCCION, destino_de,
)
from rrhh.models import Empleado


class NormalizacionEmpleadoTests(TestCase):
    def _empleado(self, **kwargs):
        return Empleado.objects.create(codigo=kwargs.pop("codigo"), nombre="X", **kwargs)

    def test_el_area_se_guarda_sin_acentos_ni_minusculas(self):
        e = self._empleado(codigo="N1", puesto_operativo="Preparación")
        e.refresh_from_db()
        self.assertEqual(e.puesto_operativo, "PREPARACION")

    def test_el_nivel_se_guarda_normalizado(self):
        e = self._empleado(codigo="N2", nivel_organizacional="jefatura")
        e.refresh_from_db()
        self.assertEqual(e.nivel_organizacional, "JEFATURA")

    def test_variantes_de_captura_terminan_en_el_mismo_valor(self):
        valores = ["Cuartos Fríos", "CUARTOS FRIOS", "  cuartos  frios  "]
        guardados = set()
        for i, valor in enumerate(valores):
            e = self._empleado(codigo=f"N3{i}", puesto_operativo=valor)
            e.refresh_from_db()
            guardados.add(e.puesto_operativo)
        # Tres formas de teclearlo, un solo valor en la base.
        self.assertEqual(guardados, {"CUARTOS FRIOS"})

    def test_el_destino_ya_no_depende_de_como_se_teclee(self):
        # Antes, «Preparación» con acento no empataba y caía en producción
        # por omisión en vez de por dato.
        e = self._empleado(codigo="N4", puesto_operativo="Cuartos_Fríos",
                           departamento=Empleado.DEP_PRODUCCION)
        e.refresh_from_db()
        self.assertEqual(
            destino_de(e.nivel_organizacional, e.puesto_operativo), DESTINO_CEDIS,
        )

    def test_el_jefe_se_detecta_aunque_lo_escriban_en_minusculas(self):
        e = self._empleado(codigo="N5", nivel_organizacional="Jefatura",
                           departamento=Empleado.DEP_PRODUCCION)
        e.refresh_from_db()
        self.assertEqual(
            destino_de(e.nivel_organizacional, e.puesto_operativo), DESTINO_ADMINISTRACION,
        )

    def test_vacio_sigue_siendo_vacio(self):
        e = self._empleado(codigo="N6")
        e.refresh_from_db()
        self.assertEqual(e.puesto_operativo, "")
        self.assertEqual(destino_de(e.nivel_organizacional, e.puesto_operativo), DESTINO_PRODUCCION)
