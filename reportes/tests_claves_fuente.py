"""Mover una regla con update() deja su clave canónica apuntando al rubro viejo."""

from io import StringIO

from django.core.management import call_command
from django.test import TestCase

from reportes.models import (
    AreaPresupuesto,
    ReglaFuenteRubro,
    RubroPresupuesto,
)
from core.models import Sucursal


class ClavesFuenteTests(TestCase):
    def setUp(self):
        self.area = AreaPresupuesto.objects.create(codigo="pruebas", nombre="Pruebas")
        self.vieja = Sucursal.objects.create(codigo="VIEJA", nombre="Sucursal Vieja")
        self.nueva = Sucursal.objects.create(codigo="NUEVA", nombre="Sucursal Nueva")
        self.rubro_viejo = RubroPresupuesto.objects.create(
            concepto="Renta", area=self.area, sucursal=self.vieja, tipo=RubroPresupuesto.TIPO_EGRESO
        )
        self.rubro_nuevo = RubroPresupuesto.objects.create(
            concepto="Renta", area=self.area, sucursal=self.nueva, tipo=RubroPresupuesto.TIPO_EGRESO
        )
        self.regla = ReglaFuenteRubro.objects.create(
            rubro=self.rubro_viejo,
            tipo_fuente=ReglaFuenteRubro.FUENTE_OBLIGACION_GASTO,
            activa=True,
        )

    def test_update_deja_la_clave_del_rubro_anterior(self):
        clave_original = self.regla.clave_fuente
        ReglaFuenteRubro.objects.filter(pk=self.regla.pk).update(rubro=self.rubro_nuevo)
        movida = ReglaFuenteRubro.objects.get(pk=self.regla.pk)
        self.assertEqual(movida.clave_fuente, clave_original)
        self.assertNotEqual(movida.clave_fuente, movida.calcular_clave_fuente())

    def test_el_comando_la_repara(self):
        ReglaFuenteRubro.objects.filter(pk=self.regla.pk).update(rubro=self.rubro_nuevo)
        call_command("recalcular_claves_fuente", "--apply", stdout=StringIO())
        reparada = ReglaFuenteRubro.objects.get(pk=self.regla.pk)
        self.assertEqual(reparada.clave_fuente, reparada.calcular_clave_fuente())

    def test_save_si_la_recalcula(self):
        self.regla.rubro = self.rubro_nuevo
        self.regla.save()
        self.assertEqual(self.regla.clave_fuente, self.regla.calcular_clave_fuente())

    def test_es_repetible_y_no_toca_las_correctas(self):
        antes = ReglaFuenteRubro.objects.get(pk=self.regla.pk).clave_fuente
        call_command("recalcular_claves_fuente", "--apply", stdout=StringIO())
        self.assertEqual(ReglaFuenteRubro.objects.get(pk=self.regla.pk).clave_fuente, antes)
