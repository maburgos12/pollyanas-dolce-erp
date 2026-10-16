from dataclasses import replace
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase

from rrhh.models import Empleado, NominaLinea, NominaPeriodo
from rrhh.services.lista_raya import EmpleadoListaRaya, ListaRayaParseResult, importar_lista_raya_nomina


class ListaRayaProteccionIdentidadTests(TestCase):
    def setUp(self):
        self.empleado = Empleado.objects.create(
            codigo="356", nombre="PERSONA ADMINISTRATIVA", area="ADMINISTRACION",
            departamento="ADMINISTRACION", puesto="Auxiliar de Administración",
            rfc="AAAA010101AAA", curp="AAAA010101HSLAAA01", nss="12345678901",
        )
        self.row = EmpleadoListaRaya(
            codigo="356", nombre=self.empleado.nombre, area="PRODUCCION",
            rfc=self.empleado.rfc, curp=self.empleado.curp, nss=self.empleado.nss,
            fecha_ingreso=date(2026, 9, 1), salario_diario=Decimal("300"),
            sdi=Decimal("300"), sbc=Decimal("300"), dias_pagados=Decimal("15"),
            horas_trabajadas=Decimal("120"), horas_dia=Decimal("8"),
            horas_extra=Decimal("0"), ausencias=Decimal("0"), incapacidades=Decimal("0"),
            total_percepciones=Decimal("4500"), total_deducciones=Decimal("0"),
            neto=Decimal("4500"), conceptos=[],
        )

    def importar(self, row, **kwargs):
        result = ListaRayaParseResult(
            source_path="fake.xls", source_hash="identity-test", empresa="Test",
            fecha_inicio=date(2026, 9, 1), fecha_fin=date(2026, 9, 15),
            periodo_numero="17", empleados=[row], total_empleados_reportado=1,
            total_percepciones_reportado=Decimal("4500"),
            total_deducciones_reportado=Decimal("0"), total_neto_reportado=Decimal("4500"),
        )
        with patch("rrhh.services.lista_raya.parse_lista_raya_xls", return_value=result):
            return importar_lista_raya_nomina("fake.xls", **kwargs)

    def test_no_sustituye_identidad_ni_lineas_al_reimportar(self):
        self.importar(self.row)
        linea = NominaLinea.objects.get()
        for campo, valor in (("nombre", "OTRA PERSONA"), ("rfc", "BBBB010101BBB"),
                             ("curp", "BBBB010101HSLBBB02"), ("nss", "98765432109")):
            with self.subTest(campo=campo):
                with self.assertRaisesRegex(ValueError, "356.*identidad"):
                    self.importar(replace(self.row, **{campo: valor}), replace=True)
                self.empleado.refresh_from_db()
                self.assertEqual(self.empleado.nombre, "PERSONA ADMINISTRATIVA")
                self.assertTrue(NominaLinea.objects.filter(pk=linea.pk).exists())
                self.assertEqual(NominaPeriodo.objects.count(), 1)

    def test_conserva_area_rrhh_y_acepta_formato_equivalente(self):
        self.importar(replace(self.row, nombre=" persona   administrativa ", rfc="AAAA-010101-AAA"))
        self.empleado.refresh_from_db()
        self.assertEqual(self.empleado.area, "ADMINISTRACION")
        self.assertEqual(self.empleado.departamento, "ADMINISTRACION")
        self.assertEqual(NominaLinea.objects.get().empleado_id, self.empleado.pk)

    def test_completa_area_vacia(self):
        self.empleado.area = ""
        self.empleado.save(update_fields=["area"])
        self.importar(self.row)
        self.empleado.refresh_from_db()
        self.assertEqual(self.empleado.area, "PRODUCCION")
