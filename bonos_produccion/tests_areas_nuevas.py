"""Preparación y cuartos fríos como áreas propias del bono de producción."""
from decimal import Decimal

from django.test import TestCase

from rrhh.models import Empleado

from .models import (
    AREA_ARMADO,
    AREA_CRUCERO,
    AREA_CUARTOS_FRIOS,
    AREA_LOGISTICA,
    AREA_PREPARACION,
    AREA_PRODUCCION,
    AREAS_PRODUCCION,
    ConfigBonoArea,
    ConfigBonoPeriodo,
    area_bono_produccion_empleado,
    normalizar_area_produccion,
)


class AreasNuevasTests(TestCase):
    def setUp(self):
        self.periodo = ConfigBonoPeriodo.objects.create(
            mes=9, anio=2026,
            monto_crucero=Decimal("300.00"),
            monto_logistica=Decimal("250.00"),
            monto_preparacion=Decimal("300.00"),
            monto_cuartos_frios=Decimal("250.00"),
        )

    def test_las_areas_nuevas_aparecen_en_el_catalogo(self):
        etiquetas = dict(AREAS_PRODUCCION)
        self.assertEqual(etiquetas[AREA_PREPARACION], "Preparación")
        self.assertEqual(etiquetas[AREA_CUARTOS_FRIOS], "Cuartos fríos")

    def test_un_area_nueva_no_puede_pagar_cero(self):
        # get_monto_area devuelve 0.00 para lo desconocido: sin su campo de
        # monto, estrenar un área dejaría el bono en cero sin avisar.
        self.assertEqual(self.periodo.get_monto_area(AREA_PREPARACION), Decimal("300.00"))
        self.assertEqual(self.periodo.get_monto_area(AREA_CUARTOS_FRIOS), Decimal("250.00"))

    def test_preparacion_paga_igual_que_el_area_de_origen(self):
        # Laiza y Sara vienen de Crucero; Christian, de embetunado.
        self.assertEqual(
            self.periodo.get_monto_area(AREA_PREPARACION),
            self.periodo.get_monto_area(AREA_CRUCERO),
        )

    def test_cuartos_frios_paga_igual_que_logistica(self):
        self.assertEqual(
            self.periodo.get_monto_area(AREA_CUARTOS_FRIOS),
            self.periodo.get_monto_area(AREA_LOGISTICA),
        )

    def test_cuartos_frios_no_se_mide_por_piezas(self):
        # Resguarda inventario: comparte las reglas de logística.
        reglas = ConfigBonoArea.defaults_for_area(AREA_CUARTOS_FRIOS)
        self.assertFalse(reglas["usa_produccion"])
        self.assertEqual(reglas["pct_produccion"], Decimal("0.00"))
        self.assertEqual(reglas["pct_asistencia"], Decimal("50.00"))

    def test_preparacion_si_se_mide_por_piezas(self):
        reglas = ConfigBonoArea.defaults_for_area(AREA_PREPARACION)
        self.assertTrue(reglas["usa_produccion"])
        self.assertEqual(reglas["pct_produccion"], Decimal("65.00"))

    def test_el_area_sale_del_puesto_operativo(self):
        for area_op, esperada in [
            ("PREPARACION", AREA_PREPARACION),
            ("CUARTOS_FRIOS", AREA_CUARTOS_FRIOS),
            ("ARMADO", AREA_ARMADO),
        ]:
            empleado = Empleado.objects.create(
                codigo=f"T{area_op[:6]}", nombre=area_op,
                departamento=Empleado.DEP_PRODUCCION, puesto_operativo=area_op,
            )
            self.assertEqual(area_bono_produccion_empleado(empleado), esperada)

    def test_normaliza_acentos_y_espacios(self):
        for texto in ("preparación", "PREPARACIONES", " Preparacion "):
            self.assertEqual(normalizar_area_produccion(texto), AREA_PREPARACION)
        for texto in ("cuartos fríos", "CUARTOS FRIOS", "cuartos_frios"):
            self.assertEqual(normalizar_area_produccion(texto), AREA_CUARTOS_FRIOS)

    def test_crucero_sigue_siendo_valida_para_el_historico(self):
        # Los bonos ya pagados apuntan ahí; quitarla los dejaría huérfanos.
        self.assertIn(AREA_CRUCERO, dict(AREAS_PRODUCCION))
        self.assertEqual(self.periodo.get_monto_area(AREA_CRUCERO), Decimal("300.00"))

    def test_toda_area_del_catalogo_se_puede_capturar_y_pagar(self):
        # Dos mapas distintos deciden el importe: el de la pantalla y el del
        # cálculo. Un área que falte en cualquiera se muestra pero paga cero.
        from .views_html import AREA_AMOUNT_FIELDS

        for area, label in AREAS_PRODUCCION:
            self.assertIn(area, AREA_AMOUNT_FIELDS, f"{label} no es editable")
            self.assertTrue(
                hasattr(self.periodo, AREA_AMOUNT_FIELDS[area]),
                f"{label} apunta a un campo inexistente",
            )
            self.periodo.get_monto_area(area)  # no debe reventar

    def test_asegurar_reglas_crea_las_dos_areas_nuevas(self):
        self.periodo.asegurar_reglas_area()
        areas = set(self.periodo.reglas_area.values_list("area", flat=True))
        self.assertIn(AREA_PREPARACION, areas)
        self.assertIn(AREA_CUARTOS_FRIOS, areas)
        # Y no rompe las que ya existían.
        self.assertIn(AREA_PRODUCCION, areas)
        self.assertIn(AREA_LOGISTICA, areas)
