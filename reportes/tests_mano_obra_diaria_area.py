from datetime import date
from decimal import Decimal
from uuid import uuid4

from django.test import TestCase

from maestros.models import Insumo
from pos_bridge.models import PointBranch, PointProductionLine
from recetas.models import Receta
from reportes.models import RecetaAreaProduccion
from reportes.services_mano_obra_diaria_area import (
    area_produccion_empleado,
    calcular_costo_diario_area,
    costo_mano_obra_diario_receta,
    nomina_diaria_area,
    unidades_area_dia,
)
from rrhh.models import Empleado, NominaLinea, NominaPeriodo


class AreaProduccionEmpleadoTests(TestCase):
    def _empleado(self, *, puesto_operativo):
        return Empleado.objects.create(
            codigo=f"E-{uuid4().hex[:6]}",
            nombre="Empleado Test",
            departamento=Empleado.DEP_PRODUCCION,
            puesto_operativo=puesto_operativo,
            fecha_ingreso=date(2026, 1, 1),
            salario_diario=Decimal("400.00"),
        )

    def test_clasifica_hornos_armado_embetunado_directo(self):
        self.assertEqual(area_produccion_empleado(self._empleado(puesto_operativo="HORNOS")), "HORNOS")
        self.assertEqual(area_produccion_empleado(self._empleado(puesto_operativo="ARMADO")), "ARMADO")
        self.assertEqual(area_produccion_empleado(self._empleado(puesto_operativo="EMBETUNADO")), "EMBETUNADO")

    def test_otro_puesto_no_clasifica(self):
        self.assertIsNone(area_produccion_empleado(self._empleado(puesto_operativo="ENVIO_SUCURSAL")))


class NominaDiariaAreaTests(TestCase):
    def _empleado(self, *, puesto_operativo):
        return Empleado.objects.create(
            codigo=f"E-{uuid4().hex[:6]}",
            nombre="Empleado Test",
            departamento=Empleado.DEP_PRODUCCION,
            puesto_operativo=puesto_operativo,
            fecha_ingreso=date(2026, 1, 1),
            salario_diario=Decimal("400.00"),
        )

    def _periodo(self, *, inicio, fin, estatus=NominaPeriodo.ESTATUS_CERRADA):
        return NominaPeriodo.objects.create(fecha_inicio=inicio, fecha_fin=fin, estatus=estatus)

    def test_prorratea_nomina_del_area_entre_dias_laborables(self):
        empleado = self._empleado(puesto_operativo="HORNOS")
        otro_empleado = self._empleado(puesto_operativo="ARMADO")
        periodo = self._periodo(inicio=date(2026, 5, 1), fin=date(2026, 5, 14))  # 14 dias
        NominaLinea.objects.create(periodo=periodo, empleado=empleado, salario_base=Decimal("4200.00"))
        NominaLinea.objects.create(periodo=periodo, empleado=otro_empleado, salario_base=Decimal("1000.00"))

        # 14 dias * 6/7 = 12 dias laborables; 4200/12 = 350.00
        resultado = nomina_diaria_area(date(2026, 5, 5), "HORNOS")

        self.assertEqual(resultado, Decimal("350.00"))

    def test_sin_periodo_vigente_retorna_none(self):
        self.assertIsNone(nomina_diaria_area(date(2026, 6, 1), "HORNOS"))

    def test_periodo_en_borrador_no_cuenta(self):
        empleado = self._empleado(puesto_operativo="HORNOS")
        periodo = self._periodo(inicio=date(2026, 5, 1), fin=date(2026, 5, 14), estatus=NominaPeriodo.ESTATUS_BORRADOR)
        NominaLinea.objects.create(periodo=periodo, empleado=empleado, salario_base=Decimal("4200.00"))

        self.assertIsNone(nomina_diaria_area(date(2026, 5, 5), "HORNOS"))


class UnidadesAreaDiaTests(TestCase):
    def setUp(self):
        self.branch = PointBranch.objects.create(external_id=f"B-{uuid4().hex[:6]}", name="Sucursal Test")

    def _receta(self, *, nombre, familia):
        return Receta.objects.create(
            nombre=nombre,
            codigo_point=f"COD-{uuid4().hex[:6]}",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            modo_costeo=Receta.MODO_COSTEO_FABRICADO,
            familia=familia,
            hash_contenido=f"h-{uuid4()}",
        )

    def _produccion(self, *, receta, cantidad, fecha):
        return PointProductionLine.objects.create(
            branch=self.branch,
            receta=receta,
            item_name=receta.nombre,
            produced_quantity=Decimal(str(cantidad)),
            production_date=fecha,
            source_hash=str(uuid4()),
        )

    def test_excepcion_de_receta_tiene_prioridad_sobre_familia(self):
        pastel_normal = self._receta(nombre="Pastel Normal", familia="Pastel")
        pastel_excepcion = self._receta(nombre="Pastel Sin Embetunado", familia="Pastel")
        RecetaAreaProduccion.objects.create(familia="Pastel", area="EMBETUNADO")
        RecetaAreaProduccion.objects.create(receta=pastel_excepcion, area="HORNOS")

        self._produccion(receta=pastel_normal, cantidad=10, fecha=date(2026, 6, 1))
        self._produccion(receta=pastel_excepcion, cantidad=5, fecha=date(2026, 6, 1))

        # pastel_normal cuenta para EMBETUNADO (via familia); pastel_excepcion NO
        # (tiene su propia excepcion que solo lo pone en HORNOS)
        self.assertEqual(unidades_area_dia(date(2026, 6, 1), "EMBETUNADO"), Decimal("10"))
        self.assertEqual(unidades_area_dia(date(2026, 6, 1), "HORNOS"), Decimal("5"))

    def test_produccion_ligada_a_insumo_interno_tambien_cuenta(self):
        # ~51% de la produccion real en Point se liga a Insumo (masas,
        # betunes, rellenos), no a Receta. Insumo.categoria usa las mismas
        # etiquetas que Receta.familia (verificado en produccion: PAN,
        # GALLETAS, MASAS, etc.).
        RecetaAreaProduccion.objects.create(familia="MASAS", area="ARMADO")
        masa_hojaldre = Insumo.objects.create(
            nombre="Masa Hojaldre", tipo_item=Insumo.TIPO_INTERNO, categoria="MASAS",
        )
        PointProductionLine.objects.create(
            branch=self.branch, insumo=masa_hojaldre, item_name="Masa Hojaldre",
            produced_quantity=Decimal("40"), production_date=date(2026, 6, 1),
            source_hash=str(uuid4()),
        )

        self.assertEqual(unidades_area_dia(date(2026, 6, 1), "ARMADO"), Decimal("40"))

    def test_insumo_materia_prima_no_cuenta_como_produccion_interna(self):
        RecetaAreaProduccion.objects.create(familia="MASAS", area="ARMADO")
        harina = Insumo.objects.create(
            nombre="Harina", tipo_item=Insumo.TIPO_MATERIA_PRIMA, categoria="MASAS",
        )
        PointProductionLine.objects.create(
            branch=self.branch, insumo=harina, item_name="Harina",
            produced_quantity=Decimal("40"), production_date=date(2026, 6, 1),
            source_hash=str(uuid4()),
        )

        self.assertEqual(unidades_area_dia(date(2026, 6, 1), "ARMADO"), Decimal("0"))

    def test_produccion_sin_receta_no_se_cuenta(self):
        RecetaAreaProduccion.objects.create(familia="Pastel", area="HORNOS")
        PointProductionLine.objects.create(
            branch=self.branch,
            receta=None,
            item_name="Insumo sin match",
            produced_quantity=Decimal("99"),
            production_date=date(2026, 6, 1),
            source_hash=str(uuid4()),
        )

        self.assertEqual(unidades_area_dia(date(2026, 6, 1), "HORNOS"), Decimal("0"))


class CalcularCostoDiarioAreaTests(TestCase):
    def _empleado(self, *, puesto_operativo):
        return Empleado.objects.create(
            codigo=f"E-{uuid4().hex[:6]}",
            nombre="Empleado Test",
            departamento=Empleado.DEP_PRODUCCION,
            puesto_operativo=puesto_operativo,
            fecha_ingreso=date(2026, 1, 1),
            salario_diario=Decimal("400.00"),
        )

    def _receta(self, *, nombre, familia):
        return Receta.objects.create(
            nombre=nombre,
            codigo_point=f"COD-{uuid4().hex[:6]}",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            modo_costeo=Receta.MODO_COSTEO_FABRICADO,
            familia=familia,
            hash_contenido=f"h-{uuid4()}",
        )

    def test_sin_unidades_costo_unidad_es_none_no_forzado(self):
        empleado = self._empleado(puesto_operativo="HORNOS")
        periodo = NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 5, 1), fecha_fin=date(2026, 5, 14), estatus=NominaPeriodo.ESTATUS_CERRADA
        )
        NominaLinea.objects.create(periodo=periodo, empleado=empleado, salario_base=Decimal("4200.00"))

        snapshot = calcular_costo_diario_area(date(2026, 5, 5), "HORNOS")

        self.assertIsNone(snapshot.costo_unidad)
        self.assertTrue(snapshot.es_dia_laborable_esperado)
        self.assertEqual(snapshot.unidades_producidas, Decimal("0"))

    def test_con_nomina_y_unidades_calcula_costo(self):
        empleado = self._empleado(puesto_operativo="HORNOS")
        periodo = NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 5, 1), fecha_fin=date(2026, 5, 14), estatus=NominaPeriodo.ESTATUS_CERRADA
        )
        NominaLinea.objects.create(periodo=periodo, empleado=empleado, salario_base=Decimal("4200.00"))
        branch = PointBranch.objects.create(external_id=f"B-{uuid4().hex[:6]}", name="Sucursal Test")
        receta = self._receta(nombre="Pan Blanco", familia="PAN")
        RecetaAreaProduccion.objects.create(familia="PAN", area="HORNOS")
        PointProductionLine.objects.create(
            branch=branch, receta=receta, item_name="Pan Blanco",
            produced_quantity=Decimal("100"), production_date=date(2026, 5, 5),
            source_hash=str(uuid4()),
        )

        snapshot = calcular_costo_diario_area(date(2026, 5, 5), "HORNOS")

        # nomina_diaria = 4200 / 12 = 350.00; costo_unidad = 350/100 = 3.50
        self.assertEqual(snapshot.costo_unidad, Decimal("3.50"))

    def test_costo_mano_obra_diario_receta_sin_clasificar(self):
        receta = self._receta(nombre="Postre Nuevo", familia="Sin Clasificar Aun")

        resultado = costo_mano_obra_diario_receta(date(2026, 5, 5), receta)

        self.assertTrue(resultado["sin_clasificar"])
        self.assertFalse(resultado["completo"])
        self.assertIsNone(resultado["costo_total"])

    def test_costo_mano_obra_diario_receta_declara_area_faltante(self):
        # El costo de un área es un fondo compartido entre TODAS las recetas
        # que le pertenecen ese día, no algo aislado por receta.
        pan_base = self._receta(nombre="Pan Base", familia="PAN")
        RecetaAreaProduccion.objects.create(familia="PAN", area="HORNOS")

        pastel_test = self._receta(nombre="Pastel Test", familia="Pastel Especial")
        RecetaAreaProduccion.objects.create(familia="Pastel Especial", area="HORNOS")
        RecetaAreaProduccion.objects.create(familia="Pastel Especial", area="ARMADO")

        empleado_hornos = self._empleado(puesto_operativo="HORNOS")
        periodo = NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 5, 1), fecha_fin=date(2026, 5, 14), estatus=NominaPeriodo.ESTATUS_CERRADA
        )
        NominaLinea.objects.create(periodo=periodo, empleado=empleado_hornos, salario_base=Decimal("4200.00"))

        branch = PointBranch.objects.create(external_id=f"B-{uuid4().hex[:6]}", name="Sucursal Test")
        # Solo se produjo Pan Base (HORNOS) ese día. Pastel Test (HORNOS+ARMADO)
        # no se produjo, y nadie más produjo nada de ARMADO ese día.
        PointProductionLine.objects.create(
            branch=branch, receta=pan_base, item_name="Pan Base",
            produced_quantity=Decimal("100"), production_date=date(2026, 5, 5),
            source_hash=str(uuid4()),
        )

        resultado = costo_mano_obra_diario_receta(date(2026, 5, 5), pastel_test)

        self.assertFalse(resultado["completo"])
        self.assertIn("ARMADO", resultado["areas_faltantes"])
        self.assertNotIn("HORNOS", resultado["areas_faltantes"])
        self.assertIsNone(resultado["costo_total"])
