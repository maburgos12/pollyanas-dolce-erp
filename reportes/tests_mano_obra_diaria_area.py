from datetime import date
from decimal import Decimal
from uuid import uuid4

from django.test import TestCase

from maestros.models import Insumo
from pos_bridge.models import PointBranch, PointProductionLine
from recetas.models import Receta
from reportes.models import FamiliaGrupoManoObra, RecetaAreaProduccion
from reportes.services_mano_obra_diaria_area import (
    _grupos_insumo_por_area,
    _recetas_minutos_por_area,
    area_produccion_empleado,
    calcular_costo_diario_area,
    costo_mano_obra_diario_receta,
    empleados_area_periodo,
    minutos_area_dia,
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

    def test_clasificar_grupo_pastel_incluye_variantes_de_tamano(self):
        # RecetaAreaProduccion guarda el GRUPO canónico "Pastel", pero Point
        # trae la producción real bajo familias distintas por tamaño
        # (Pastel Chico/Grande/Mediano/Mini) — decisión de negocio confirmada
        # por Mauricio de fusionar por tamaño, no una fusión de texto genérica.
        FamiliaGrupoManoObra.objects.create(familia_real="Pastel Chico", grupo="Pastel")
        FamiliaGrupoManoObra.objects.create(familia_real="Pastel Grande", grupo="Pastel")
        RecetaAreaProduccion.objects.create(familia="Pastel", area="EMBETUNADO")
        pastel_chico = self._receta(nombre="Pastel Chico Fresa", familia="Pastel Chico")
        pastel_grande = self._receta(nombre="Pastel Grande Chocolate", familia="Pastel Grande")

        self._produccion(receta=pastel_chico, cantidad=6, fecha=date(2026, 6, 1))
        self._produccion(receta=pastel_grande, cantidad=4, fecha=date(2026, 6, 1))

        self.assertEqual(unidades_area_dia(date(2026, 6, 1), "EMBETUNADO"), Decimal("10"))

    def test_clasificar_grupo_betun_incluye_familia_original_de_point(self):
        # "Betún y Rellenos" y "Betún, Cremas, Rellenos (INSUMO PRODUCIDO)"
        # son, confirmado por Mauricio, el mismo grupo de producción.
        FamiliaGrupoManoObra.objects.create(
            familia_real="Betún y Rellenos", grupo="Betún, Cremas, Rellenos (INSUMO PRODUCIDO)"
        )
        RecetaAreaProduccion.objects.create(
            familia="Betún, Cremas, Rellenos (INSUMO PRODUCIDO)", area="ARMADO"
        )
        betun_original = Insumo.objects.create(
            nombre="Betún de Vainilla", tipo_item=Insumo.TIPO_INTERNO, categoria="Betún y Rellenos",
        )
        PointProductionLine.objects.create(
            branch=self.branch, insumo=betun_original, item_name="Betún de Vainilla",
            produced_quantity=Decimal("15"), production_date=date(2026, 6, 1),
            source_hash=str(uuid4()),
        )

        self.assertEqual(unidades_area_dia(date(2026, 6, 1), "ARMADO"), Decimal("15"))


class MinutosEstandarPiezaTests(TestCase):
    def test_calcula_con_los_3_campos_presentes(self):
        fila = RecetaAreaProduccion.objects.create(
            familia="Pastel", area="HORNOS",
            lote_personas=2, lote_minutos=Decimal("20"), lote_piezas=30,
        )
        # 2 personas * 20 min = 40 minutos-persona / 30 piezas
        self.assertEqual(fila.minutos_estandar_pieza, Decimal("40") / Decimal("30"))

    def test_none_si_falta_cualquier_campo(self):
        fila = RecetaAreaProduccion.objects.create(familia="Pastel", area="HORNOS")
        self.assertIsNone(fila.minutos_estandar_pieza)

        fila.lote_personas = 2
        fila.lote_minutos = Decimal("20")
        self.assertIsNone(fila.minutos_estandar_pieza)


class MinutosAreaDiaTests(TestCase):
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

    def test_receta_calibrada_aporta_piezas_por_minuto_pieza(self):
        receta = self._receta(nombre="Pastel Grande Fresa", familia="Pastel")
        RecetaAreaProduccion.objects.create(
            familia="Pastel", area="EMBETUNADO",
            lote_personas=1, lote_minutos=Decimal("20"), lote_piezas=10,
        )  # 2 min/pieza
        PointProductionLine.objects.create(
            branch=self.branch, receta=receta, item_name=receta.nombre,
            produced_quantity=Decimal("15"), production_date=date(2026, 6, 1),
            source_hash=str(uuid4()),
        )

        self.assertEqual(minutos_area_dia(date(2026, 6, 1), "EMBETUNADO"), Decimal("30"))

    def test_receta_clasificada_sin_calibrar_no_aporta(self):
        receta = self._receta(nombre="Pay de Queso", familia="Pay")
        RecetaAreaProduccion.objects.create(familia="Pay", area="HORNOS")  # sin lote capturado
        PointProductionLine.objects.create(
            branch=self.branch, receta=receta, item_name=receta.nombre,
            produced_quantity=Decimal("20"), production_date=date(2026, 6, 1),
            source_hash=str(uuid4()),
        )

        self.assertEqual(minutos_area_dia(date(2026, 6, 1), "HORNOS"), Decimal("0"))


class GruposInsumoPorAreaTests(TestCase):
    """Calibración de mano de obra para Catálogos (Insumo) por preparación
    específica, no por categoria — la unidad (kg/lt/pza) es consistente
    por preparación, no por categoria (verificado con datos reales:
    "Betún, Cremas, Rellenos" mezcla KG y Litro según la preparación)."""

    def setUp(self):
        self.branch = PointBranch.objects.create(external_id=f"B-{uuid4().hex[:6]}", name="Sucursal Test")

    def test_insumo_sin_grupo_mano_obra_resuelve_por_su_propio_nombre(self):
        betun = Insumo.objects.create(
            nombre="Betún Dream Whip Pastel", tipo_item=Insumo.TIPO_INTERNO,
            categoria="Betún, Cremas, Rellenos (INSUMO PRODUCIDO)",
        )
        RecetaAreaProduccion.objects.create(
            familia="Betún Dream Whip Pastel", area="EMBETUNADO", es_grupo_insumo=True,
            lote_personas=1, lote_minutos=Decimal("30"), lote_piezas=10,
        )  # 3 min/kg

        minutos = _grupos_insumo_por_area("EMBETUNADO")

        self.assertEqual(minutos[betun.id], Decimal("3"))

    def test_dos_preparaciones_fusionadas_agregan_minutos_bajo_el_mismo_grupo(self):
        pan_chico = Insumo.objects.create(
            nombre="Pan Vainilla Dawn Chico", tipo_item=Insumo.TIPO_INTERNO,
            categoria="PAN", grupo_mano_obra="Pan Vainilla Dawn",
        )
        pan_grande = Insumo.objects.create(
            nombre="Pan Vainilla Dawn Grande", tipo_item=Insumo.TIPO_INTERNO,
            categoria="PAN", grupo_mano_obra="Pan Vainilla Dawn",
        )
        RecetaAreaProduccion.objects.create(
            familia="Pan Vainilla Dawn", area="HORNOS", es_grupo_insumo=True,
            lote_personas=1, lote_minutos=Decimal("10"), lote_piezas=5,
        )  # 2 min/pieza

        minutos = _grupos_insumo_por_area("HORNOS")

        self.assertEqual(minutos[pan_chico.id], Decimal("2"))
        self.assertEqual(minutos[pan_grande.id], Decimal("2"))

    def test_preparaciones_distintas_no_comparten_minuto_aunque_compartan_categoria(self):
        # Caso real: "Betún, Cremas, Rellenos" mezcla KG (Betún Dream Whip)
        # y Litro (Mezcla 3 Leches) — antes de esta vuelta ambos compartían
        # el minuto/unidad de la categoria completa, lo cual mezclaba
        # unidades incompatibles.
        betun = Insumo.objects.create(
            nombre="Betún Dream Whip Pastel", tipo_item=Insumo.TIPO_INTERNO,
            categoria="Betún, Cremas, Rellenos (INSUMO PRODUCIDO)",
        )
        mezcla = Insumo.objects.create(
            nombre="Mezcla 3 Leches", tipo_item=Insumo.TIPO_INTERNO,
            categoria="Betún, Cremas, Rellenos (INSUMO PRODUCIDO)",
        )
        RecetaAreaProduccion.objects.create(
            familia="Betún Dream Whip Pastel", area="EMBETUNADO", es_grupo_insumo=True,
            lote_personas=1, lote_minutos=Decimal("30"), lote_piezas=10,
        )  # 3 min/kg
        RecetaAreaProduccion.objects.create(
            familia="Mezcla 3 Leches", area="EMBETUNADO", es_grupo_insumo=True,
            lote_personas=2, lote_minutos=Decimal("15"), lote_piezas=6,
        )  # 5 min/litro
        PointProductionLine.objects.create(
            branch=self.branch, insumo=betun, item_name=betun.nombre, unit="KG",
            produced_quantity=Decimal("20"), production_date=date(2026, 6, 1),
            source_hash=str(uuid4()),
        )
        PointProductionLine.objects.create(
            branch=self.branch, insumo=mezcla, item_name=mezcla.nombre, unit="Litro",
            produced_quantity=Decimal("10"), production_date=date(2026, 6, 1),
            source_hash=str(uuid4()),
        )

        # 20 kg * 3 min/kg + 10 litros * 5 min/litro = 60 + 50 = 110
        self.assertEqual(minutos_area_dia(date(2026, 6, 1), "EMBETUNADO"), Decimal("110"))

    def test_familia_receta_y_grupo_insumo_con_mismo_texto_no_colisionan(self):
        receta = Receta.objects.create(
            nombre="Pastel Tres Leches", codigo_point=f"COD-{uuid4().hex[:6]}",
            tipo=Receta.TIPO_PRODUCTO_FINAL, modo_costeo=Receta.MODO_COSTEO_FABRICADO,
            familia="Tres Leches", hash_contenido=f"h-{uuid4()}",
        )
        insumo = Insumo.objects.create(
            nombre="Tres Leches", tipo_item=Insumo.TIPO_INTERNO, categoria="MASAS",
        )
        RecetaAreaProduccion.objects.create(
            familia="Tres Leches", area="HORNOS", es_grupo_insumo=False,
            lote_personas=1, lote_minutos=Decimal("10"), lote_piezas=10,
        )  # 1 min/pieza, receta
        RecetaAreaProduccion.objects.create(
            familia="Tres Leches", area="ARMADO", es_grupo_insumo=True,
            lote_personas=1, lote_minutos=Decimal("20"), lote_piezas=10,
        )  # 2 min/unidad, insumo

        # La fila de receta (HORNOS) no debe aparecer al resolver insumos,
        # y viceversa — cada namespace se resuelve por separado aunque
        # compartan el mismo texto en "familia".
        self.assertEqual(_recetas_minutos_por_area("HORNOS"), {receta.id: Decimal("1")})
        self.assertEqual(_grupos_insumo_por_area("HORNOS"), {})
        self.assertEqual(_recetas_minutos_por_area("ARMADO"), {})
        self.assertEqual(_grupos_insumo_por_area("ARMADO"), {insumo.id: Decimal("2")})


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

    def test_sin_minutos_demandados_costo_minuto_es_none_no_forzado(self):
        empleado = self._empleado(puesto_operativo="HORNOS")
        periodo = NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 5, 1), fecha_fin=date(2026, 5, 14), estatus=NominaPeriodo.ESTATUS_CERRADA
        )
        NominaLinea.objects.create(periodo=periodo, empleado=empleado, salario_base=Decimal("4200.00"))

        snapshot = calcular_costo_diario_area(date(2026, 5, 5), "HORNOS")

        self.assertIsNone(snapshot.costo_minuto)
        self.assertTrue(snapshot.es_dia_laborable_esperado)
        self.assertEqual(snapshot.unidades_producidas, Decimal("0"))
        # 1 empleado en HORNOS ese período * 480 min de turno estándar
        self.assertEqual(snapshot.minutos_disponibles, Decimal("480"))

    def test_con_nomina_y_minutos_calibrados_calcula_costo_minuto(self):
        empleado = self._empleado(puesto_operativo="HORNOS")
        periodo = NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 5, 1), fecha_fin=date(2026, 5, 14), estatus=NominaPeriodo.ESTATUS_CERRADA
        )
        NominaLinea.objects.create(periodo=periodo, empleado=empleado, salario_base=Decimal("4200.00"))
        branch = PointBranch.objects.create(external_id=f"B-{uuid4().hex[:6]}", name="Sucursal Test")
        receta = self._receta(nombre="Pan Blanco", familia="PAN")
        RecetaAreaProduccion.objects.create(
            familia="PAN", area="HORNOS",
            lote_personas=1, lote_minutos=Decimal("100"), lote_piezas=100,
        )  # 1 min/pieza
        PointProductionLine.objects.create(
            branch=branch, receta=receta, item_name="Pan Blanco",
            produced_quantity=Decimal("100"), production_date=date(2026, 5, 5),
            source_hash=str(uuid4()),
        )

        snapshot = calcular_costo_diario_area(date(2026, 5, 5), "HORNOS")

        # nomina_diaria = 4200 / 12 = 350.00; minutos_demandados = 100*1 = 100
        # costo_minuto = 350/100 = 3.50
        self.assertEqual(snapshot.minutos_demandados, Decimal("100"))
        self.assertEqual(snapshot.costo_minuto, Decimal("3.50"))

    def test_costo_mano_obra_diario_receta_sin_clasificar(self):
        receta = self._receta(nombre="Postre Nuevo", familia="Sin Clasificar Aun")

        resultado = costo_mano_obra_diario_receta(date(2026, 5, 5), receta)

        self.assertTrue(resultado["sin_clasificar"])
        self.assertFalse(resultado["completo"])
        self.assertIsNone(resultado["costo_total"])

    def test_costo_mano_obra_diario_receta_declara_area_faltante(self):
        # El costo de un área es un fondo compartido entre TODAS las recetas
        # que le pertenecen ese día, no algo aislado por receta. Ambas
        # familias están calibradas (con minutos) — lo que falta es
        # producción real en ARMADO ese día, no calibración.
        pan_base = self._receta(nombre="Pan Base", familia="PAN")
        RecetaAreaProduccion.objects.create(
            familia="PAN", area="HORNOS",
            lote_personas=1, lote_minutos=Decimal("100"), lote_piezas=100,
        )

        pastel_test = self._receta(nombre="Pastel Test", familia="Pastel Especial")
        RecetaAreaProduccion.objects.create(
            familia="Pastel Especial", area="HORNOS",
            lote_personas=1, lote_minutos=Decimal("100"), lote_piezas=100,
        )
        RecetaAreaProduccion.objects.create(
            familia="Pastel Especial", area="ARMADO",
            lote_personas=1, lote_minutos=Decimal("50"), lote_piezas=50,
        )

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

    def test_receta_con_familia_variante_resuelve_por_grupo_canonico(self):
        # La receta trae la familia real de Point ("Pastel Chico"), pero la
        # clasificación (con minutos calibrados) se guardó contra el grupo
        # canónico ("Pastel").
        FamiliaGrupoManoObra.objects.create(familia_real="Pastel Chico", grupo="Pastel")
        pastel_chico = self._receta(nombre="Pastel Chico Fresa", familia="Pastel Chico")
        RecetaAreaProduccion.objects.create(
            familia="Pastel", area="HORNOS",
            lote_personas=1, lote_minutos=Decimal("100"), lote_piezas=100,
        )  # 1 min/pieza

        empleado_hornos = self._empleado(puesto_operativo="HORNOS")
        periodo = NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 5, 1), fecha_fin=date(2026, 5, 14), estatus=NominaPeriodo.ESTATUS_CERRADA
        )
        NominaLinea.objects.create(periodo=periodo, empleado=empleado_hornos, salario_base=Decimal("4200.00"))

        branch = PointBranch.objects.create(external_id=f"B-{uuid4().hex[:6]}", name="Sucursal Test")
        PointProductionLine.objects.create(
            branch=branch, receta=pastel_chico, item_name="Pastel Chico Fresa",
            produced_quantity=Decimal("100"), production_date=date(2026, 5, 5),
            source_hash=str(uuid4()),
        )

        resultado = costo_mano_obra_diario_receta(date(2026, 5, 5), pastel_chico)

        # minutos_receta (1.0) * costo_minuto (350/100=3.50) = 3.50
        self.assertTrue(resultado["completo"])
        self.assertEqual(resultado["costo_total"], Decimal("3.50"))

    def test_receta_clasificada_sin_calibrar_declara_area_faltante(self):
        # Distinto del caso "sin producción": aquí SÍ hubo producción y
        # nómina ese día, pero nadie ha capturado los minutos de esta
        # familia todavía — no se inventa un costo.
        receta = self._receta(nombre="Pay de Queso", familia="Pay")
        RecetaAreaProduccion.objects.create(familia="Pay", area="HORNOS")  # sin lote

        empleado_hornos = self._empleado(puesto_operativo="HORNOS")
        periodo = NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 5, 1), fecha_fin=date(2026, 5, 14), estatus=NominaPeriodo.ESTATUS_CERRADA
        )
        NominaLinea.objects.create(periodo=periodo, empleado=empleado_hornos, salario_base=Decimal("4200.00"))

        branch = PointBranch.objects.create(external_id=f"B-{uuid4().hex[:6]}", name="Sucursal Test")
        PointProductionLine.objects.create(
            branch=branch, receta=receta, item_name="Pay de Queso",
            produced_quantity=Decimal("20"), production_date=date(2026, 5, 5),
            source_hash=str(uuid4()),
        )

        resultado = costo_mano_obra_diario_receta(date(2026, 5, 5), receta)

        self.assertFalse(resultado["completo"])
        self.assertIn("HORNOS", resultado["areas_faltantes"])
        self.assertIsNone(resultado["costo_total"])
        self.assertFalse(resultado["sin_clasificar"])

    def test_empleados_area_periodo_cuenta_headcount_no_nomina(self):
        self._empleado(puesto_operativo="HORNOS")
        self._empleado(puesto_operativo="HORNOS")
        periodo = NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 5, 1), fecha_fin=date(2026, 5, 14), estatus=NominaPeriodo.ESTATUS_CERRADA
        )
        for empleado in Empleado.objects.filter(puesto_operativo="HORNOS"):
            NominaLinea.objects.create(periodo=periodo, empleado=empleado, salario_base=Decimal("4200.00"))

        self.assertEqual(empleados_area_periodo(date(2026, 5, 5), "HORNOS"), 2)
        self.assertEqual(empleados_area_periodo(date(2026, 6, 1), "HORNOS"), 0)
