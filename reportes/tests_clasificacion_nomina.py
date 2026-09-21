"""El costo de fabricar no es todo el departamento de Producción."""
from datetime import date
from decimal import Decimal

from django.test import TestCase

from reportes.clasificacion_nomina import (
    DESTINO_ADMINISTRACION,
    DESTINO_CEDIS,
    DESTINO_LOGISTICA,
    DESTINO_PRODUCCION,
    destino_de,
)
from reportes.models import AreaPresupuesto, ReglaFuenteRubro, RubroPresupuesto
from reportes.services_presupuesto_real import PresupuestoRealConsolidacionService
from rrhh.models import Empleado, NominaConceptoLinea, NominaLinea, NominaPeriodo


class ClasificacionNominaTests(TestCase):
    def test_los_jefes_de_area_son_corporativo(self):
        self.assertEqual(
            destino_de(Empleado.NIVEL_JEFATURA, "HORNOS"), DESTINO_ADMINISTRACION,
        )
        self.assertEqual(
            destino_de(Empleado.NIVEL_DIRECCION, ""), DESTINO_ADMINISTRACION,
        )

    def test_encargadas_y_supervision_se_quedan_en_su_area(self):
        # Están en el piso, no dirigiendo desde corporativo.
        for nivel in (Empleado.NIVEL_ENCARGADA, Empleado.NIVEL_SUPERVISION):
            self.assertEqual(destino_de(nivel, ""), DESTINO_PRODUCCION)

    def test_el_area_operativa_decide_el_resto(self):
        self.assertEqual(
            destino_de(Empleado.NIVEL_COLABORADOR, "ENVIO_SUCURSAL"), DESTINO_LOGISTICA,
        )
        self.assertEqual(
            destino_de(Empleado.NIVEL_COLABORADOR, "CUARTOS_FRIOS"), DESTINO_CEDIS,
        )

    def test_las_areas_que_fabrican_cuentan_como_produccion(self):
        for area in ("HORNOS", "EMBETUNADO", "ARMADO", "PREPARACION"):
            self.assertEqual(
                destino_de(Empleado.NIVEL_COLABORADOR, area), DESTINO_PRODUCCION,
            )

    def test_crucero_sigue_contando_como_produccion(self):
        # Etiqueta sin reasignar: marcaba a quien producía fuera de CEDIS.
        self.assertEqual(
            destino_de(Empleado.NIVEL_COLABORADOR, "CRUCERO"), DESTINO_PRODUCCION,
        )

    def test_sin_datos_cae_en_el_predeterminado(self):
        self.assertEqual(destino_de(None, None), DESTINO_PRODUCCION)

    def test_el_nivel_manda_sobre_el_area(self):
        # Un jefe con área asignada sigue siendo corporativo.
        self.assertEqual(
            destino_de(Empleado.NIVEL_JEFATURA, "ENVIO_SUCURSAL"), DESTINO_ADMINISTRACION,
        )

    def test_ignora_espacios_y_minusculas(self):
        self.assertEqual(destino_de("  jefatura ", ""), DESTINO_ADMINISTRACION)
        self.assertEqual(destino_de("colaborador", " cuartos_frios "), DESTINO_CEDIS)


class ReglaNominaConDestinoTests(TestCase):
    """El filtro `destino` sobre reglas de nómina, extremo a extremo."""

    def setUp(self):
        self.area = AreaPresupuesto.objects.create(codigo="produccion-test", nombre="Producción")
        self.periodo = NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 5, 1), fecha_fin=date(2026, 5, 15),
            estatus=NominaPeriodo.ESTATUS_CERRADA,
        )
        for codigo, nivel, area_op, monto in [
            ("H1", Empleado.NIVEL_COLABORADOR, "HORNOS", "10000.00"),
            ("J1", Empleado.NIVEL_JEFATURA, "", "20000.00"),
            ("C1", Empleado.NIVEL_COLABORADOR, "CUARTOS_FRIOS", "9000.00"),
            ("E1", Empleado.NIVEL_COLABORADOR, "ENVIO_SUCURSAL", "8000.00"),
        ]:
            empleado = Empleado.objects.create(
                codigo=codigo, nombre=codigo, puesto_operativo=area_op,
                nivel_organizacional=nivel, departamento=Empleado.DEP_PRODUCCION,
            )
            NominaLinea.objects.create(
                periodo=self.periodo, empleado=empleado, salario_base=Decimal(monto),
            )

    def _monto(self, filtros):
        rubro = RubroPresupuesto.objects.create(
            area=self.area, concepto=f"Sueldo {filtros.get('destino', 'todo')}",
            tipo=RubroPresupuesto.TIPO_EGRESO,
        )
        regla = ReglaFuenteRubro(
            rubro=rubro, tipo_fuente=ReglaFuenteRubro.FUENTE_NOMINA, filtros=filtros,
        )
        svc = PresupuestoRealConsolidacionService()
        return svc._monto_nomina(regla, svc._build_nomina_index(date(2026, 5, 1)))[0]

    def test_sin_destino_toma_el_departamento_completo(self):
        # Compatibilidad: las reglas que ya existen no cambian de resultado.
        self.assertEqual(
            self._monto({"campo_monto": "salario_base", "departamento": "PRODUCCION"}),
            Decimal("47000.00"),
        )

    def test_con_destino_produccion_deja_fuera_a_quien_no_fabrica(self):
        self.assertEqual(
            self._monto({
                "campo_monto": "salario_base", "departamento": "PRODUCCION",
                "destino": "PRODUCCION",
            }),
            Decimal("10000.00"),
        )

    def test_cada_destino_recibe_lo_suyo_y_juntos_cierran(self):
        base = {"campo_monto": "salario_base", "departamento": "PRODUCCION"}
        partes = {
            destino: self._monto({**base, "destino": destino})
            for destino in ("PRODUCCION", "CEDIS", "LOGISTICA", "ADMINISTRACION")
        }
        self.assertEqual(partes["CEDIS"], Decimal("9000.00"))
        self.assertEqual(partes["LOGISTICA"], Decimal("8000.00"))
        self.assertEqual(partes["ADMINISTRACION"], Decimal("20000.00"))
        # Nada se pierde ni se duplica al repartir.
        self.assertEqual(sum(partes.values()), Decimal("47000.00"))

    def test_el_destino_tambien_aplica_a_conceptos_de_nomina(self):
        # La despensa y las vacaciones llegan por NOMINA_CONCEPTO.
        for codigo, monto in [("H1", "500.00"), ("J1", "700.00")]:
            linea = NominaLinea.objects.get(empleado__codigo=codigo, periodo=self.periodo)
            NominaConceptoLinea.objects.create(
                linea=linea, tipo=NominaConceptoLinea.TIPO_PERCEPCION,
                codigo_concepto="32", nombre="Despensa", importe=Decimal(monto),
            )
        rubro = RubroPresupuesto.objects.create(
            area=self.area, concepto="Despensa", tipo=RubroPresupuesto.TIPO_EGRESO,
        )
        svc = PresupuestoRealConsolidacionService()
        indice = svc._build_nomina_concepto_index(date(2026, 5, 1))
        base = {
            "tipo_concepto": "PERCEPCION", "codigos_concepto": ["32"],
            "departamentos": ["PRODUCCION"],
        }
        sin_destino = ReglaFuenteRubro(
            rubro=rubro, tipo_fuente=ReglaFuenteRubro.FUENTE_NOMINA_CONCEPTO, filtros=base,
        )
        con_destino = ReglaFuenteRubro(
            rubro=rubro, tipo_fuente=ReglaFuenteRubro.FUENTE_NOMINA_CONCEPTO,
            filtros={**base, "destino": "PRODUCCION"},
        )
        self.assertEqual(svc._monto_nomina_concepto(sin_destino, indice)[0], Decimal("1200.00"))
        self.assertEqual(svc._monto_nomina_concepto(con_destino, indice)[0], Decimal("500.00"))

    def test_destino_invalido_falla_en_vez_de_devolver_cero(self):
        with self.assertRaisesMessage(ValueError, "destino de nómina inválido"):
            self._monto({"campo_monto": "salario_base", "destino": "INVENTADO"})
