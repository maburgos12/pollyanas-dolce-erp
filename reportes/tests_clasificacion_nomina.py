"""El costo de fabricar no es todo el departamento de Producción."""
from datetime import date
from decimal import Decimal

from django.test import TestCase

from core.models import Sucursal
from reportes.clasificacion_nomina import (
    DESTINO_ADMINISTRACION,
    DESTINO_CEDIS,
    DESTINO_LOGISTICA,
    DESTINO_PRODUCCION,
    destino_de,
)
from reportes.models import (
    AreaPresupuesto, ReglaFuenteRubro, RubroPresupuesto,
)
from reportes.services_presupuesto_real import PresupuestoRealConsolidacionService
from rrhh.models import Empleado, NominaConceptoLinea, NominaLinea, NominaPeriodo


class ClasificacionNominaTests(TestCase):
    def test_el_puesto_manda_sobre_el_area(self):
        # Cuartos fríos y envío a sucursales comparten área operativa pero su
        # costo va a lugares distintos.
        self.assertEqual(
            destino_de("Cuartos Fríos", "ENVIO_SUCURSAL"), DESTINO_CEDIS,
        )
        self.assertEqual(
            destino_de("Envio a sucursales", "ENVIO_SUCURSAL"), DESTINO_LOGISTICA,
        )

    def test_el_jefe_de_produccion_es_administracion(self):
        self.assertEqual(destino_de("Jefe de Producción", ""), DESTINO_ADMINISTRACION)

    def test_encargada_y_supervisora_siguen_siendo_produccion(self):
        for puesto in ("Encargada de Producción", "Supervisora de Producción"):
            self.assertEqual(destino_de(puesto, ""), DESTINO_PRODUCCION)

    def test_crucero_ya_no_divide_la_produccion_por_locacion(self):
        # La etiqueta sobrevive en RRHH pero esas personas ya están en planta.
        self.assertEqual(destino_de("", "CRUCERO"), DESTINO_PRODUCCION)

    def test_sin_puesto_ni_area_cae_en_produccion(self):
        self.assertEqual(destino_de(None, None), DESTINO_PRODUCCION)

    def test_ignora_acentos_y_mayusculas(self):
        self.assertEqual(destino_de("JEFE DE PRODUCCION", ""), DESTINO_ADMINISTRACION)
        self.assertEqual(destino_de("  cuartos frios  ", ""), DESTINO_CEDIS)


class ReglaNominaConDestinoTests(TestCase):
    """El filtro `destino` sobre reglas de nómina, extremo a extremo."""

    def setUp(self):
        self.area = AreaPresupuesto.objects.create(codigo="produccion-test", nombre="Producción")
        self.periodo = NominaPeriodo.objects.create(
            fecha_inicio=date(2026, 5, 1), fecha_fin=date(2026, 5, 15),
            estatus=NominaPeriodo.ESTATUS_CERRADA,
        )
        for codigo, puesto, area_op, monto in [
            ("H1", "Hornero", "HORNOS", "10000.00"),
            ("J1", "Jefe de Producción", "", "20000.00"),
            ("C1", "Cuartos Fríos", "ENVIO_SUCURSAL", "9000.00"),
            ("E1", "Envio a sucursales", "ENVIO_SUCURSAL", "8000.00"),
        ]:
            empleado = Empleado.objects.create(
                codigo=codigo, nombre=puesto, puesto=puesto,
                puesto_operativo=area_op, departamento=Empleado.DEP_PRODUCCION,
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
        consolidador = PresupuestoRealConsolidacionService()
        indice = consolidador._build_nomina_index(date(2026, 5, 1))
        return consolidador._monto_nomina(regla, indice)[0]

    def test_sin_destino_toma_el_departamento_completo(self):
        # Compatibilidad: las reglas que ya existen no cambian de resultado.
        total = self._monto({"campo_monto": "salario_base", "departamento": "PRODUCCION"})
        self.assertEqual(total, Decimal("47000.00"))

    def test_con_destino_produccion_deja_fuera_a_quien_no_fabrica(self):
        total = self._monto({
            "campo_monto": "salario_base", "departamento": "PRODUCCION",
            "destino": "PRODUCCION",
        })
        self.assertEqual(total, Decimal("10000.00"))

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
        # La despensa y las vacaciones llegan por NOMINA_CONCEPTO, y deben
        # separarse igual que el sueldo.
        for codigo, monto in [("H1", "500.00"), ("J1", "700.00")]:
            linea = NominaLinea.objects.get(empleado__codigo=codigo, periodo=self.periodo)
            NominaConceptoLinea.objects.create(
                linea=linea, tipo=NominaConceptoLinea.TIPO_PERCEPCION,
                codigo_concepto="32", nombre="Despensa", importe=Decimal(monto),
            )
        rubro = RubroPresupuesto.objects.create(
            area=self.area, concepto="Despensa", tipo=RubroPresupuesto.TIPO_EGRESO,
        )
        consolidador = PresupuestoRealConsolidacionService()
        indice = consolidador._build_nomina_concepto_index(date(2026, 5, 1))
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
        self.assertEqual(
            consolidador._monto_nomina_concepto(sin_destino, indice)[0], Decimal("1200.00"),
        )
        self.assertEqual(
            consolidador._monto_nomina_concepto(con_destino, indice)[0], Decimal("500.00"),
        )

    def test_destino_invalido_falla_en_vez_de_devolver_cero(self):
        with self.assertRaisesMessage(ValueError, "destino de nómina inválido"):
            self._monto({"campo_monto": "salario_base", "destino": "INVENTADO"})
