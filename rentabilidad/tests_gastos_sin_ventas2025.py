from datetime import date
from decimal import Decimal

from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.models import Sucursal
from rentabilidad.models_rentabilidad import EstadoRentabilidad, SucursalRentabilidad
from rentabilidad.services_resultado import registros_del_mes


class GastosSinVentas2025Tests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="PREVIA", nombre="Gasto histórico", activa=False)

    def costos(self, estado="COMPLETO"):
        filas = [dict(sucursal_id=self.sucursal.pk, area="gastos-venta", familia="telefono",
            concepto="Teléfono documentado", monto_mensual=Decimal(monto), estado=estado,
            soporte={}, origen="GASTO_OPERATIVO", registro_id=i)
            for i, monto in enumerate(("17.00", "5.00"), 1)]
        return {"filas": filas, "pendientes": [], "resultado": {
            "sucursales": {}, "centrales": [{"concepto": "Control empresa", "monto": Decimal("100")}],
            "central_registrado": Decimal("100"), "fuente": {"rows": 0, "max_sale_date": None},
        }}

    def test_gasto_confirmado_sin_ventas_reduce_empresa_una_vez_sin_snapshot(self):
        costos = self.costos()
        with CaptureQueriesContext(connection) as queries:
            rows = registros_del_mes(date(2025, 2, 1), costos)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertIsNone(row.pk)
        self.assertFalse(row.en_perimetro_ventas)
        self.assertEqual(row.ventas_netas, 0)
        self.assertEqual(row.gasto_fijo_total, Decimal("22"))
        self.assertEqual(row.utilidad_operativa-costos["resultado"]["central_registrado"], Decimal("-122"))
        self.assertEqual(row.estado, EstadoRentabilidad.SIN_DATOS)
        self.assertEqual(row.punto_equilibrio_mensual, 0)
        self.assertIsNone(row.roi_anualizado)
        self.assertIsNone(row.payback_meses_estimados)
        self.assertIsNone(row.fecha_apertura)
        self.assertFalse(SucursalRentabilidad.objects.exists())
        self.assertFalse([q for q in queries if q["sql"].lstrip().split()[0] in {"INSERT", "UPDATE", "DELETE"}])

    def test_snapshot_existente_no_duplica_gasto_documentado(self):
        snapshot = SucursalRentabilidad.objects.create(sucursal=self.sucursal,
            periodo=date(2025, 2, 1), otros_gastos_fijos=Decimal("999"))
        rows = registros_del_mes(date(2025, 2, 1), self.costos())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].pk, snapshot.pk)
        self.assertEqual(rows[0].gasto_fijo_total, Decimal("22"))
        snapshot.refresh_from_db()
        self.assertEqual(snapshot.otros_gastos_fijos, Decimal("999"))

    def test_fuente_parcial_no_amplia_perimetro2025(self):
        self.assertEqual(registros_del_mes(date(2025, 2, 1), self.costos("PARCIAL")), [])

    def test_2026_conserva_perimetro_previo(self):
        self.assertEqual(registros_del_mes(date(2026, 2, 1), self.costos()), [])
