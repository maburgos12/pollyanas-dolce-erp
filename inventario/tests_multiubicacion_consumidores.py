import csv
from datetime import date
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from django.core.management import call_command
from django.test import TestCase

from inventario.models import ExistenciaInsumo
from inventario.services_auditoria_insumos import ConsumoInsumoAuditService
from maestros.models import Insumo, UnidadMedida


class AuditoriaInsumosMultiubicacionTests(TestCase):
    def setUp(self):
        unidad = UnidadMedida.objects.create(
            codigo="kg-auditoria-multi",
            nombre="Kilogramo auditoría multi",
            tipo=UnidadMedida.TIPO_MASA,
        )
        self.insumo = Insumo.objects.create(
            nombre="Insumo auditoría multiubicación",
            unidad_base=unidad,
            activo=True,
        )
        ExistenciaInsumo.objects.create(
            insumo=self.insumo,
            almacen="CFP_1_1",
            stock_actual=Decimal("8"),
        )
        ExistenciaInsumo.objects.create(
            insumo=self.insumo,
            almacen="ARMADO",
            stock_actual=Decimal("2"),
        )

    def test_consumo_real_general_usa_stock_total_multiubicacion(self):
        service = ConsumoInsumoAuditService()
        bulk = service._consumo_real_bulk(
            date(2026, 7, 1),
            date(2026, 7, 31),
            {self.insumo.id},
        )
        single = service.calcular_consumo_real(
            date(2026, 7, 1),
            date(2026, 7, 31),
            self.insumo,
        )

        self.assertEqual(bulk[self.insumo.id]["stock_final"], Decimal("10"))
        self.assertEqual(single["stock_final"], Decimal("10"))


class ConsolidarDiccionarioMultiubicacionTests(TestCase):
    def test_reporte_conserva_existencias_separadas_por_almacen(self):
        unidad = UnidadMedida.objects.create(
            codigo="kg-diccionario-multi",
            nombre="Kilogramo diccionario multi",
            tipo=UnidadMedida.TIPO_MASA,
        )
        insumo = Insumo.objects.create(
            nombre="Insumo diccionario multiubicación",
            unidad_base=unidad,
            activo=True,
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            almacen="CFP_1_1",
            stock_actual=Decimal("8"),
        )
        ExistenciaInsumo.objects.create(
            insumo=insumo,
            almacen="ARMADO",
            stock_actual=Decimal("2"),
        )

        with TemporaryDirectory() as output_dir:
            call_command("consolidar_diccionario_nombres", output_dir=output_dir)
            report_path = next(Path(output_dir).glob("diccionario_maestro_nombres_*.csv"))
            with report_path.open(encoding="utf-8") as report:
                rows = [
                    row
                    for row in csv.DictReader(report)
                    if row["insumo_id"] == str(insumo.id)
                ]

        self.assertEqual(
            {(row["almacen"], Decimal(row["stock_actual"])) for row in rows},
            {("CFP_1_1", Decimal("8")), ("ARMADO", Decimal("2"))},
        )
