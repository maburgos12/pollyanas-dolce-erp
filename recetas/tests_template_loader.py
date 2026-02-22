import csv
import os
import tempfile
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase

from maestros.models import CostoInsumo, Insumo, Proveedor, UnidadMedida
from recetas.utils.template_loader import _latest_cost_by_insumos, import_template


class TemplateLoaderPerformanceTests(TestCase):
    def setUp(self):
        self.unidad_kg = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.proveedor = Proveedor.objects.create(nombre="Proveedor Loader", activo=True)

    def _build_csv_file(self, rows: list[list[str]]) -> str:
        fd, path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["receta", "ingrediente", "cantidad", "unidad"])
            writer.writerows(rows)
        return path

    def test_latest_cost_by_insumos_returns_latest_snapshot_per_insumo(self):
        insumo_a = Insumo.objects.create(
            nombre="Harina de prueba",
            unidad_base=self.unidad_kg,
            proveedor_principal=self.proveedor,
        )
        insumo_b = Insumo.objects.create(
            nombre="Azucar de prueba",
            unidad_base=self.unidad_kg,
            proveedor_principal=self.proveedor,
        )
        CostoInsumo.objects.create(
            insumo=insumo_a,
            proveedor=self.proveedor,
            fecha=date(2026, 1, 10),
            costo_unitario=Decimal("10.00"),
            source_hash="loader-cost-a-1",
        )
        CostoInsumo.objects.create(
            insumo=insumo_a,
            proveedor=self.proveedor,
            fecha=date(2026, 1, 12),
            costo_unitario=Decimal("12.50"),
            source_hash="loader-cost-a-2",
        )
        CostoInsumo.objects.create(
            insumo=insumo_b,
            proveedor=self.proveedor,
            fecha=date(2026, 1, 11),
            costo_unitario=Decimal("8.25"),
            source_hash="loader-cost-b-1",
        )

        latest = _latest_cost_by_insumos({insumo_a.id, insumo_b.id})

        self.assertEqual(latest[insumo_a.id], Decimal("12.50"))
        self.assertEqual(latest[insumo_b.id], Decimal("8.25"))

    def test_import_template_prefetches_costs_without_per_insumo_lookup(self):
        insumo = Insumo.objects.create(
            nombre="Azucar estándar",
            unidad_base=self.unidad_kg,
            proveedor_principal=self.proveedor,
        )
        CostoInsumo.objects.create(
            insumo=insumo,
            proveedor=self.proveedor,
            fecha=date(2026, 2, 1),
            costo_unitario=Decimal("19.90"),
            source_hash="loader-cost-azucar-1",
        )
        path = self._build_csv_file(
            [
                ["Receta Performance", "Azucar estándar", "2", "kg"],
            ]
        )
        try:
            with patch("recetas.utils.template_loader._latest_cost_by_insumo") as single_lookup:
                result = import_template(path)
            self.assertEqual(result.recetas_creadas, 1)
            self.assertEqual(result.lineas_creadas, 1)
            single_lookup.assert_not_called()
        finally:
            os.unlink(path)
