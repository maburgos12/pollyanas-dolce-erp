from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from django.test import TestCase

from pos_bridge.services.derived_presentation_sync_service import PointDerivedPresentationSyncService
from recetas.models import Receta, RecetaPresentacionDerivada


class PointDerivedPresentationSyncServiceTests(TestCase):
    def setUp(self):
        self.parent_alt = Receta.objects.create(
            nombre="Pastel 3 Leches - Grande",
            codigo_point="9999",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
            categoria="Grande",
            hash_contenido="hash-parent-3leches-grande",
        )
        self.parent_seasonal = Receta.objects.create(
            nombre="Pastel 3 Pecados Edicion Navideño C",
            codigo_point="8888",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
            categoria="Chico",
            hash_contenido="hash-parent-3pecados-seasonal",
        )
        self.parent = Receta.objects.create(
            nombre="Pastel 3 Leches - Mediano",
            codigo_point="0105",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
            categoria="Mediano",
            hash_contenido="hash-parent-3leches-mediano",
        )
        self.derived_existing = Receta.objects.create(
            nombre="Pastel 3 Leches - Rebanada",
            codigo_point="0106",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
            categoria="Rebanada",
            hash_contenido="hash-derived-3leches-rebanada",
        )
        self.pay_parent = Receta.objects.create(
            nombre="Pay de Plátano Grande",
            codigo_point="0005",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pay",
            categoria="Grande",
            hash_contenido="hash-pay-platano-grande",
        )
        self.parent_3pecados = Receta.objects.create(
            nombre="Pastel 3 Pecados - Mediano",
            codigo_point="0108",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
            categoria="Mediano",
            hash_contenido="hash-parent-3pecados-mediano",
        )

    def _write_report(self, tmpdir: str) -> Path:
        path = Path(tmpdir) / "20260319_042414_point_recipe_gap_audit.json"
        payload = {
            "items": [
                {
                    "product": {
                        "codigo": "0106",
                        "nombre": "Pastel de 3 Leches Rebanada",
                        "familia": "Pastel",
                        "categoria": "Rebanada",
                    },
                    "status": "DERIVED_PRESENTATION",
                    "derived_rule": {
                        "kind": "SLICE_FROM_PARENT",
                        "units_per_parent": 6,
                        "parent_size_hint": "MEDIANO",
                        "requires_direct_components": True,
                        "recommended_action": "Ligar a receta padre y mantener empaque/etiqueta como componentes directos del SKU derivado.",
                    },
                },
                {
                    "product": {
                        "codigo": "0007",
                        "nombre": "Pay de Plátano Rebanada",
                        "familia": "Pay",
                        "categoria": "Rebanada",
                    },
                    "status": "DERIVED_PRESENTATION",
                    "derived_rule": {
                        "kind": "SLICE_FROM_PARENT",
                        "units_per_parent": 8,
                        "parent_size_hint": "GRANDE",
                        "requires_direct_components": True,
                        "recommended_action": "Ligar a receta padre y mantener empaque/etiqueta como componentes directos del SKU derivado.",
                    },
                },
                {
                    "product": {
                        "codigo": "0110",
                        "nombre": "Pastel de 3 Pecados R",
                        "familia": "Pastel",
                        "categoria": "Rebanada",
                    },
                    "status": "DERIVED_PRESENTATION",
                    "derived_rule": {
                        "kind": "SLICE_FROM_PARENT",
                        "units_per_parent": 10,
                        "parent_size_hint": "MEDIANO",
                        "requires_direct_components": True,
                        "recommended_action": "Ligar a receta padre y mantener empaque/etiqueta como componentes directos del SKU derivado.",
                    },
                },
            ]
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def test_sync_creates_relations_and_missing_placeholder_recipe(self):
        with TemporaryDirectory() as tmpdir:
            report_path = self._write_report(tmpdir)
            service = PointDerivedPresentationSyncService(storage_root=Path(tmpdir))

            result = service.sync(report_path=str(report_path))

        self.assertEqual(result.summary["derived_items_seen"], 3)
        self.assertEqual(result.summary["relations_created"], 3)
        self.assertEqual(result.summary["derived_recipes_created"], 2)

        rel_3l = RecetaPresentacionDerivada.objects.get(codigo_point_derivado="0106")
        self.assertEqual(rel_3l.receta_padre_id, self.parent.id)
        self.assertEqual(rel_3l.receta_derivada_id, self.derived_existing.id)
        self.assertEqual(str(rel_3l.unidades_por_padre), "6.000000")
        self.assertTrue(rel_3l.requiere_componentes_directos)

        rel_pay = RecetaPresentacionDerivada.objects.get(codigo_point_derivado="0007")
        self.assertEqual(rel_pay.receta_padre_id, self.pay_parent.id)
        self.assertEqual(rel_pay.receta_derivada.codigo_point, "0007")
        self.assertEqual(rel_pay.receta_derivada.sheet_name, PointDerivedPresentationSyncService.SHEET_NAME)

        rel_3p = RecetaPresentacionDerivada.objects.get(codigo_point_derivado="0110")
        self.assertEqual(rel_3p.receta_padre_id, self.parent_3pecados.id)
