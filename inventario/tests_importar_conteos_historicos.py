import json
import re
from datetime import date
from decimal import Decimal
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from uuid import uuid4, uuid5

from django.contrib.auth import get_user_model
from django.core.management import call_command, get_commands
from django.core.management.base import CommandError
from django.db import connection
from django.test import SimpleTestCase, TestCase
from django.test.utils import CaptureQueriesContext

from core.models import Sucursal
from inventario.models import ExistenciaInsumo, MovimientoInventario
from inventario.management.commands.importar_conteos_sucursal_historicos import (
    IMPORT_NAMESPACE,
)
from inventario.models_conteos import (
    ConteoSucursal,
    EventoConteoSucursal,
    LecturaConteoSucursal,
    LineaConteoSucursal,
)
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import (
    PointInsumoInventorySnapshot,
    PointInventorySnapshot,
    PointProduct,
    PointSyncJob,
)
from pos_bridge.services.product_count_units import COUNT_UNIT_KEY, catalog_count_unit


class ImportarConteosHistoricosCommandTests(SimpleTestCase):
    def test_command_is_registered(self):
        self.assertIn("importar_conteos_sucursal_historicos", get_commands())


class ImportarConteosHistoricosTests(TestCase):
    def setUp(self):
        self.tempdir = TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.actor = get_user_model().objects.create_superuser("maburgos12")
        self.branches = [
            Sucursal.objects.create(codigo=f"H{i}", nombre=f"Histórica {i}")
            for i in range(1, 5)
        ]
        self.product = PointProduct.objects.create(
            external_id="hist-product",
            sku="SKU-HIST",
            name="Producto histórico",
            metadata={
                COUNT_UNIT_KEY: catalog_count_unit(
                    {"Codigo": "SKU-HIST", "Unidad": "pza"}
                )
            },
        )
        unit = UnidadMedida.objects.create(
            codigo="kg-hist",
            nombre="Kilogramo histórico",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=1,
        )
        self.insumo = Insumo.objects.create(
            codigo_point="INS-HIST",
            nombre="Insumo histórico",
            unidad_base=unit,
        )
        self.payload = {
            "fuente": {
                "nombre": "conteos_octubre.xlsx",
                "sha256_declarado": "a" * 64,
                "verificacion_externa": {
                    "metodo": "shasum -a 256",
                    "verificado_por": "Mauricio Burgos",
                },
            },
            "fecha": "2026-10-31",
            "actor": "maburgos12",
            "sucursales": [
                {
                    "codigo": branch.codigo,
                    "conteos": [
                        {
                            "domain": "producto",
                            "catalogo_id": self.product.pk,
                            "sku": "SKU-HIST",
                            "catalogo_nombre": "Producto histórico",
                            "external_id": "hist-product",
                            "cantidad": "0" if index == 0 else str(index),
                            "hoja": branch.nombre,
                            "celda": "B2",
                        },
                        {
                            "domain": "insumo",
                            "catalogo_id": self.insumo.pk,
                            "codigo_point": "INS-HIST",
                            "catalogo_nombre": "Insumo histórico",
                            "cantidad": "1.250",
                            "hoja": branch.nombre,
                            "celda": "C3",
                        },
                    ],
                }
                for index, branch in enumerate(self.branches)
            ],
        }

    def run_import(self, *, aplicar=False, payload=None):
        path = Path(self.tempdir.name) / "payload.json"
        path.write_text(json.dumps(payload or self.payload), encoding="utf-8")
        output = StringIO()
        args = ["importar_conteos_sucursal_historicos", str(path)]
        if aplicar:
            args.append("--aplicar")
        call_command(*args, stdout=output)
        return output.getvalue()

    def assert_no_evidence(self):
        self.assertEqual(ConteoSucursal.objects.count(), 0)
        self.assertEqual(LineaConteoSucursal.objects.count(), 0)
        self.assertEqual(LecturaConteoSucursal.objects.count(), 0)
        self.assertEqual(EventoConteoSucursal.objects.count(), 0)

    def test_dry_run_validates_without_writes(self):
        output = self.run_import()
        self.assertIn("DRY-RUN", output)
        self.assert_no_evidence()

    def test_apply_creates_sent_evidence_and_preserves_zero(self):
        self.run_import(aplicar=True)
        self.assertEqual(ConteoSucursal.objects.count(), 4)
        self.assertEqual(LineaConteoSucursal.objects.count(), 8)
        self.assertEqual(LecturaConteoSucursal.objects.count(), 8)
        counts = ConteoSucursal.objects.order_by("sucursal__codigo")
        self.assertTrue(all(count.estado == "ENVIADO" for count in counts))
        self.assertTrue(all(count.responsable == self.actor for count in counts))
        self.assertTrue(all(count.creado_por == self.actor for count in counts))
        self.assertTrue(all(count.iniciado_en is None for count in counts))
        self.assertTrue(all(count.aceptado_en is None for count in counts))
        self.assertTrue(all(count.conciliacion == {} for count in counts))
        self.assertTrue(
            all("sin hora física comprobable; no ajusta Point" in count.observaciones for count in counts)
        )
        zero = LecturaConteoSucursal.objects.get(
            linea__conteo__sucursal=self.branches[0], linea__producto=self.product
        )
        self.assertEqual(zero.cantidad, Decimal("0"))
        self.assertIsNone(zero.contado_en)

    def test_second_apply_is_idempotent_and_changed_payload_conflicts(self):
        self.run_import(aplicar=True)
        totals = tuple(
            model.objects.count()
            for model in (
                ConteoSucursal,
                LineaConteoSucursal,
                LecturaConteoSucursal,
                EventoConteoSucursal,
            )
        )
        output = self.run_import(aplicar=True)
        self.assertIn("4 ya importados", output)
        self.assertEqual(
            totals,
            tuple(
                model.objects.count()
                for model in (
                    ConteoSucursal,
                    LineaConteoSucursal,
                    LecturaConteoSucursal,
                    EventoConteoSucursal,
                )
            ),
        )
        changed = json.loads(json.dumps(self.payload))
        changed["sucursales"][0]["conteos"][0]["cantidad"] = "9"
        with self.assertRaisesMessage(CommandError, "otros datos"):
            self.run_import(aplicar=True, payload=changed)
        self.assertEqual(totals[0], ConteoSucursal.objects.count())

    def test_conflict_on_fourth_branch_rolls_back_the_entire_batch(self):
        ConteoSucursal.objects.create(
            sucursal=self.branches[3],
            responsable=self.actor,
            creado_por=self.actor,
            fecha=date(2026, 10, 31),
            titulo="Conteo ya aceptado",
            estado="ACEPTADO",
            request_id=uuid4(),
            payload_hash="b" * 64,
        )
        with self.assertRaisesRegex(CommandError, "(?i)ya existe"):
            self.run_import(aplicar=True)
        self.assertEqual(ConteoSucursal.objects.count(), 1)
        self.assertFalse(ConteoSucursal.objects.filter(estado="ENVIADO").exists())

    def test_stdin_payload_applies_without_copying_a_file(self):
        output = StringIO()
        with patch(
            "inventario.management.commands.importar_conteos_sucursal_historicos.sys.stdin",
            StringIO(json.dumps(self.payload)),
        ):
            call_command(
                "importar_conteos_sucursal_historicos", "-", "--aplicar", stdout=output
            )
        self.assertEqual(ConteoSucursal.objects.count(), 4)
        self.assertIn("4 creados", output.getvalue())

    def test_active_actor_without_coordination_is_rejected(self):
        get_user_model().objects.create_user("capturista-sin-coordinacion")
        payload = json.loads(json.dumps(self.payload))
        payload["actor"] = "capturista-sin-coordinacion"
        with self.assertRaisesMessage(CommandError, "coordinar"):
            self.run_import(aplicar=True, payload=payload)
        self.assert_no_evidence()

    def test_duplicate_sku_without_catalog_id_rejects_but_full_identity_resolves(self):
        PointProduct.objects.create(
            external_id="hist-product-duplicate",
            sku="SKU-HIST",
            name="Producto histórico duplicado",
        )
        missing_id = json.loads(json.dumps(self.payload))
        del missing_id["sucursales"][0]["conteos"][0]["catalogo_id"]
        with self.assertRaisesMessage(CommandError, "ambiguo"):
            self.run_import(aplicar=True, payload=missing_id)
        self.assert_no_evidence()

        self.run_import(aplicar=True)
        self.assertEqual(ConteoSucursal.objects.count(), 4)

    def test_same_legacy_code_across_domains_preserves_exact_identity(self):
        self.insumo.codigo_point = self.product.sku
        self.insumo.save(update_fields=['codigo_point'])
        payload = json.loads(json.dumps(self.payload))
        for branch in payload['sucursales']:
            insumo = next(row for row in branch['conteos'] if row['domain'] == 'insumo')
            insumo['codigo_point'] = self.product.sku
        self.run_import(aplicar=True, payload=payload)
        lines=LineaConteoSucursal.objects.filter(conteo__sucursal=self.branches[0]).select_related('producto')
        self.assertEqual(set(lines.values_list('codigo',flat=True)),{self.product.sku})
        self.assertEqual({line.codigo_unico for line in lines},
                         {f'POINT_PRODUCT:{self.product.external_id}',f'ERP_INSUMO:{self.insumo.pk}'})

    def test_inactive_identifier_rejects_and_rolls_back_all(self):
        self.product.active = False
        self.product.save(update_fields=["active"])
        with self.assertRaisesMessage(CommandError, "inactivo"):
            self.run_import(aplicar=True)
        self.assert_no_evidence()

    def test_mismatched_catalog_identity_rejects(self):
        for field, value in (("catalogo_nombre", "Nombre Excel"), ("sku", " SKU-HIST ")):
            changed = json.loads(json.dumps(self.payload))
            changed["sucursales"][0]["conteos"][0][field] = value
            with self.subTest(field=field), self.assertRaisesMessage(
                CommandError, "no coincide"
            ):
                self.run_import(aplicar=True, payload=changed)
            self.assert_no_evidence()

    def test_insumo_identity_and_product_external_id_must_match_exactly(self):
        cases = (
            ("insumo", "codigo_point", "OTRO-CODIGO"),
            ("insumo", "catalogo_nombre", "Otro insumo"),
            ("producto", "external_id", "external-id-incorrecto"),
        )
        for domain, field, value in cases:
            payload = json.loads(json.dumps(self.payload))
            line = next(
                row
                for row in payload["sucursales"][0]["conteos"]
                if row["domain"] == domain
            )
            line[field] = value
            with self.subTest(domain=domain, field=field), self.assertRaisesMessage(
                CommandError, "no coincide"
            ):
                self.run_import(aplicar=True, payload=payload)
            self.assert_no_evidence()

    def test_uuid5_contract_and_cancelled_count_allows_import(self):
        cancelled = ConteoSucursal.objects.create(
            sucursal=self.branches[0],
            responsable=self.actor,
            creado_por=self.actor,
            fecha=date(2026, 10, 31),
            titulo="Cancelado previo",
            estado="CANCELADO",
            request_id=uuid4(),
            payload_hash="c" * 64,
        )
        self.run_import(aplicar=True)
        imported = ConteoSucursal.objects.exclude(pk=cancelled.pk).get(
            sucursal=self.branches[0]
        )
        expected = uuid5(
            IMPORT_NAMESPACE,
            f'{self.payload["fuente"]["sha256_declarado"]}:2026-10-31:{self.branches[0].codigo}',
        )
        self.assertEqual(imported.request_id, expected)
        self.assertEqual(ConteoSucursal.objects.filter(estado="ENVIADO").count(), 4)

    def test_duplicate_branch_codes_are_rejected(self):
        payload = json.loads(json.dumps(self.payload))
        payload["sucursales"][3]["codigo"] = payload["sucursales"][0]["codigo"]
        with self.assertRaisesMessage(CommandError, "distintas"):
            self.run_import(aplicar=True, payload=payload)
        self.assert_no_evidence()

    def test_inactive_insumo_rejects_and_rolls_back_all_branches(self):
        self.insumo.activo = False
        self.insumo.save(update_fields=["activo"])
        with self.assertRaisesMessage(CommandError, "inactivo"):
            self.run_import(aplicar=True)
        self.assert_no_evidence()

    def test_product_external_id_is_optional_when_other_identity_matches(self):
        payload = json.loads(json.dumps(self.payload))
        for branch in payload["sucursales"]:
            product = next(
                row for row in branch["conteos"] if row["domain"] == "producto"
            )
            del product["external_id"]
        self.run_import(aplicar=True, payload=payload)
        self.assertEqual(ConteoSucursal.objects.filter(estado="ENVIADO").count(), 4)
        self.assertEqual(LineaConteoSucursal.objects.filter(producto=self.product).count(), 4)

    def test_rejects_extra_fields_at_every_payload_level(self):
        mutations = []
        for path in (
            (),
            ("fuente",),
            ("sucursales", 0),
            ("sucursales", 0, "conteos", 0),
        ):
            payload = json.loads(json.dumps(self.payload))
            target = payload
            for key in path:
                target = target[key]
            target["extra_no_admitido"] = True
            mutations.append(payload)
        for payload in mutations:
            with self.subTest(keys=payload.keys()), self.assertRaises(CommandError):
                self.run_import(aplicar=True, payload=payload)
            self.assert_no_evidence()

    def test_declared_digest_requires_external_verification_metadata(self):
        for field in ("metodo", "verificado_por"):
            payload = json.loads(json.dumps(self.payload))
            payload["fuente"]["verificacion_externa"][field] = ""
            with self.subTest(field=field), self.assertRaisesMessage(
                CommandError, field
            ):
                self.run_import(aplicar=True, payload=payload)
            self.assert_no_evidence()

    def test_current_or_future_date_is_rejected(self):
        from django.utils import timezone

        for value in (timezone.localdate(), timezone.localdate().replace(year=2030)):
            payload = json.loads(json.dumps(self.payload))
            payload["fecha"] = value.isoformat()
            with self.subTest(fecha=value), self.assertRaisesMessage(
                CommandError, "anterior"
            ):
                self.run_import(aplicar=True, payload=payload)
            self.assert_no_evidence()

    def test_compact_date_is_rejected(self):
        payload = json.loads(json.dumps(self.payload))
        payload["fecha"] = "20261031"
        with self.assertRaisesMessage(CommandError, "YYYY-MM-DD"):
            self.run_import(aplicar=True, payload=payload)
        self.assert_no_evidence()

    def test_catalog_drift_during_freeze_is_rejected(self):
        frozen = {
            "producto_id": self.product.pk,
            "codigo": "CODIGO-CAMBIADO",
            "nombre": self.product.name,
            "unidad": "pza",
            "fuente_unidad": "catálogo",
        }
        with patch(
            "inventario.management.commands.importar_conteos_sucursal_historicos.resolver_articulo_conteo",
            return_value=frozen,
        ), self.assertRaisesMessage(CommandError, "cambió"):
            self.run_import(aplicar=True)
        self.assert_no_evidence()

    def test_declared_external_id_drift_during_freeze_is_rejected(self):
        from inventario.services_conteos import resolver_articulo_conteo as real_resolver

        def drift(key):
            frozen = real_resolver(key)
            if key.startswith("p"):
                PointProduct.objects.filter(pk=self.product.pk).update(
                    external_id="external-id-cambiado"
                )
            return frozen

        with patch(
            "inventario.management.commands.importar_conteos_sucursal_historicos.resolver_articulo_conteo",
            side_effect=drift,
        ), self.assertRaisesMessage(CommandError, "cambió"):
            self.run_import(aplicar=True)
        self.product.refresh_from_db()
        self.assertEqual(self.product.external_id, "hist-product")
        self.assert_no_evidence()

    def test_rejects_blank_negative_duplicate_and_non_four_branch_payloads(self):
        mutations = []
        blank = json.loads(json.dumps(self.payload))
        blank["sucursales"][0]["conteos"][0]["cantidad"] = ""
        mutations.append((blank, "cantidad"))
        negative = json.loads(json.dumps(self.payload))
        negative["sucursales"][0]["conteos"][0]["cantidad"] = "-1"
        mutations.append((negative, "cantidad"))
        duplicate = json.loads(json.dumps(self.payload))
        duplicate["sucursales"][0]["conteos"].append(
            duplicate["sucursales"][0]["conteos"][0]
        )
        mutations.append((duplicate, "duplicado"))
        only_three = json.loads(json.dumps(self.payload))
        only_three["sucursales"].pop()
        mutations.append((only_three, "cuatro"))
        for payload, message in mutations:
            with self.subTest(message=message), self.assertRaisesRegex(
                CommandError, f"(?i){message}"
            ):
                self.run_import(aplicar=True, payload=payload)
            self.assert_no_evidence()

    def test_actor_provenance_events_and_frozen_catalog_values(self):
        self.run_import(aplicar=True)
        count = ConteoSucursal.objects.get(sucursal=self.branches[0])
        self.assertEqual(
            list(count.eventos.values_list("action", flat=True)),
            ["importacion_historica", "enviar_importacion_historica"],
        )
        self.assertTrue(all(event.actor == self.actor for event in count.eventos.all()))
        imported = count.eventos.get(action="importacion_historica").payload
        self.assertEqual(imported["fuente"], self.payload["fuente"])
        self.assertEqual(imported["procedencia"]["archivo"], "conteos_octubre.xlsx")
        self.assertEqual(imported["procedencia"]["sha256_declarado"], "a" * 64)
        self.assertEqual(
            imported["procedencia"]["verificacion_externa"],
            self.payload["fuente"]["verificacion_externa"],
        )
        self.assertEqual(
            imported["procedencia"]["celdas"][0]["hoja"], self.branches[0].nombre
        )
        self.assertEqual(imported["procedencia"]["celdas"][0]["celda"], "B2")
        self.assertEqual(imported["resumen"]["lineas"], 2)
        product_line = count.lineas.get(producto=self.product)
        self.assertEqual(product_line.codigo, "SKU-HIST")
        self.assertEqual(product_line.nombre, "Producto histórico")
        self.assertEqual(product_line.unidad, "pza")
        self.assertTrue(product_line.fuente_unidad)
        reading = product_line.lecturas.get()
        self.assertEqual(reading.actor, self.actor)
        self.assertIn("importación histórica", reading.incidencia.lower())

        insumo_reading = count.lineas.get(insumo=self.insumo).lecturas.get()
        self.assertEqual(insumo_reading.ronda, 1)
        self.assertEqual(insumo_reading.cantidad, Decimal("1.250000"))
        self.assertIn("digest declarada", insumo_reading.incidencia)
        self.assertIn("a" * 64, insumo_reading.incidencia)
        self.assertIn(self.branches[0].nombre, insumo_reading.incidencia)
        self.assertIn("C3", insumo_reading.incidencia)

    def test_imported_events_are_immutable(self):
        self.run_import(aplicar=True)
        event = EventoConteoSucursal.objects.get(
            conteo__sucursal=self.branches[0], action="importacion_historica"
        )
        event.payload = {"alterado": True}
        with self.assertRaisesMessage(ValueError, "inmutables"):
            event.save()
        with self.assertRaisesMessage(ValueError, "no se eliminan"):
            event.delete()

    def test_apply_only_writes_count_evidence_not_point_or_stock(self):
        guarded = (
            MovimientoInventario,
            ExistenciaInsumo,
            PointInventorySnapshot,
            PointInsumoInventorySnapshot,
            PointSyncJob,
            PointProduct,
            Insumo,
        )
        before = {model: model.objects.count() for model in guarded}
        allowed = {
            ConteoSucursal._meta.db_table,
            LineaConteoSucursal._meta.db_table,
            LecturaConteoSucursal._meta.db_table,
            EventoConteoSucursal._meta.db_table,
        }
        with CaptureQueriesContext(connection) as queries:
            self.run_import(aplicar=True)
        for query in queries.captured_queries:
            sql = query["sql"]
            if not re.match(r"^\s*(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)", sql, re.I):
                continue
            match = re.match(
                r'^\s*(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+"?([a-z_][a-z_0-9]*)"?',
                sql,
                re.I,
            )
            self.assertIsNotNone(match, f"SQL mutante inesperado: {sql}")
            self.assertIn(match.group(1), allowed, f"Escritura fuera de evidencia: {sql}")
        self.assertEqual(before, {model: model.objects.count() for model in guarded})
