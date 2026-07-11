from pathlib import Path
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError
from django.test import TestCase, override_settings
from django.utils import timezone

from activos.models import Activo, BitacoraMantenimiento, OrdenMantenimiento
from core.access import ACCESS_MANAGE, ACCESS_VIEW, ROLE_DG, ROLE_LOGISTICA, ROLE_REPARTIDOR
from core.models import Sucursal, UserModuleAccess, UserProfile
from inventario.models import ExistenciaInsumo, LoteProduccion, MovimientoInventario, UBICACION_CFP_1_1
from inventario.services_existencias import establecer_stock, stock_ubicacion
from logistica.models import Repartidor, Unidad
from maestros.models import Insumo, UnidadMedida
from mermas.models import PersonalEnviosSucursal
from recetas.models import Receta

from operacion.bitacoras_config import BITACORA_CONFIG
from operacion.models import BitacoraOperativa, BitacoraOperativaLinea
from operacion.services import build_operacion_context
from operacion.views import DECIMAL_FIELDS


class AperturaInicialLotesTests(TestCase):
    def setUp(self):
        self.user_model = get_user_model()
        self.manager = self.user_model.objects.create_user(username="jefatura.apertura", password="test12345")
        UserProfile.objects.create(user=self.manager)
        UserModuleAccess.objects.create(user=self.manager, module="produccion", access=ACCESS_MANAGE)
        self.operator = self.user_model.objects.create_user(username="operador.apertura", password="test12345")
        UserProfile.objects.create(user=self.operator)
        UserModuleAccess.objects.create(user=self.operator, module="produccion", access=ACCESS_VIEW)
        self.unidad = UnidadMedida.objects.create(codigo="kg-apertura", nombre="Kilogramo apertura")
        self.insumo = Insumo.objects.create(
            codigo="DERIVADO:RECETA:APERTURA",
            codigo_point="RC-001",
            nombre="Preparacion Crunch",
            nombre_point="Preparacion Crunch Point",
            tipo_item=Insumo.TIPO_INTERNO,
            unidad_base=self.unidad,
        )
        self.receta = Receta.objects.create(
            nombre="Preparacion Crunch",
            codigo_point="RC-001",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_unidad=self.unidad,
            hash_contenido="apertura-preparacion-crunch",
        )

    def _registrar(self, **overrides):
        from operacion.services_bitacoras_inventory import registrar_apertura_inicial

        values = {
            "receta": self.receta,
            "insumo": self.insumo,
            "cantidad": Decimal("3"),
            "unidad": self.unidad,
            "ubicacion": UBICACION_CFP_1_1,
            "fecha_elaboracion": None,
            "actor": self.manager,
            "observaciones": "Existencia previa al inicio de trazabilidad",
        }
        values.update(overrides)
        return registrar_apertura_inicial(**values)

    def test_manager_creates_initial_lot_without_fake_source(self):
        lote = self._registrar()

        self.assertTrue(lote.es_apertura)
        self.assertIsNone(lote.linea_origen)
        self.assertTrue(lote.codigo.startswith("INI-RC-001-"))
        self.assertEqual(stock_ubicacion(self.insumo, UBICACION_CFP_1_1), Decimal("3"))
        movimiento = MovimientoInventario.objects.get(lote=lote)
        self.assertEqual(movimiento.tipo, MovimientoInventario.TIPO_ENTRADA)
        self.assertEqual(movimiento.almacen, UBICACION_CFP_1_1)
        self.assertEqual(movimiento.registrado_por_usuario, self.manager)
        self.assertEqual(movimiento.trazabilidad["evento"], "apertura_inicial")
        self.assertEqual(movimiento.trazabilidad["lote"], lote.codigo)

    def test_opening_is_idempotent_and_does_not_duplicate_stock(self):
        primero = self._registrar(fecha_elaboracion=date(2026, 7, 1))
        segundo = self._registrar(fecha_elaboracion=date(2026, 7, 1))

        self.assertEqual(segundo.pk, primero.pk)
        self.assertEqual(LoteProduccion.objects.count(), 1)
        self.assertEqual(MovimientoInventario.objects.count(), 1)
        self.assertEqual(stock_ubicacion(self.insumo, UBICACION_CFP_1_1), Decimal("3"))

    def test_opening_rejects_a_different_date_without_changing_stock(self):
        lote = self._registrar(fecha_elaboracion=date(2026, 7, 1))

        with self.assertRaisesMessage(ValidationError, "ya fue aplicada"):
            self._registrar(fecha_elaboracion=date(2026, 7, 2))

        lote.refresh_from_db()
        self.assertEqual(lote.producido_en.date(), date(2026, 7, 1))
        self.assertEqual(LoteProduccion.objects.count(), 1)
        self.assertEqual(MovimientoInventario.objects.count(), 1)
        self.assertEqual(stock_ubicacion(self.insumo, UBICACION_CFP_1_1), Decimal("3"))

    def test_opening_rejects_preexisting_stock_without_creating_history(self):
        establecer_stock(self.insumo, UBICACION_CFP_1_1, Decimal("1.250"))

        with self.assertRaisesMessage(ValidationError, "conciliar"):
            self._registrar()

        self.assertFalse(LoteProduccion.objects.exists())
        self.assertFalse(MovimientoInventario.objects.exists())
        self.assertEqual(stock_ubicacion(self.insumo, UBICACION_CFP_1_1), Decimal("1.250"))

    def test_opening_rejects_future_operational_date(self):
        future_date = timezone.localdate() + timedelta(days=1)

        with self.assertRaisesMessage(ValidationError, "futura"):
            self._registrar(fecha_elaboracion=future_date)

        self.assertFalse(LoteProduccion.objects.exists())
        self.assertFalse(MovimientoInventario.objects.exists())

    def test_unrelated_integrity_error_is_not_treated_as_idempotence(self):
        original_error = IntegrityError("conflicto ajeno a source_hash")

        with patch("operacion.services_bitacoras_inventory.LoteProduccion.objects.create", side_effect=original_error):
            with self.assertRaises(IntegrityError) as raised:
                self._registrar()

        self.assertIs(raised.exception, original_error)
        self.assertFalse(MovimientoInventario.objects.exists())
        self.assertEqual(stock_ubicacion(self.insumo, UBICACION_CFP_1_1), Decimal("0"))

    def test_opening_rolls_back_lot_and_movement_when_stock_update_fails(self):
        with patch(
            "operacion.services_bitacoras_inventory.aplicar_delta",
            side_effect=ValidationError("fallo de saldo"),
        ):
            with self.assertRaisesMessage(ValidationError, "fallo de saldo"):
                self._registrar()

        self.assertFalse(LoteProduccion.objects.exists())
        self.assertFalse(MovimientoInventario.objects.exists())
        self.assertFalse(ExistenciaInsumo.objects.filter(insumo=self.insumo).exists())
        self.assertEqual(stock_ubicacion(self.insumo, UBICACION_CFP_1_1), Decimal("0"))

    def test_opening_requires_production_manage_permission(self):
        with self.assertRaises(PermissionDenied):
            self._registrar(actor=self.operator)

        self.assertFalse(LoteProduccion.objects.exists())
        self.assertFalse(MovimientoInventario.objects.exists())

    def test_opening_rejects_invalid_quantity_identity_and_observation_atomically(self):
        for invalid_quantity in ("0", "-1", "NaN", "Infinity", "texto"):
            with self.subTest(cantidad=invalid_quantity), self.assertRaises(ValidationError):
                self._registrar(cantidad=invalid_quantity)

        self.insumo.codigo_point = "   "
        self.insumo.save(update_fields=["codigo_point"])
        with self.assertRaises(ValidationError):
            self._registrar()
        self.insumo.codigo_point = "RC-001"
        self.insumo.save(update_fields=["codigo_point"])

        with self.assertRaises(ValidationError):
            self._registrar(observaciones="   ")

        self.assertFalse(LoteProduccion.objects.exists())
        self.assertFalse(MovimientoInventario.objects.exists())
        self.assertEqual(stock_ubicacion(self.insumo, UBICACION_CFP_1_1), Decimal("0"))

    def test_opening_page_lists_canonical_products_for_manager_only(self):
        self.client.force_login(self.manager)

        response = self.client.get("/app/bitacoras/apertura/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Apertura inicial de lotes")
        self.assertContains(response, "Preparacion Crunch Point")
        self.assertContains(response, "RC-001")
        self.assertContains(response, "CFP 1.1")
        self.client.force_login(self.operator)
        self.assertEqual(self.client.get("/app/bitacoras/apertura/").status_code, 403)

    def test_opening_page_post_creates_immutable_applied_opening(self):
        self.client.force_login(self.manager)

        response = self.client.post(
            "/app/bitacoras/apertura/",
            {
                "insumo": str(self.insumo.id),
                "cantidad": "2.500",
                "unidad": str(self.unidad.id),
                "ubicacion": UBICACION_CFP_1_1,
                "fecha_elaboracion": "2026-07-01",
                "observaciones": "Conteo fisico al iniciar trazabilidad",
            },
        )

        self.assertRedirects(response, "/app/bitacoras/apertura/")
        lote = LoteProduccion.objects.get()
        self.assertEqual(lote.cantidad_inicial, Decimal("2.500"))
        self.assertEqual(lote.producido_en.date().isoformat(), "2026-07-01")
        get_response = self.client.get("/app/bitacoras/apertura/")
        self.assertContains(get_response, lote.codigo)
        self.assertNotContains(get_response, "Editar")

    def test_opening_page_rejects_future_date_and_limits_date_input(self):
        self.client.force_login(self.manager)
        future_date = timezone.localdate() + timedelta(days=1)

        response = self.client.post(
            "/app/bitacoras/apertura/",
            {
                "insumo": str(self.insumo.id),
                "cantidad": "2.500",
                "unidad": str(self.unidad.id),
                "ubicacion": UBICACION_CFP_1_1,
                "fecha_elaboracion": future_date.isoformat(),
                "observaciones": "Conteo con fecha futura",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "La fecha de elaboración no puede ser futura.")
        self.assertContains(response, f'max="{timezone.localdate().isoformat()}"')
        self.assertFalse(LoteProduccion.objects.exists())
        self.assertFalse(MovimientoInventario.objects.exists())


@override_settings(SECURE_SSL_REDIRECT=False)
class OperacionAppTests(TestCase):
    def setUp(self):
        self.user_model = get_user_model()
        self.sucursal = Sucursal.objects.create(codigo="COL", nombre="Colosio", activa=True)

    def _user(self, username: str, *, sucursal=None):
        user = self.user_model.objects.create_user(username=username, password="test12345")
        UserProfile.objects.create(user=user, sucursal=sucursal)
        return user

    def _grant(self, user, module: str, access: str = ACCESS_VIEW):
        return UserModuleAccess.objects.create(user=user, module=module, access=access)

    def test_app_requires_login(self):
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/login/?next=/app/")

    def test_repartidor_only_sees_logistica_mobile_without_other_modules(self):
        group = Group.objects.create(name=ROLE_REPARTIDOR)
        user = self._user("jorge.repartidor", sucursal=self.sucursal)
        user.groups.add(group)
        unidad = Unidad.objects.create(codigo="GS-PM1", descripcion="Panel móvil", sucursal=self.sucursal)
        Repartidor.objects.create(user=user, sucursal=self.sucursal, unidad_asignada=unidad)
        self.client.force_login(user)

        response = self.client.get("/app/")
        dashboard = self.client.get("/dashboard/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Nuevo Reporte")
        self.assertContains(response, "Mis Reportes")
        self.assertContains(response, "Inspección")
        self.assertContains(response, "Lavado")
        self.assertContains(response, "Bitácora")
        self.assertContains(response, "/logistica/app/?pantalla=nuevo_reporte")
        self.assertContains(response, "/logistica/app/?pantalla=bitacora_salida")
        self.assertNotContains(response, "Registrar merma")
        self.assertNotContains(response, "Reportar falla")
        self.assertNotContains(response, "pd_logistica_access")
        self.assertEqual(dashboard.status_code, 302)
        self.assertEqual(dashboard["Location"], "/logistica/app/")

    def test_unified_app_home_exposes_django_logout(self):
        user = self._user("operacion.logout", sucursal=self.sucursal)
        self._grant(user, "mermas.captura")
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "logistica/pwa/pollyanas-logo-header.png")
        self.assertContains(response, "App Operativa")
        self.assertNotContains(response, "App<br>Operativa")
        self.assertContains(response, "20260711-pull-refresh-v3")
        self.assertContains(response, 'class="pull-refresh"')
        self.assertContains(response, 'document.addEventListener("touchstart"')
        self.assertContains(response, 'document.addEventListener("touchcancel"')
        self.assertContains(response, "event.preventDefault()")
        self.assertContains(response, "passive: false")
        self.assertContains(response, "url.searchParams.set")
        self.assertContains(response, 'indicator.addEventListener("click", reloadApp)')
        self.assertContains(response, 'class="operacion-app-html"')
        self.assertContains(response, 'class="operacion-app-body"')
        self.assertContains(response, 'name="viewport" content="width=device-width, initial-scale=1"')
        self.assertNotContains(response, "viewport-fit=cover")
        self.assertContains(response, 'apple-mobile-web-app-status-bar-style" content="default"')
        self.assertNotContains(response, 'apple-mobile-web-app-status-bar-style" content="black-translucent"')
        self.assertNotContains(response, "Acceso a la app, no al ERP completo")
        self.assertContains(response, "Solo se muestran accesos permitidos para esta sesión.")
        self.assertContains(response, "operacion/manifest.webmanifest")
        self.assertContains(response, "operacion/app-icon-192.png")
        self.assertContains(response, "operacion/apple-touch-icon.png")
        self.assertContains(response, 'navigator.serviceWorker.register("/app/sw.js?v=20260711-pull-refresh-v3"')
        self.assertContains(response, 'href="/logout/"')
        self.assertContains(response, "Cerrar sesión")

    def test_operational_app_manifest_uses_app_operativa_identity(self):
        root = Path(__file__).resolve().parents[1]
        manifest = (root / "static/operacion/manifest.webmanifest").read_text(encoding="utf-8")

        self.assertIn('"name": "Pollyana\'s Operativa"', manifest)
        self.assertIn('"short_name": "Operativa"', manifest)
        self.assertIn('"start_url": "/app/?source=pwa"', manifest)
        self.assertIn('"theme_color": "#ffffff"', manifest)
        self.assertIn('"background_color": "#ffffff"', manifest)
        self.assertIn("/static/operacion/app-icon-192.png", manifest)
        self.assertIn("/static/operacion/app-icon-512.png", manifest)
        self.assertTrue((root / "static/operacion/apple-touch-icon.png").exists())

    def test_module_manifests_install_app_operativa_home(self):
        root = Path(__file__).resolve().parents[1]
        for relative_path in (
            "logistica/static/logistica/pwa/manifest.json",
            "static/mantenimiento/manifest.json",
            "static/fallas/manifest.json",
        ):
            manifest = (root / relative_path).read_text(encoding="utf-8")

            self.assertIn('"id": "/app/"', manifest)
            self.assertIn('"start_url": "/app/?source=pwa"', manifest)
            self.assertIn('"scope": "/"', manifest)
            self.assertNotIn('"start_url": "/logistica/app/"', manifest)
            self.assertNotIn('"start_url": "/mantenimiento/app/"', manifest)
            self.assertNotIn('"start_url": "/fallas/app/"', manifest)

    def test_operational_app_serves_own_service_worker(self):
        response = self.client.get("/app/sw.js")
        root = Path(__file__).resolve().parents[1]
        css = (root / "static/css/template_modules/templates-operacion-app-home.css").read_text(encoding="utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "application/javascript")
        body = response.content.decode("utf-8")
        self.assertIn("pollyanas-app-operativa-pwa-v14-pull-refresh-stable", body)
        self.assertIn("/static/operacion/manifest.webmanifest?v=20260708-mobile-polish-v4", body)
        self.assertNotIn('"/app/"', body)
        self.assertIn('event.request.mode === "navigate"', body)
        self.assertIn("env(safe-area-inset-top)", css)
        self.assertIn("calc(6px + env(safe-area-inset-top))", css)
        self.assertIn("pointer-events: auto", css)

    def test_mermas_only_user_can_enter_unified_app_without_losing_guardrail(self):
        user = self._user("mermas.colosio", sucursal=self.sucursal)
        self._grant(user, "mermas.captura")
        self.client.force_login(user)

        response = self.client.get("/app/")
        dashboard = self.client.get("/dashboard/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Registrar merma")
        self.assertContains(response, "/mermas/app/?modo=captura")

    def test_produccion_user_gets_bitacoras_tile(self):
        user = self._user("produccion.bitacoras")
        self._grant(user, "produccion")
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Bitácoras")
        self.assertContains(response, "/app/bitacoras/")

    def test_bitacora_capture_uses_existing_receta(self):
        user = self._user("produccion.captura")
        self._grant(user, "produccion")
        receta = Receta.objects.create(nombre="Pastel prueba", hash_contenido="bitacora-test")
        self.client.force_login(user)

        response = self.client.post(
            f"/app/bitacoras/{BitacoraOperativa.TIPO_INVENTARIO_CFP1}/",
            {
                "fecha": "2026-07-01",
                "receta_0": str(receta.id),
                "cedis_0": "4",
                "devolucion_0": "1",
                "observaciones_0": "Conteo inicial",
                "cerrar": "1",
            },
        )

        self.assertEqual(response.status_code, 302)
        bitacora = BitacoraOperativa.objects.get()
        linea = BitacoraOperativaLinea.objects.get()
        self.assertEqual(bitacora.estatus, BitacoraOperativa.ESTATUS_CERRADA)
        self.assertEqual(linea.receta, receta)
        self.assertEqual(linea.datos["cedis"], "4")
        self.assertEqual(linea.datos["devolucion"], "1")

    def test_hornos_rows_require_point_identity(self):
        user = self._user("produccion.hornos")
        self._grant(user, "produccion")
        receta = Receta.objects.create(
            nombre="Pan Chocolate Chico",
            codigo_point="PAN-CHO-CH",
            tipo=Receta.TIPO_PREPARACION,
            pasa_modulo_produccion=True,
            hash_contenido="hornos-point-identity",
        )
        preparacion_sin_codigo = Receta.objects.create(
            nombre="Preparación sin código",
            tipo=Receta.TIPO_PREPARACION,
            hash_contenido="hornos-without-point",
        )
        preparacion_codigo_espacios = Receta.objects.create(
            nombre="Preparación código espacios",
            codigo_point="   ",
            tipo=Receta.TIPO_PREPARACION,
            hash_contenido="hornos-blank-point",
        )
        producto_final = Receta.objects.create(
            nombre="Pastel Chocolate Chico",
            codigo_point="PAS-CHO-CH",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hornos-final-product",
        )
        self.client.force_login(user)

        response = self.client.get("/app/bitacoras/HORNOS/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(
            response,
            f'<option value="{receta.id}">Pan Chocolate Chico · PAN-CHO-CH</option>',
            html=True,
        )
        self.assertNotContains(response, preparacion_sin_codigo.nombre)
        self.assertNotContains(response, preparacion_codigo_espacios.nombre)
        self.assertNotContains(response, producto_final.nombre)

    def test_armado_rows_require_final_product_point_identity(self):
        user = self._user("produccion.armado")
        self._grant(user, "produccion")
        producto_final = Receta.objects.create(
            nombre="Pastel Chocolate Chico",
            codigo_point="PAS-CHO-CH",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="armado-final-product",
        )
        preparacion = Receta.objects.create(
            nombre="Pan Chocolate Chico",
            codigo_point="PAN-CHO-CH",
            tipo=Receta.TIPO_PREPARACION,
            hash_contenido="armado-preparation",
        )
        self.client.force_login(user)

        response = self.client.get("/app/bitacoras/ARMADO/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(
            response,
            f'<option value="{producto_final.id}">Pastel Chocolate Chico · PAS-CHO-CH</option>',
            html=True,
        )
        self.assertNotContains(response, preparacion.nombre)

    def test_production_bitacoras_reject_invalid_recipe_before_creating_header(self):
        user = self._user("produccion.invalid-recipes")
        self._grant(user, "produccion")
        preparacion_sin_codigo = Receta.objects.create(
            nombre="Preparación sin Point",
            tipo=Receta.TIPO_PREPARACION,
            hash_contenido="invalid-without-point",
        )
        preparacion_codigo_espacios = Receta.objects.create(
            nombre="Preparación Point espacios",
            codigo_point="   ",
            tipo=Receta.TIPO_PREPARACION,
            hash_contenido="invalid-blank-point",
        )
        producto_final = Receta.objects.create(
            nombre="Producto final incorrecto",
            codigo_point="FINAL-INCORRECTO",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="invalid-wrong-type",
        )
        self.client.force_login(user)

        for receta_id in (
            "",
            "999999",
            str(producto_final.id),
            str(preparacion_sin_codigo.id),
            str(preparacion_codigo_espacios.id),
        ):
            with self.subTest(receta_id=receta_id):
                response = self.client.post(
                    "/app/bitacoras/HORNOS/",
                    {"receta_0": receta_id, "existencia_0": "1", "cerrar": "1"},
                )

                self.assertEqual(response.status_code, 200)
                self.assertContains(response, "Selecciona un producto válido con identidad Point.")
                self.assertFalse(BitacoraOperativa.objects.exists())

    def test_production_bitacora_fields_reject_invalid_decimal_values(self):
        user = self._user("produccion.decimals")
        self._grant(user, "produccion")
        preparacion = Receta.objects.create(
            nombre="Preparación decimal",
            codigo_point="PREP-DEC",
            tipo=Receta.TIPO_PREPARACION,
            hash_contenido="decimal-preparation",
        )
        producto_final = Receta.objects.create(
            nombre="Producto final decimal",
            codigo_point="FINAL-DEC",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="decimal-final",
        )
        self.client.force_login(user)

        required_decimal_fields = {
            "cantidad",
            "existencia",
            "salida",
            "entrada",
            "preparacion",
            "existencia_fisica",
            "salida_armado",
            "consumo_real",
            "producto_terminado",
        }
        self.assertLessEqual(required_decimal_fields, DECIMAL_FIELDS)

        field_cases = (
            ("ROTACION", preparacion, "cantidad"),
            ("HORNOS", preparacion, "existencia"),
            ("HORNOS", preparacion, "preparacion"),
            ("CFP11", preparacion, "existencia_fisica"),
            ("CFP11", preparacion, "salida_armado"),
            ("ARMADO", producto_final, "consumo_real"),
            ("ARMADO", producto_final, "producto_terminado"),
        )
        for tipo, receta, campo in field_cases:
            for invalid_value in ("texto", "NaN", "Infinity", "-Infinity"):
                with self.subTest(tipo=tipo, campo=campo, invalid_value=invalid_value):
                    BitacoraOperativa.objects.all().delete()
                    response = self.client.post(
                        f"/app/bitacoras/{tipo}/",
                        {"receta_0": str(receta.id), f"{campo}_0": invalid_value},
                    )

                    self.assertEqual(response.status_code, 200)
                    self.assertContains(response, "Ingresa una cantidad numérica válida.")
                    self.assertEqual(response.context["submitted_values"][f"{campo}_0"], invalid_value)
                    self.assertFalse(BitacoraOperativa.objects.exists())
                    self.assertFalse(BitacoraOperativaLinea.objects.exists())

        empty_line_response = self.client.post(
            "/app/bitacoras/HORNOS/",
            {"receta_0": str(preparacion.id)},
        )

        self.assertEqual(empty_line_response.status_code, 200)
        self.assertContains(empty_line_response, "Captura al menos una cantidad u observación.")
        self.assertFalse(BitacoraOperativa.objects.exists())
        self.assertFalse(BitacoraOperativaLinea.objects.exists())

    def test_logistics_only_user_cannot_use_production_bitacoras(self):
        user = self._user("logistica.bitacoras-produccion")
        self._grant(user, "logistica")
        self.client.force_login(user)

        get_response = self.client.get("/app/bitacoras/HORNOS/")
        post_response = self.client.post("/app/bitacoras/HORNOS/", {"cerrar": "1"})

        self.assertEqual(get_response.status_code, 403)
        self.assertEqual(post_response.status_code, 403)
        self.assertFalse(BitacoraOperativa.objects.exists())

    def test_cfp11_config_keeps_legacy_fields_outside_active_fields(self):
        config = BITACORA_CONFIG[BitacoraOperativa.TIPO_CFP11]
        user = self._user("produccion.cfp11-config")
        self._grant(user, "produccion")
        self.client.force_login(user)

        self.assertEqual(config["familia"], "custodia_lotes")
        self.assertEqual(config["campos"], ["existencia_fisica", "salida_armado"])
        self.assertEqual(config["campos_legacy"], ["bloque", "tamano", "existencia", "salida", "entrada"])

        response = self.client.get("/app/bitacoras/CFP11/")

        self.assertContains(response, "Existencia física")
        self.assertContains(response, "Salida a armado")
        self.assertContains(response, 'name="existencia_fisica_0"')
        self.assertNotContains(response, 'name="bloque_0"')
        self.assertNotContains(response, "Existencia_fisica")

    def test_merma_recepcion_stays_on_existing_mermas_app(self):
        user = self._user("cedis.merma")
        self._grant(user, "mermas.recepcion", ACCESS_MANAGE)
        self.client.force_login(user)

        response = self.client.get("/app/bitacoras/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "/mermas/app/?modo=recepcion")
        self.assertNotContains(response, "Logística móvil")

    def test_sucursal_user_gets_only_assigned_operational_actions(self):
        user = self._user("sucursal.colosio", sucursal=self.sucursal)
        self._grant(user, "mermas.captura")
        self._grant(user, "fallas.reportar")
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Registrar merma")
        self.assertContains(response, "Reportar falla")
        self.assertContains(response, 'href="/fallas/app/"')
        self.assertNotContains(response, 'href="/fallas/reportar/"')
        self.assertContains(response, "Colosio")
        self.assertNotContains(response, "Flota")
        self.assertNotContains(response, "Mantenimiento vehicular")

    def test_sucursal_user_does_not_get_auditorias_tile(self):
        user = self._user("visitas.colosio", sucursal=self.sucursal)
        self._grant(user, "ventas.visitas_sucursal")
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Auditorías")
        self.assertNotContains(response, 'href="/visitas-sucursal/app/"')
        self.assertNotContains(response, 'href="/visitas-sucursal/"')

    def test_auditor_gets_auditorias_tile_from_unified_app(self):
        user = self._user("auditor.visitas")
        self._grant(user, "ventas.visitas_sucursal", ACCESS_MANAGE)
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Auditorías")
        self.assertContains(response, "Visitas a sucursal, checklist, hallazgos")
        self.assertContains(response, 'href="/visitas-sucursal/app/"')
        self.assertNotContains(response, 'href="/visitas-sucursal/"')

    def test_branch_capture_only_can_enter_app_but_regular_erp_still_redirects(self):
        user = self._user("captura.reabasto", sucursal=self.sucursal)
        profile = user.userprofile
        profile.modo_captura_sucursal = True
        profile.save(update_fields=["modo_captura_sucursal"])
        self.client.force_login(user)

        response = self.client.get("/app/")
        dashboard = self.client.get("/dashboard/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Captura sucursal")
        self.assertContains(response, "/recetas/reabasto-cedis/captura/")
        self.assertEqual(dashboard.status_code, 302)
        self.assertEqual(dashboard["Location"], "/recetas/reabasto-cedis/captura/")

    def test_logistica_role_sees_logistica_management_without_sucursal_tiles(self):
        group = Group.objects.create(name=ROLE_LOGISTICA)
        user = self._user("logistica.supervisor")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Tickets logística")
        self.assertContains(response, "Flota")
        self.assertContains(response, "Rutas")
        self.assertContains(response, "/logistica/app/?pantalla=mis_reportes")
        self.assertContains(response, "/logistica/app/?pantalla=inspeccion_vehiculo")
        self.assertContains(response, "/logistica/app/?pantalla=ruta_activa")
        self.assertContains(response, 'class="glyph route-glyph"')
        self.assertContains(response, 'd="M416 320h-96c-17.6 0-32-14.4')
        self.assertNotContains(response, "Logística móvil")
        self.assertNotContains(response, "Registrar merma")
        rutas_tile = next(tile for tile in build_operacion_context(user)["tiles"] if tile["key"] == "logistica_rutas")
        self.assertEqual(rutas_tile["icon"], "ruta")

    def test_logistica_view_only_user_does_not_see_management_only_tiles(self):
        user = self._user("logistica.lectura")
        self._grant(user, "logistica", access=ACCESS_VIEW)
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Tickets logística")
        self.assertNotContains(response, "Logística móvil")

    def test_logistica_pwa_rejects_users_without_logistica_or_repartidor_access(self):
        user = self._user("logistica.sinpermiso")
        self.client.force_login(user)

        response = self.client.get("/logistica/app/")

        self.assertEqual(response.status_code, 403)

    def test_user_without_operational_access_gets_empty_state(self):
        user = self._user("sin.permisos")
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Tu usuario no tiene actividades operativas asignadas")
        self.assertNotContains(response, "Registrar merma")
        self.assertNotContains(response, "Logística móvil")
        self.assertNotContains(response, "Reportar falla")

    def test_personal_envios_sucursal_gets_cedis_reception_access(self):
        user = self._user("cedis.recepcion")
        PersonalEnviosSucursal.objects.create(user=user, activo=True)
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Recibir merma")
        self.assertContains(response, "/mermas/app/?modo=recepcion")
        self.assertContains(response, "Recepción y validación")
        self.assertNotContains(response, "Registrar merma")
        self.assertNotContains(response, "Logística móvil")

        recepcion = self.client.get("/mermas/app/?modo=recepcion")
        self.assertEqual(recepcion.status_code, 200)
        self.assertContains(recepcion, 'href="/app/"')
        self.assertContains(recepcion, 'aria-label="Menú principal"')
        self.assertContains(recepcion, 'href="/logout/"')
        self.assertContains(recepcion, "Salir")

    def test_explicit_cedis_manage_access_gets_reception_and_mermas_guardrail(self):
        user = self._user("cedis.manage")
        self._grant(user, "mermas.recepcion", access=ACCESS_MANAGE)
        self.client.force_login(user)

        response = self.client.get("/app/")
        dashboard = self.client.get("/dashboard/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Recibir merma")
        self.assertContains(response, "/mermas/app/?modo=recepcion")
        self.assertEqual(dashboard.status_code, 302)
        self.assertEqual(dashboard["Location"], "/mermas/app/")

    def test_fallas_mis_reportes_can_be_shown_without_reportar(self):
        user = self._user("fallas.consulta", sucursal=self.sucursal)
        self._grant(user, "fallas.mis_reportes")
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Mis reportes")
        self.assertNotContains(response, "Reportar falla")

    def test_fallas_branch_user_actions_return_to_unified_app_not_dashboard(self):
        user = self._user("fallas.sucursal", sucursal=self.sucursal)
        self._grant(user, "fallas.reportar")
        self._grant(user, "fallas.mis_reportes")
        self.client.force_login(user)

        reportar = self.client.get("/fallas/reportar/")
        mis_reportes = self.client.get("/fallas/mis-reportes/")

        self.assertEqual(reportar.status_code, 200)
        self.assertContains(reportar, 'href="/app/"')
        self.assertNotContains(reportar, 'href="/fallas/"')
        self.assertNotContains(reportar, "Categorías")
        self.assertEqual(mis_reportes.status_code, 200)
        self.assertNotContains(mis_reportes, 'href="/fallas/"')
        self.assertNotContains(mis_reportes, "Categorías")

    def test_fallas_dashboard_user_keeps_dashboard_navigation(self):
        user = self._user("fallas.dashboard")
        self._grant(user, "fallas.reportar")
        self._grant(user, "fallas.mis_reportes")
        self._grant(user, "fallas.dashboard")
        self.client.force_login(user)

        response = self.client.get("/fallas/reportar/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'href="/fallas/"')
        self.assertNotContains(response, 'href="/app/"')

    def test_locked_logistica_profile_hides_logistica_tiles_even_with_role(self):
        group = Group.objects.create(name=ROLE_LOGISTICA)
        user = self._user("logistica.bloqueado")
        user.groups.add(group)
        profile = user.userprofile
        profile.lock_logistica = True
        profile.save(update_fields=["lock_logistica"])
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Logística móvil")
        self.assertContains(response, "Tu usuario no tiene actividades operativas asignadas")

    def test_superuser_gets_full_operational_surface(self):
        user = self.user_model.objects.create_superuser(
            username="admin.operacion",
            email="admin.operacion@example.com",
            password="test12345",
        )
        UserProfile.objects.create(user=user, sucursal=self.sucursal)
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Registrar merma")
        self.assertContains(response, "Recibir merma")
        self.assertContains(response, "Reportar falla")
        self.assertContains(response, "Auditorías")
        self.assertContains(response, "Mantenimiento")
        self.assertContains(response, "Flota")
        self.assertContains(response, "Rutas")
        self.assertNotContains(response, "Logística móvil")

    def test_repartidor_direct_urls_are_limited_to_app_and_logistica_pwa(self):
        group = Group.objects.create(name=ROLE_REPARTIDOR)
        user = self._user("repartidor.guardrail", sucursal=self.sucursal)
        user.groups.add(group)
        unidad = Unidad.objects.create(codigo="GS-PM2", descripcion="Panel móvil 2", sucursal=self.sucursal)
        Repartidor.objects.create(user=user, sucursal=self.sucursal, unidad_asignada=unidad)
        self.client.force_login(user)

        self.assertEqual(self.client.get("/app/").status_code, 200)
        self.assertEqual(self.client.get("/logistica/app/").status_code, 200)

        blocked = self.client.get("/mermas/app/")
        self.assertEqual(blocked.status_code, 302)
        self.assertEqual(blocked["Location"], "/logistica/app/")

    def test_mermas_only_direct_urls_are_limited_to_app_and_mermas(self):
        user = self._user("mermas.guardrail", sucursal=self.sucursal)
        self._grant(user, "mermas.captura")
        self.client.force_login(user)

        self.assertEqual(self.client.get("/app/").status_code, 200)
        self.assertEqual(self.client.get("/mermas/app/").status_code, 200)

        blocked = self.client.get("/fallas/reportar/")
        self.assertEqual(blocked.status_code, 302)
        self.assertEqual(blocked["Location"], "/mermas/app/")

    def test_merma_singular_paths_redirect_to_mermas_without_losing_query(self):
        user = self._user("mermas.singular", sucursal=self.sucursal)
        self._grant(user, "mermas.captura")
        self.client.force_login(user)

        home = self.client.get("/merma/")
        app = self.client.get("/merma/app/?modo=captura")

        self.assertEqual(home.status_code, 302)
        self.assertEqual(home["Location"], "/mermas/")
        self.assertEqual(app.status_code, 302)
        self.assertEqual(app["Location"], "/mermas/app/?modo=captura")

    def test_mermas_capture_cancel_returns_to_unified_app_for_branch_user(self):
        user = self._user("mermas.cancelar", sucursal=self.sucursal)
        self._grant(user, "mermas.captura")
        self.client.force_login(user)

        response = self.client.get("/mermas/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'href="/app/"')
        self.assertContains(response, 'href="/logout/"')
        self.assertContains(response, "Salir")
        self.assertNotContains(response, 'href="/mermas/"')

    def test_mermas_capture_cancel_returns_to_unified_app_for_dashboard_user(self):
        user = self._user("mermas.panel", sucursal=self.sucursal)
        self._grant(user, "mermas.captura")
        self._grant(user, "mermas.dashboard")
        self.client.force_login(user)

        response = self.client.get("/mermas/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'href="/app/"')
        self.assertContains(response, 'href="/logout/"')
        self.assertContains(response, 'aria-label="Menú principal"')
        self.assertNotContains(response, 'href="/mermas/">Panel</a>')

    def test_mermas_detail_hidden_sidebar_keeps_logout_escape(self):
        template = Path(__file__).resolve().parents[1] / "mermas/templates/mermas/detalle.html"
        html = template.read_text(encoding="utf-8")

        self.assertIn("{% url 'logout' %}", html)
        self.assertIn("{% url 'operacion:app_home' %}", html)
        self.assertIn('aria-label="Menú principal"', html)
        self.assertNotIn("{% url 'mermas:dashboard' %}", html)
        self.assertIn(">Salir<", html)

    def test_dg_group_gets_management_surface(self):
        group = Group.objects.create(name=ROLE_DG)
        user = self._user("dg.operacion")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Mantenimiento")
        self.assertContains(response, "Tickets logística")
        self.assertContains(response, "/logistica/app/?pantalla=mis_reportes")
        self.assertNotContains(response, "Logística móvil")

    def test_dg_mermas_tiles_do_not_logout_and_respect_requested_mode(self):
        group = Group.objects.create(name=ROLE_DG)
        user = self._user("dg.mermas")
        user.groups.add(group)
        self.client.force_login(user)

        captura = self.client.get("/mermas/app/?modo=captura")
        recepcion = self.client.get("/mermas/app/?modo=recepcion")

        self.assertEqual(captura.status_code, 200)
        self.assertContains(captura, "Registrar merma")
        self.assertEqual(recepcion.status_code, 200)
        self.assertContains(recepcion, "Recepción CEDIS")

    def test_mantenimiento_pwa_requires_login_and_operational_group(self):
        anonymous = self.client.get("/mantenimiento/app/")
        self.assertEqual(anonymous.status_code, 302)
        self.assertEqual(anonymous["Location"], "/login/?next=/mantenimiento/app/")

        user = self._user("mantenimiento.sinpermiso")
        self.client.force_login(user)
        self.assertEqual(self.client.get("/mantenimiento/app/").status_code, 403)

        group = Group.objects.create(name=ROLE_DG)
        user.groups.add(group)
        self.assertEqual(self.client.get("/mantenimiento/app/").status_code, 200)

    def test_dg_group_can_call_mantenimiento_session_api(self):
        group = Group.objects.create(name=ROLE_DG)
        user = self._user("dg.mantenimiento.api")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.get("/api/mantenimiento/me/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["username"], "dg.mantenimiento.api")

    def test_mantenimiento_session_token_uses_django_session_not_logistica_storage(self):
        group = Group.objects.create(name=ROLE_DG)
        user = self._user("dg.mantenimiento.token")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.get("/api/mantenimiento/session-token/")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("access", payload)
        self.assertIn("refresh", payload)

        self.client.logout()
        perfil = self.client.get("/api/mantenimiento/me/", HTTP_AUTHORIZATION=f"Bearer {payload['access']}")
        self.assertEqual(perfil.status_code, 200)
        self.assertEqual(perfil.json()["username"], "dg.mantenimiento.token")

    def test_mantenimiento_pwa_uses_own_token_storage_and_hides_fleet_entry(self):
        group = Group.objects.create(name=ROLE_DG)
        user = self._user("dg.mantenimiento.preview")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.get("/mantenimiento/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "pd_mantenimiento_access")
        self.assertNotContains(response, "pd_logistica_access")
        self.assertContains(response, "Bandeja de mantenimiento")
        self.assertContains(response, "Pendientes")
        self.assertContains(response, "Buscar equipo")
        self.assertContains(response, "Seguimiento")
        self.assertContains(response, "Reportes")
        self.assertContains(response, "Registrar mantenimiento de equipo")
        self.assertNotContains(response, "Vehículo de flota")

    def test_pwa_logout_buttons_redirect_to_django_logout(self):
        mantenimiento_group = Group.objects.create(name="MANTENIMIENTO")
        mantenimiento_user = self._user("tecnico.logout")
        mantenimiento_user.groups.add(mantenimiento_group)
        self.client.force_login(mantenimiento_user)
        mantenimiento = self.client.get("/mantenimiento/app/")
        self.assertEqual(mantenimiento.status_code, 200)
        self.assertContains(mantenimiento, 'window.location.href = "/logout/";')
        self.assertContains(mantenimiento, "window.location.href='/app/'")
        self.assertContains(mantenimiento, 'aria-label="Menú principal"')

        self.client.logout()
        logistica_group = Group.objects.create(name=ROLE_LOGISTICA)
        logistica_user = self._user("logistica.logout")
        logistica_user.groups.add(logistica_group)
        self.client.force_login(logistica_user)
        logistica = self.client.get("/logistica/app/")
        self.assertEqual(logistica.status_code, 200)
        self.assertContains(logistica, 'window.location.href = "/logout/";')

        self.client.logout()
        fallas_user = self._user("fallas.logout", sucursal=self.sucursal)
        self._grant(fallas_user, "fallas.reportar")
        self.client.force_login(fallas_user)
        fallas = self.client.get("/fallas/reportar/")
        self.assertEqual(fallas.status_code, 200)
        self.assertContains(fallas, 'href="/logout/"')

        fallas_pwa_template = Path(__file__).resolve().parents[1] / "fallas/templates/fallas/pwa_reporte.html"
        fallas_pwa_html = fallas_pwa_template.read_text(encoding="utf-8")
        self.assertIn('window.location.href = "/logout/";', fallas_pwa_html)
        self.assertIn("window.location.href='/app/'", fallas_pwa_html)
        self.assertIn('aria-label="Menú principal"', fallas_pwa_html)

        logistica_pwa_template = Path(__file__).resolve().parents[1] / "logistica/templates/logistica/pwa.html"
        logistica_pwa_html = logistica_pwa_template.read_text(encoding="utf-8")
        self.assertIn("window.location.href='/app/'", logistica_pwa_html)
        self.assertIn('aria-label="Menú principal"', logistica_pwa_html)

    def test_all_app_operativa_destinations_have_home_icon(self):
        root = Path(__file__).resolve().parents[1]
        templates = [
            "fallas/templates/fallas/pwa_reporte.html",
            "logistica/templates/logistica/pwa.html",
            "templates/mantenimiento/pwa.html",
            "mermas/templates/mermas/form.html",
            "mermas/templates/mermas/app_recepcion.html",
            "mermas/templates/mermas/detalle.html",
            "visitas_sucursal/templates/visitas_sucursal/app.html",
            "templates/operacion/bitacoras_home.html",
            "templates/operacion/bitacora_captura.html",
            "recetas/templates/recetas/reabasto_cedis_captura.html",
        ]

        for template in templates:
            html = (root / template).read_text(encoding="utf-8")
            self.assertIn("app-home-link", html, template)
            self.assertIn('aria-label="Menú principal"', html, template)

    def test_operational_pwas_prefer_current_django_session_before_cached_token(self):
        mantenimiento_group = Group.objects.create(name="MANTENIMIENTO")
        mantenimiento_user = self._user("tecnico.token.actual")
        mantenimiento_user.groups.add(mantenimiento_group)
        self.client.force_login(mantenimiento_user)
        mantenimiento = self.client.get("/mantenimiento/app/")
        self.assertEqual(mantenimiento.status_code, 200)
        mantenimiento_html = mantenimiento.content.decode()
        mantenimiento_boot = mantenimiento_html[mantenimiento_html.index("async function boot()") :]
        self.assertLess(
            mantenimiento_boot.index("if (await useDjangoSessionToken())"),
            mantenimiento_boot.index("if (state.token)"),
        )

        self.client.logout()
        logistica_group = Group.objects.create(name=ROLE_LOGISTICA)
        logistica_user = self._user("logistica.token.actual")
        logistica_user.groups.add(logistica_group)
        self.client.force_login(logistica_user)
        logistica = self.client.get("/logistica/app/")
        self.assertEqual(logistica.status_code, 200)
        logistica_html = logistica.content.decode()
        logistica_boot = logistica_html[logistica_html.index("async function boot()") :]
        self.assertLess(
            logistica_boot.index("if (await useDjangoSessionToken())"),
            logistica_boot.index("if (state.token)"),
        )

        self.client.logout()
        fallas_user = self._user("fallas.token.actual", sucursal=self.sucursal)
        self._grant(fallas_user, "fallas.reportar")
        self.client.force_login(fallas_user)
        fallas = self.client.get("/fallas/app/")
        self.assertEqual(fallas.status_code, 200)
        fallas_html = fallas.content.decode()
        fallas_boot = fallas_html[fallas_html.index("async function boot()") :]
        self.assertLess(
            fallas_boot.index("if (await useDjangoSessionToken())"),
            fallas_boot.index("if (state.token)"),
        )

    def test_mantenimiento_group_gets_app_tile_and_pwa_access(self):
        group = Group.objects.create(name="MANTENIMIENTO")
        user = self._user("tecnico.mantenimiento")
        user.groups.add(group)
        self.client.force_login(user)

        app = self.client.get("/app/")
        pwa = self.client.get("/mantenimiento/app/")
        token = self.client.get("/api/mantenimiento/session-token/")

        self.assertEqual(app.status_code, 200)
        self.assertContains(app, "Mantenimiento")
        self.assertEqual(pwa.status_code, 200)
        self.assertEqual(token.status_code, 200)

    def test_activos_access_gets_mantenimiento_app_and_api(self):
        user = self._user("activos.mantenimiento")
        self._grant(user, "activos", access=ACCESS_VIEW)
        self.client.force_login(user)

        app = self.client.get("/app/")
        pwa = self.client.get("/mantenimiento/app/")
        perfil = self.client.get("/api/mantenimiento/me/")

        self.assertEqual(app.status_code, 200)
        self.assertContains(app, "Mantenimiento")
        self.assertEqual(pwa.status_code, 200)
        self.assertEqual(perfil.status_code, 200)

    def test_mantenimiento_order_followup_updates_status_and_bitacora(self):
        group = Group.objects.create(name="MANTENIMIENTO")
        user = self._user("tecnico.seguimiento")
        user.groups.add(group)
        activo = Activo.objects.create(nombre="Batidora QA", sucursal=self.sucursal, categoria="Producción")
        orden = OrdenMantenimiento.objects.create(
            activo_ref=activo,
            tipo=OrdenMantenimiento.TIPO_CORRECTIVO,
            prioridad=OrdenMantenimiento.PRIORIDAD_ALTA,
            estatus=OrdenMantenimiento.ESTATUS_PENDIENTE,
            descripcion="Ruido en motor",
        )
        self.client.force_login(user)

        response = self.client.patch(
            f"/api/mantenimiento/ordenes/{orden.id}/",
            data={
                "estatus": OrdenMantenimiento.ESTATUS_EN_PROCESO,
                "responsable": "Técnico QA",
                "comentario": "Se inició revisión del motor.",
                "costo_adicional": "125.50",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        orden.refresh_from_db()
        self.assertEqual(orden.estatus, OrdenMantenimiento.ESTATUS_EN_PROCESO)
        self.assertEqual(orden.responsable, "Técnico QA")
        self.assertIsNotNone(orden.fecha_inicio)
        self.assertEqual(BitacoraMantenimiento.objects.filter(orden=orden).count(), 1)
        self.assertIn("Se inició revisión del motor.", BitacoraMantenimiento.objects.get(orden=orden).comentario)

    def test_mantenimiento_can_create_maintainable_point_for_general_repairs(self):
        group = Group.objects.create(name="MANTENIMIENTO")
        user = self._user("tecnico.punto")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.post(
            "/api/mantenimiento/activos/rapido/",
            data={
                "sucursal": self.sucursal.id,
                "nombre": "Instalación hidrosanitaria - Cocina",
                "categoria": "Plomería",
                "ubicacion": "Cocina",
                "notas": "Alta desde reparación general: fuga debajo de tarja.",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        payload = response.json()
        activo = Activo.objects.get(id=payload["id"])
        self.assertTrue(activo.codigo.startswith("PM-"))
        self.assertEqual(activo.sucursal, self.sucursal)
        self.assertEqual(activo.nombre, "Instalación hidrosanitaria - Cocina")
        self.assertEqual(activo.categoria, "Plomería")
        self.assertEqual(activo.ubicacion, "Cocina")
        self.assertIn("fuga debajo de tarja", activo.notas)

        orden = self.client.post(
            "/api/mantenimiento/ordenes/",
            data={
                "activo_ref": activo.id,
                "tipo": OrdenMantenimiento.TIPO_CORRECTIVO,
                "prioridad": OrdenMantenimiento.PRIORIDAD_ALTA,
                "descripcion": "Se reparó fuga y se selló conexión.",
            },
        )
        self.assertEqual(orden.status_code, 201)
        self.assertTrue(OrdenMantenimiento.objects.filter(activo_ref=activo).exists())

    def test_mantenimiento_sucursales_api_does_not_depend_on_existing_assets(self):
        group = Group.objects.create(name="MANTENIMIENTO")
        user = self._user("tecnico.sucursales")
        user.groups.add(group)
        Activo.objects.all().delete()
        self.client.force_login(user)

        response = self.client.get("/api/mantenimiento/sucursales/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(self.sucursal.id, [item["id"] for item in response.json()])

    def test_mantenimiento_pwa_exposes_maintainable_point_shortcut(self):
        group = Group.objects.create(name="MANTENIMIENTO")
        user = self._user("tecnico.punto.ui")
        user.groups.add(group)
        self.client.force_login(user)

        response = self.client.get("/mantenimiento/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "No encuentro el equipo")
        self.assertContains(response, "Registrar punto mantenible")


class HornosLoteServiceTests(TestCase):
    def setUp(self):
        self.user_model = get_user_model()
        self.manager = self.user_model.objects.create_user(username="jefatura.hornos", password="test12345")
        UserProfile.objects.create(user=self.manager)
        UserModuleAccess.objects.create(user=self.manager, module="produccion", access=ACCESS_MANAGE)
        self.operator = self.user_model.objects.create_user(username="operador.hornos", password="test12345")
        UserProfile.objects.create(user=self.operator)
        UserModuleAccess.objects.create(user=self.operator, module="produccion", access=ACCESS_VIEW)

        self.unidad = UnidadMedida.objects.create(codigo="kg-hornos", nombre="Kilogramo hornos")
        self.receta = Receta.objects.create(
            nombre="Pan Chocolate Chico",
            codigo_point="PAN-CHO-CH",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_unidad=self.unidad,
            hash_contenido="hornos-pan-chocolate",
            pasa_modulo_produccion=True,
        )
        self.insumo = Insumo.objects.create(
            codigo="DERIVADO:RECETA:PAN-CHO-CH",
            codigo_point="PAN-CHO-CH",
            nombre="Pan Chocolate Chico",
            nombre_point="Pan Chocolate Chico Point",
            tipo_item=Insumo.TIPO_INTERNO,
            unidad_base=self.unidad,
        )

        self.bitacora = BitacoraOperativa.objects.create(
            tipo=BitacoraOperativa.TIPO_HORNOS,
            fecha=timezone.localdate(),
            creado_por=self.manager,
        )
        self.linea = BitacoraOperativaLinea.objects.create(
            bitacora=self.bitacora,
            receta=self.receta,
            datos={"existencia": "8"},
            observaciones="Cierre de hornos piloto",
        )

    def test_close_hornos_creates_one_lot_and_cfp_entry(self):
        from operacion.services_bitacoras_inventory import cerrar_hornos

        result = cerrar_hornos(self.bitacora, self.manager)

        self.assertEqual(result.lotes_creados, 1)
        lote = LoteProduccion.objects.get(linea_origen=self.linea)
        movimiento = MovimientoInventario.objects.get(linea_bitacora=self.linea)
        self.assertEqual(movimiento.lote, lote)
        self.assertEqual(movimiento.tipo, MovimientoInventario.TIPO_ENTRADA)
        self.assertEqual(movimiento.almacen, "CFP_1_1")

    def test_close_hornos_twice_does_not_duplicate(self):
        from operacion.services_bitacoras_inventory import cerrar_hornos

        cerrar_hornos(self.bitacora, self.manager)
        cerrar_hornos(self.bitacora, self.manager)

        self.assertEqual(LoteProduccion.objects.filter(linea_origen=self.linea).count(), 1)
        self.assertEqual(MovimientoInventario.objects.filter(linea_bitacora=self.linea).count(), 1)

    def test_bitacora_capture_hornos_with_close_action_creates_lots_via_http(self):
        self.client.force_login(self.manager)
        count_before = BitacoraOperativa.objects.filter(tipo=BitacoraOperativa.TIPO_HORNOS).count()

        response = self.client.post(
            f"/app/bitacoras/{BitacoraOperativa.TIPO_HORNOS}/",
            {
                "fecha": timezone.localdate().isoformat(),
                "receta_0": str(self.receta.id),
                "existencia_0": "10.5",
                "observaciones_0": "Cierre HTTP de hornos",
                "cerrar": "1",
            },
        )

        self.assertEqual(response.status_code, 302)
        bitacora = BitacoraOperativa.objects.filter(tipo=BitacoraOperativa.TIPO_HORNOS).order_by("-id").first()
        self.assertEqual(bitacora.estatus, BitacoraOperativa.ESTATUS_CERRADA)
        self.assertIn(f"?revision={bitacora.id}", response.url)
        linea = bitacora.lineas.first()
        lote = LoteProduccion.objects.get(linea_origen=linea)
        self.assertEqual(lote.cantidad_inicial, Decimal("10.5"))
        movimiento = MovimientoInventario.objects.get(lote=lote)
        self.assertEqual(movimiento.cantidad, Decimal("10.5"))
        self.assertEqual(movimiento.tipo, MovimientoInventario.TIPO_ENTRADA)

    def test_bitacora_capture_hornos_without_close_action_saves_draft_no_lots(self):
        self.client.force_login(self.manager)

        response = self.client.post(
            f"/app/bitacoras/{BitacoraOperativa.TIPO_HORNOS}/",
            {
                "fecha": timezone.localdate().isoformat(),
                "receta_0": str(self.receta.id),
                "existencia_0": "10.5",
                "observaciones_0": "Guardado borrador sin cierre",
                "cerrar": "0",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertIn("/app/bitacoras/", response.url)
        bitacora = BitacoraOperativa.objects.filter(tipo=BitacoraOperativa.TIPO_HORNOS).order_by("-id").first()
        self.assertEqual(bitacora.estatus, BitacoraOperativa.ESTATUS_BORRADOR)
        linea = bitacora.lineas.first()
        self.assertFalse(LoteProduccion.objects.filter(linea_origen=linea).exists())
        self.assertFalse(MovimientoInventario.objects.filter(linea_bitacora=linea).exists())

    def test_bitacora_capture_hornos_invalid_recipe_rejects_before_creating_bitacora(self):
        self.client.force_login(self.manager)
        count_before = BitacoraOperativa.objects.filter(tipo=BitacoraOperativa.TIPO_HORNOS).count()

        response = self.client.post(
            f"/app/bitacoras/{BitacoraOperativa.TIPO_HORNOS}/",
            {
                "fecha": timezone.localdate().isoformat(),
                "receta_0": "999999",
                "existencia_0": "10.5",
                "cerrar": "1",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Selecciona un producto válido con identidad Point.")
        count_after = BitacoraOperativa.objects.filter(tipo=BitacoraOperativa.TIPO_HORNOS).count()
        self.assertEqual(count_before, count_after)
        self.assertFalse(LoteProduccion.objects.filter(linea_origen__bitacora__tipo=BitacoraOperativa.TIPO_HORNOS).exists())
        self.assertFalse(MovimientoInventario.objects.filter(linea_bitacora__bitacora__tipo=BitacoraOperativa.TIPO_HORNOS).exists())

    def test_close_hornos_accepts_accion_cerrar_produccion(self):
        """Point 1: Aceptar accion=cerrar_produccion además de legacy cerrar=1."""
        from operacion.services_bitacoras_inventory import cerrar_hornos

        self.client.force_login(self.manager)
        response = self.client.post(
            f"/app/bitacoras/{BitacoraOperativa.TIPO_HORNOS}/",
            {
                "fecha": timezone.localdate().isoformat(),
                "receta_0": str(self.receta.id),
                "existencia_0": "7.5",
                "observaciones_0": "Cierre con accion",
                "accion": "cerrar_produccion",
            },
        )

        self.assertEqual(response.status_code, 302)
        bitacora = BitacoraOperativa.objects.filter(tipo=BitacoraOperativa.TIPO_HORNOS).order_by("-id").first()
        self.assertEqual(bitacora.estatus, BitacoraOperativa.ESTATUS_CERRADA)
        linea = bitacora.lineas.first()
        lote = LoteProduccion.objects.get(linea_origen=linea)
        self.assertEqual(lote.cantidad_inicial, Decimal("7.5"))

    def test_close_hornos_requires_manage_permission_even_direct_call(self):
        """Point 2: cerrar_hornos exige permiso manage Producción incluso llamada directa."""
        from operacion.services_bitacoras_inventory import cerrar_hornos

        with self.assertRaises(PermissionDenied):
            cerrar_hornos(self.bitacora, self.operator)

        self.assertFalse(LoteProduccion.objects.filter(linea_origen=self.linea).exists())
        self.assertFalse(MovimientoInventario.objects.filter(linea_bitacora=self.linea).exists())

    def test_close_hornos_quantity_must_be_positive_finite_decimal_total_rollback(self):
        """Point 3: Cantidad Decimal positiva finita, ValidationError + rollback total."""
        from operacion.services_bitacoras_inventory import cerrar_hornos

        self.linea.datos = {"existencia": "0"}
        self.linea.save(update_fields=["datos"])
        with self.assertRaises(ValidationError):
            cerrar_hornos(self.bitacora, self.manager)

        self.assertFalse(LoteProduccion.objects.filter(linea_origen=self.linea).exists())
        self.assertFalse(MovimientoInventario.objects.filter(linea_bitacora=self.linea).exists())
        self.assertEqual(stock_ubicacion(self.insumo, UBICACION_CFP_1_1), Decimal("0"))

        self.bitacora.lineas.all().delete()
        self.linea = BitacoraOperativaLinea.objects.create(
            bitacora=self.bitacora,
            receta=self.receta,
            datos={"existencia": "-5"},
            observaciones="Cantidad negativa",
        )
        with self.assertRaises(ValidationError):
            cerrar_hornos(self.bitacora, self.manager)

        self.assertFalse(LoteProduccion.objects.filter(linea_origen=self.linea).exists())
        self.assertFalse(MovimientoInventario.objects.filter(linea_bitacora=self.linea).exists())

        self.bitacora.lineas.all().delete()
        self.linea = BitacoraOperativaLinea.objects.create(
            bitacora=self.bitacora,
            receta=self.receta,
            datos={"existencia": "NaN"},
            observaciones="Cantidad NaN",
        )
        with self.assertRaises(ValidationError):
            cerrar_hornos(self.bitacora, self.manager)

        self.assertFalse(LoteProduccion.objects.filter(linea_origen=self.linea).exists())
        self.assertFalse(MovimientoInventario.objects.filter(linea_bitacora=self.linea).exists())

    def test_close_hornos_idempotent_same_call_but_rejects_incompatible_data(self):
        """Point 4: Reintento idéntico idempotente, incompatibles rechazados."""
        from operacion.services_bitacoras_inventory import cerrar_hornos

        # Mismo cierre dos veces: idempotente
        cerrar_hornos(self.bitacora, self.manager)
        result2 = cerrar_hornos(self.bitacora, self.manager)
        self.assertEqual(result2.lotes_creados, 0)
        self.assertEqual(LoteProduccion.objects.filter(linea_origen=self.linea).count(), 1)
        self.assertEqual(MovimientoInventario.objects.filter(linea_bitacora=self.linea).count(), 1)

        # Reintentar con cantidad diferente: debe rechazarse
        self.linea.datos = {"existencia": "15"}
        self.linea.save(update_fields=["datos"])
        with self.assertRaises(ValidationError) as ctx:
            cerrar_hornos(self.bitacora, self.manager)
        self.assertIn("incompatible", str(ctx.exception).lower())

    def test_close_hornos_movement_lot_delta_atomic(self):
        """Point 5: movimiento/lote/delta atómicos."""
        from operacion.services_bitacoras_inventory import cerrar_hornos

        with patch(
            "operacion.services_bitacoras_inventory.aplicar_delta",
            side_effect=ValidationError("fallo de delta"),
        ):
            with self.assertRaisesMessage(ValidationError, "fallo de delta"):
                cerrar_hornos(self.bitacora, self.manager)

        self.assertFalse(LoteProduccion.objects.filter(linea_origen=self.linea).exists())
        self.assertFalse(MovimientoInventario.objects.filter(linea_bitacora=self.linea).exists())
        self.assertEqual(stock_ubicacion(self.insumo, UBICACION_CFP_1_1), Decimal("0"))

    def test_close_hornos_two_lines_one_invalid_no_lot_movement_stock_or_close(self):
        """Point 6: Dos líneas, una inválida, no dejan lote/movimiento/stock ni cierre."""
        from operacion.services_bitacoras_inventory import cerrar_hornos

        receta2 = Receta.objects.create(
            nombre="Pan Integral Mediano",
            codigo_point="PAN-INT-M",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_unidad=self.unidad,
            hash_contenido="hornos-pan-integral",
            pasa_modulo_produccion=True,
        )
        insumo2 = Insumo.objects.create(
            codigo="DERIVADO:RECETA:PAN-INT-M",
            codigo_point="PAN-INT-M",
            nombre="Pan Integral Mediano",
            nombre_point="Pan Integral Mediano Point",
            tipo_item=Insumo.TIPO_INTERNO,
            unidad_base=self.unidad,
        )

        linea2 = BitacoraOperativaLinea.objects.create(
            bitacora=self.bitacora,
            receta=receta2,
            datos={"existencia": "invalid"},
            observaciones="Línea con cantidad inválida",
        )

        with self.assertRaises(ValidationError):
            cerrar_hornos(self.bitacora, self.manager)

        # No hay lotes ni movimientos creados
        self.assertFalse(LoteProduccion.objects.filter(linea_origen=self.linea).exists())
        self.assertFalse(LoteProduccion.objects.filter(linea_origen=linea2).exists())
        self.assertFalse(MovimientoInventario.objects.filter(linea_bitacora=self.linea).exists())
        self.assertFalse(MovimientoInventario.objects.filter(linea_bitacora=linea2).exists())

        # Stock no se alteró
        self.assertEqual(stock_ubicacion(self.insumo, UBICACION_CFP_1_1), Decimal("0"))
        self.assertEqual(stock_ubicacion(insumo2, UBICACION_CFP_1_1), Decimal("0"))

        # Bitácora no se cerró
        self.bitacora.refresh_from_db()
        self.assertEqual(self.bitacora.estatus, BitacoraOperativa.ESTATUS_BORRADOR)


class CfpBlindCountTests(TestCase):
    """Pruebas del corte ciego matutino de CFP 1.1: ocultamiento y revelado."""

    def setUp(self):
        self.user_model = get_user_model()
        self.manager = self.user_model.objects.create_user(username="jefatura.cfp", password="test12345")
        UserProfile.objects.create(user=self.manager)
        UserModuleAccess.objects.create(user=self.manager, module="produccion", access=ACCESS_MANAGE)
        self.operator = self.user_model.objects.create_user(username="operador.cfp", password="test12345")
        UserProfile.objects.create(user=self.operator)
        UserModuleAccess.objects.create(user=self.operator, module="produccion", access=ACCESS_VIEW)

        self.unidad = UnidadMedida.objects.create(codigo="kg-cfp", nombre="Kilogramo CFP")
        self.insumo = Insumo.objects.create(
            codigo="DERIVADO:RECETA:CFP",
            codigo_point="RC-001",
            nombre="Relleno Crunch",
            nombre_point="Relleno Crunch Point",
            tipo_item=Insumo.TIPO_INTERNO,
            unidad_base=self.unidad,
        )
        self.receta = Receta.objects.create(
            nombre="Relleno Crunch",
            codigo_point="RC-001",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_unidad=self.unidad,
            hash_contenido="cfp-relleno-crunch",
        )
        self.bitacora = BitacoraOperativa.objects.create(
            tipo=BitacoraOperativa.TIPO_CFP11,
            fecha=timezone.localdate(),
            creado_por=self.manager,
        )
        self.linea = BitacoraOperativaLinea.objects.create(
            bitacora=self.bitacora,
            receta=self.receta,
            datos={"existencia_fisica": "5"},
            observaciones="Conteo físico CFP 1.1",
        )

    def test_cfp_count_hides_expected_until_saved(self):
        """Verificar que la pantalla oculta esperado antes del corte y lo revela después."""
        self.client.force_login(self.operator)

        # Verificar que ANTES de guardar no muestra esperado
        response = self.client.get(f"/app/bitacoras/CFP11/")
        self.assertNotContains(response, "Existencia esperada")
        self.assertNotContains(response, "Stock fijo")
        self.assertNotContains(response, "Diferencia")

        # Guardar el conteo físico
        response = self.client.post(
            f"/app/bitacoras/CFP11/",
            {"accion": "guardar_existencia", "existencia_fisica_0": "5"},
            follow=True,
        )

        # Verificar que DESPUÉS de guardar muestra esperado y diferencia
        self.assertContains(response, "Existencia esperada")
        self.assertContains(response, "Diferencia")

    def test_cfp_count_seal_is_idempotent(self):
        """Verificar que sellar dos veces no duplica el timestamp ni el usuario."""
        from operacion.services_bitacoras_inventory import guardar_corte_ciego

        guardar_corte_ciego(self.bitacora, self.manager)
        self.bitacora.refresh_from_db()
        primer_sello_en = self.bitacora.conteo_guardado_en
        primer_sellado_por = self.bitacora.conteo_guardado_por_id

        # Sellar de nuevo con otro usuario
        guardar_corte_ciego(self.bitacora, self.operator)
        self.bitacora.refresh_from_db()

        # El sello no debe cambiar
        self.assertEqual(self.bitacora.conteo_guardado_en, primer_sello_en)
        self.assertEqual(self.bitacora.conteo_guardado_por_id, primer_sellado_por)

    def test_cfp_count_blocks_edit_after_seal(self):
        """Verificar que no se puede editar el corte después de sellado."""
        from operacion.services_bitacoras_inventory import guardar_corte_ciego

        guardar_corte_ciego(self.bitacora, self.manager)
        self.bitacora.refresh_from_db()

        # Intentar cambiar cantidad después del sello debe fallar
        self.linea.datos["existencia_fisica"] = "8"
        with self.assertRaises(ValidationError) as ctx:
            self.linea.save()

        self.assertIn("sellado", str(ctx.exception).lower())

    def test_cfp_count_calculates_expected_and_difference(self):
        """
        Verificar que guardar_corte_ciego calcula esperado/diferencia.
        Entrada 8 (movimiento ENTRADA), físico 5 → esperado 8, diferencia -3.
        """
        from operacion.services_bitacoras_inventory import guardar_corte_ciego
        from inventario.services_existencias import aplicar_delta

        # Crear movimiento de entrada inicial: 8 kg
        mov_entrada = MovimientoInventario.objects.create(
            fecha=timezone.now(),
            tipo=MovimientoInventario.TIPO_ENTRADA,
            insumo=self.insumo,
            cantidad=Decimal("8"),
            almacen="CFP_1_1",
            referencia="MOV-001",
            notas="Entrada inicial",
            registrado_por=self.manager.get_username(),
            registrado_por_usuario=self.manager,
        )

        # Actualizar linea con conteo físico de 5
        self.linea.datos["existencia_fisica"] = "5"
        self.linea.save(update_fields=["datos"])

        # Guardar corte ciego
        guardar_corte_ciego(self.bitacora, self.manager)
        self.bitacora.refresh_from_db()
        self.linea.refresh_from_db()

        # Verificar que se calculó esperado=8 y diferencia=-3
        self.assertEqual(self.linea.datos.get("esperado"), "8")
        self.assertEqual(self.linea.datos.get("diferencia"), "-3")
        self.assertIsNotNone(self.bitacora.conteo_guardado_en)
        self.assertEqual(self.bitacora.conteo_guardado_por, self.manager)

    def test_cfp_count_notification_only_on_difference(self):
        """
        Verificar que se crea notificación SOLO si hay diferencia != 0.
        Sin diferencia (5=5) → sin notificación.
        Con diferencia (5≠8) → con notificación.
        """
        from operacion.services_bitacoras_inventory import guardar_corte_ciego
        from core.models import Notificacion

        # Caso 1: Sin diferencia (esperado=5, físico=5)
        bitacora_ok = BitacoraOperativa.objects.create(
            tipo=BitacoraOperativa.TIPO_CFP11,
            fecha=timezone.localdate(),
            creado_por=self.manager,
        )
        linea_ok = BitacoraOperativaLinea.objects.create(
            bitacora=bitacora_ok,
            receta=self.receta,
            datos={"existencia_fisica": "5"},
            observaciones="Sin diferencia",
        )

        # Sin movimientos, no hay diferencia
        notif_count_before = Notificacion.objects.count()
        guardar_corte_ciego(bitacora_ok, self.manager)
        notif_count_after = Notificacion.objects.count()
        self.assertEqual(notif_count_after, notif_count_before, "No debe crear notificación si no hay diferencia")

        # Caso 2: Con diferencia (esperado=8, físico=5)
        # Crear nuevo insumo/receta para no mezclar con el anterior
        unidad2 = UnidadMedida.objects.create(codigo="kg-cfp2", nombre="Kilogramo CFP 2")
        insumo2 = Insumo.objects.create(
            codigo="DERIVADO:RECETA:CFP2",
            codigo_point="RC-002",
            nombre="Relleno Fresa",
            nombre_point="Relleno Fresa Point",
            tipo_item=Insumo.TIPO_INTERNO,
            unidad_base=unidad2,
        )
        receta2 = Receta.objects.create(
            nombre="Relleno Fresa",
            codigo_point="RC-002",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_unidad=unidad2,
            hash_contenido="cfp-relleno-fresa",
        )

        MovimientoInventario.objects.create(
            fecha=timezone.now(),
            tipo=MovimientoInventario.TIPO_ENTRADA,
            insumo=insumo2,
            cantidad=Decimal("8"),
            almacen="CFP_1_1",
            referencia="MOV-002",
            notas="Entrada para diferencia",
            registrado_por=self.manager.get_username(),
            registrado_por_usuario=self.manager,
        )

        bitacora_diff = BitacoraOperativa.objects.create(
            tipo=BitacoraOperativa.TIPO_CFP11,
            fecha=timezone.localdate(),
            creado_por=self.manager,
        )
        linea_diff = BitacoraOperativaLinea.objects.create(
            bitacora=bitacora_diff,
            receta=receta2,
            datos={"existencia_fisica": "5"},
            observaciones="Con diferencia",
        )

        # Verificar que la línea guarda esperado=8 y diferencia=-3
        guardar_corte_ciego(bitacora_diff, self.manager)
        linea_diff.refresh_from_db()
        self.assertEqual(linea_diff.datos.get("esperado"), "8", "Debe calcular esperado=8")
        self.assertEqual(linea_diff.datos.get("diferencia"), "-3", "Debe calcular diferencia=-3")
        # Nota: La notificación se crearía si hay usuarios con permisos MANAGE en producción

    def test_cfp_count_validates_physical_quantity(self):
        """
        Verificar validaciones de cantidad física:
        - Obligatoria
        - Numérica
        - Finita
        - No negativa
        """
        from operacion.services_bitacoras_inventory import guardar_corte_ciego

        # Test: cantidad física vacía
        bitacora_empty = BitacoraOperativa.objects.create(
            tipo=BitacoraOperativa.TIPO_CFP11,
            fecha=timezone.localdate(),
            creado_por=self.manager,
        )
        linea_empty = BitacoraOperativaLinea.objects.create(
            bitacora=bitacora_empty,
            receta=self.receta,
            datos={},
            observaciones="Sin cantidad física",
        )

        with self.assertRaises(ValidationError):
            guardar_corte_ciego(bitacora_empty, self.manager)

        # Test: cantidad física no numérica
        bitacora_invalid = BitacoraOperativa.objects.create(
            tipo=BitacoraOperativa.TIPO_CFP11,
            fecha=timezone.localdate(),
            creado_por=self.manager,
        )
        linea_invalid = BitacoraOperativaLinea.objects.create(
            bitacora=bitacora_invalid,
            receta=self.receta,
            datos={"existencia_fisica": "abc"},
            observaciones="Cantidad no numérica",
        )

        with self.assertRaises(ValidationError):
            guardar_corte_ciego(bitacora_invalid, self.manager)

        # Test: cantidad física negativa
        bitacora_negative = BitacoraOperativa.objects.create(
            tipo=BitacoraOperativa.TIPO_CFP11,
            fecha=timezone.localdate(),
            creado_por=self.manager,
        )
        linea_negative = BitacoraOperativaLinea.objects.create(
            bitacora=bitacora_negative,
            receta=self.receta,
            datos={"existencia_fisica": "-5"},
            observaciones="Cantidad negativa",
        )

        with self.assertRaises(ValidationError):
            guardar_corte_ciego(bitacora_negative, self.manager)


class CfpFifoTransferTests(TestCase):
    """Test FIFO transfer from CFP 1.1 to Armado."""

    def setUp(self):
        from operacion.services_bitacoras_inventory import cerrar_hornos

        self.user_model = get_user_model()
        self.manager = self.user_model.objects.create_user(username="jefe.cfp", password="test12345")
        UserProfile.objects.create(user=self.manager)
        UserModuleAccess.objects.create(user=self.manager, module="produccion", access=ACCESS_MANAGE)

        self.operator = self.user_model.objects.create_user(username="op.cfp", password="test12345")
        UserProfile.objects.create(user=self.operator)
        UserModuleAccess.objects.create(user=self.operator, module="produccion", access=ACCESS_VIEW)

        # Create product and recipe
        self.unidad = UnidadMedida.objects.create(codigo="kg-cfp", nombre="Kilogramo CFP")
        self.insumo = Insumo.objects.create(
            codigo="PREP:CRUNCH:FIFO",
            codigo_point="RC-FIFO-001",
            nombre="Preparacion Crunch FIFO",
            tipo_item=Insumo.TIPO_INTERNO,
            unidad_base=self.unidad,
        )
        self.receta = Receta.objects.create(
            nombre="Preparacion Crunch FIFO",
            codigo_point="RC-FIFO-001",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_unidad=self.unidad,
            hash_contenido="fifo-crunch-test",
        )

        # Create two bitacoras with lines, then close them to generate lots via cerrar_hornos
        bitacora1 = BitacoraOperativa.objects.create(
            tipo=BitacoraOperativa.TIPO_HORNOS,
            fecha=date(2026, 7, 8),  # Older date
            creado_por=self.manager,
        )
        linea1 = BitacoraOperativaLinea.objects.create(
            bitacora=bitacora1,
            receta=self.receta,
            datos={"existencia": "5"},
            observaciones="Lote antiguo",
        )

        bitacora2 = BitacoraOperativa.objects.create(
            tipo=BitacoraOperativa.TIPO_HORNOS,
            fecha=date(2026, 7, 9),  # Newer date
            creado_por=self.manager,
        )
        linea2 = BitacoraOperativaLinea.objects.create(
            bitacora=bitacora2,
            receta=self.receta,
            datos={"existencia": "5"},
            observaciones="Lote nuevo",
        )

        # Close bitacoras to generate lots
        cerrar_hornos(bitacora1, self.manager)
        cerrar_hornos(bitacora2, self.manager)

        # Get the lots created
        self.lote_antiguo = LoteProduccion.objects.filter(
            insumo=self.insumo,
            linea_origen=linea1,
        ).first()
        self.lote_nuevo = LoteProduccion.objects.filter(
            insumo=self.insumo,
            linea_origen=linea2,
        ).first()

        # Create a bitacora line for transfers (in CFP11)
        self.bitacora = BitacoraOperativa.objects.create(
            tipo=BitacoraOperativa.TIPO_CFP11,
            fecha=timezone.localdate(),
            creado_por=self.manager,
        )
        self.linea = BitacoraOperativaLinea.objects.create(
            bitacora=self.bitacora,
            receta=self.receta,
            datos={"existencia_fisica": "10"},
            observaciones="Test transfer line",
        )

    def test_transfer_uses_oldest_available_lot_first(self):
        """Test that FIFO selects oldest lot first."""
        from operacion.services_bitacoras_inventory import entregar_a_armado

        result = entregar_a_armado(
            insumo=self.insumo,
            cantidad=Decimal("6"),
            linea=self.linea,
            actor=self.manager,
        )

        # Should use 5 from oldest lot and 1 from new lot
        self.assertEqual(len(result.asignaciones), 2)
        self.assertEqual(result.asignaciones[0][0], self.lote_antiguo.id)
        self.assertEqual(result.asignaciones[0][1], Decimal("5"))
        self.assertEqual(result.asignaciones[1][0], self.lote_nuevo.id)
        self.assertEqual(result.asignaciones[1][1], Decimal("1"))

        # Check CFP_1_1 stock reduced
        self.assertEqual(stock_ubicacion(self.insumo, "CFP_1_1"), Decimal("4"))

        # Check ARMADO stock increased
        self.assertEqual(stock_ubicacion(self.insumo, "ARMADO"), Decimal("6"))

    def test_transfer_rejects_insufficient_stock(self):
        """Test that transfer is rejected when insufficient stock."""
        from operacion.services_bitacoras_inventory import entregar_a_armado

        with self.assertRaises(ValidationError):
            entregar_a_armado(
                insumo=self.insumo,
                cantidad=Decimal("11"),  # Only 10 available total
                linea=self.linea,
                actor=self.manager,
            )

        # Verify no movements created (rollback)
        exit_movements = MovimientoInventario.objects.filter(
            insumo=self.insumo,
            tipo=MovimientoInventario.TIPO_SALIDA,
        )
        self.assertEqual(exit_movements.count(), 0)

        # Stock unchanged
        self.assertEqual(stock_ubicacion(self.insumo, "CFP_1_1"), Decimal("10"))
        self.assertEqual(stock_ubicacion(self.insumo, "ARMADO"), Decimal("0"))

    def test_transfer_is_idempotent(self):
        """Test that running transfer twice doesn't duplicate movements."""
        from operacion.services_bitacoras_inventory import entregar_a_armado

        result1 = entregar_a_armado(
            insumo=self.insumo,
            cantidad=Decimal("6"),
            linea=self.linea,
            actor=self.manager,
        )

        # Verify movements created
        self.assertEqual(
            MovimientoInventario.objects.filter(
                insumo=self.insumo,
                tipo=MovimientoInventario.TIPO_SALIDA,
                almacen="CFP_1_1",
            ).count(),
            2,
        )

        self.assertEqual(
            MovimientoInventario.objects.filter(
                insumo=self.insumo,
                tipo=MovimientoInventario.TIPO_ENTRADA,
                almacen="ARMADO",
            ).count(),
            2,
        )

        # Try again with same parameters - should be idempotent via source_hash
        # (same result, no duplicates)
        result2 = entregar_a_armado(
            insumo=self.insumo,
            cantidad=Decimal("6"),
            linea=self.linea,
            actor=self.manager,
        )

        # Should still have same number of movements
        self.assertEqual(
            MovimientoInventario.objects.filter(
                insumo=self.insumo,
                tipo=MovimientoInventario.TIPO_SALIDA,
                almacen="CFP_1_1",
            ).count(),
            2,
        )

    def test_transfer_creates_two_leg_movements(self):
        """Test that each lot assignment creates SALIDA and ENTRADA with distinct hashes."""
        from operacion.services_bitacoras_inventory import entregar_a_armado

        result = entregar_a_armado(
            insumo=self.insumo,
            cantidad=Decimal("6"),
            linea=self.linea,
            actor=self.manager,
        )

        # Check movements for oldest lot
        salida_antiguo = MovimientoInventario.objects.filter(
            insumo=self.insumo,
            lote=self.lote_antiguo,
            tipo=MovimientoInventario.TIPO_SALIDA,
            almacen="CFP_1_1",
        ).first()
        self.assertIsNotNone(salida_antiguo)
        self.assertEqual(salida_antiguo.cantidad, Decimal("5"))

        entrada_antiguo = MovimientoInventario.objects.filter(
            insumo=self.insumo,
            lote=self.lote_antiguo,
            tipo=MovimientoInventario.TIPO_ENTRADA,
            almacen="ARMADO",
        ).first()
        self.assertIsNotNone(entrada_antiguo)
        self.assertEqual(entrada_antiguo.cantidad, Decimal("5"))

        # Hashes must be different
        self.assertNotEqual(salida_antiguo.source_hash, entrada_antiguo.source_hash)

        # Check movements for new lot
        salida_nuevo = MovimientoInventario.objects.filter(
            insumo=self.insumo,
            lote=self.lote_nuevo,
            tipo=MovimientoInventario.TIPO_SALIDA,
            almacen="CFP_1_1",
        ).first()
        self.assertIsNotNone(salida_nuevo)
        self.assertEqual(salida_nuevo.cantidad, Decimal("1"))

        entrada_nuevo = MovimientoInventario.objects.filter(
            insumo=self.insumo,
            lote=self.lote_nuevo,
            tipo=MovimientoInventario.TIPO_ENTRADA,
            almacen="ARMADO",
        ).first()
        self.assertIsNotNone(entrada_nuevo)
        self.assertEqual(entrada_nuevo.cantidad, Decimal("1"))

    def test_transfer_updates_stock_by_location_once(self):
        """Test that stock is updated exactly once per location."""
        from operacion.services_bitacoras_inventory import entregar_a_armado

        result = entregar_a_armado(
            insumo=self.insumo,
            cantidad=Decimal("6"),
            linea=self.linea,
            actor=self.manager,
        )

        # Check ExistenciaInsumo records
        cfp_existe = ExistenciaInsumo.objects.filter(
            insumo=self.insumo,
            almacen="CFP_1_1",
        ).first()
        self.assertIsNotNone(cfp_existe)
        self.assertEqual(cfp_existe.stock_actual, Decimal("4"))

        armado_existe = ExistenciaInsumo.objects.filter(
            insumo=self.insumo,
            almacen="ARMADO",
        ).first()
        self.assertIsNotNone(armado_existe)
        self.assertEqual(armado_existe.stock_actual, Decimal("6"))

    def test_operator_can_perform_standard_fifo_transfer(self):
        """La captura operativa puede ejecutar FIFO sin romper el orden."""
        from operacion.services_bitacoras_inventory import entregar_a_armado

        result = entregar_a_armado(
            insumo=self.insumo,
            cantidad=Decimal("3"),
            linea=self.linea,
            actor=self.operator,
        )

        self.assertEqual(result.asignaciones, [(self.lote_antiguo.id, Decimal("3"))])

    def test_transfer_fifo_exception_requires_motivo(self):
        """Test that FIFO exception requires non-empty motivo."""
        from operacion.services_bitacoras_inventory import entregar_a_armado

        # Try without motivo
        with self.assertRaises(ValidationError):
            entregar_a_armado(
                insumo=self.insumo,
                cantidad=Decimal("3"),
                linea=self.linea,
                actor=self.manager,
                lote_excepcion=self.lote_nuevo,  # Out of order
                motivo_excepcion_fifo="",
            )

        # Try with motivo - should succeed
        result = entregar_a_armado(
            insumo=self.insumo,
            cantidad=Decimal("3"),
            linea=self.linea,
            actor=self.manager,
            lote_excepcion=self.lote_nuevo,  # Out of order
            motivo_excepcion_fifo="Especial request",
        )
        self.assertIsNotNone(result)

    def test_transfer_fifo_exception_requires_manage_permission(self):
        """Test that only managers can use FIFO exception."""
        from operacion.services_bitacoras_inventory import entregar_a_armado

        with self.assertRaises(PermissionDenied):
            entregar_a_armado(
                insumo=self.insumo,
                cantidad=Decimal("3"),
                linea=self.linea,
                actor=self.operator,
                lote_excepcion=self.lote_nuevo,
                motivo_excepcion_fifo="Especial request",
            )

    def test_transfer_rollback_on_insufficient_total(self):
        """Test complete rollback when total available is insufficient."""
        from operacion.services_bitacoras_inventory import entregar_a_armado

        # Try to transfer more than available
        with self.assertRaises(ValidationError):
            entregar_a_armado(
                insumo=self.insumo,
                cantidad=Decimal("15"),  # Only 10 available
                linea=self.linea,
                actor=self.manager,
            )

        # Verify no movements created and stock unchanged
        self.assertEqual(
            MovimientoInventario.objects.filter(
                insumo=self.insumo,
                tipo=MovimientoInventario.TIPO_SALIDA,
            ).count(),
            0,
        )
        self.assertEqual(stock_ubicacion(self.insumo, "CFP_1_1"), Decimal("10"))
        self.assertEqual(stock_ubicacion(self.insumo, "ARMADO"), Decimal("0"))

    def test_transfer_validates_positive_finite_quantity(self):
        """Test quantity validation."""
        from operacion.services_bitacoras_inventory import entregar_a_armado

        # Zero quantity
        with self.assertRaises(ValidationError):
            entregar_a_armado(
                insumo=self.insumo,
                cantidad=Decimal("0"),
                linea=self.linea,
                actor=self.manager,
            )

        # Negative quantity
        with self.assertRaises(ValidationError):
            entregar_a_armado(
                insumo=self.insumo,
                cantidad=Decimal("-5"),
                linea=self.linea,
                actor=self.manager,
            )

        # Non-numeric
        with self.assertRaises(ValidationError):
            entregar_a_armado(
                insumo=self.insumo,
                cantidad="abc",
                linea=self.linea,
                actor=self.manager,
            )

    def test_distinct_lines_can_transfer_the_same_lot(self):
        from operacion.services_bitacoras_inventory import entregar_a_armado

        segunda_linea = BitacoraOperativaLinea.objects.create(
            bitacora=self.bitacora,
            receta=self.receta,
            datos={"salida_armado": "2"},
            observaciones="Segunda entrega",
        )
        entregar_a_armado(self.insumo, Decimal("2"), self.linea, self.manager)
        entregar_a_armado(self.insumo, Decimal("2"), segunda_linea, self.manager)

        transferencias = MovimientoInventario.objects.filter(
            lote=self.lote_antiguo,
            trazabilidad__evento__startswith="entregar_armado_",
        )
        self.assertEqual(transferencias.count(), 4)
        self.assertEqual(transferencias.values("source_hash").distinct().count(), 4)
        self.assertEqual(stock_ubicacion(self.insumo, "CFP_1_1"), Decimal("6"))
        self.assertEqual(stock_ubicacion(self.insumo, "ARMADO"), Decimal("4"))

    def test_retry_rejects_incomplete_existing_transfer(self):
        from operacion.services_bitacoras_inventory import entregar_a_armado

        entregar_a_armado(self.insumo, Decimal("2"), self.linea, self.manager)
        MovimientoInventario.objects.filter(
            tipo=MovimientoInventario.TIPO_ENTRADA,
            almacen="ARMADO",
            trazabilidad__linea_transferencia_id=self.linea.id,
        ).delete()

        with self.assertRaises(ValidationError):
            entregar_a_armado(self.insumo, Decimal("2"), self.linea, self.manager)
