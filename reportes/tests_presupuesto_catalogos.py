from __future__ import annotations

from datetime import date
from decimal import Decimal
from threading import Barrier, Lock, Thread

from django.contrib.auth import get_user_model
from django.db import close_old_connections, connections
from django.test import RequestFactory, TestCase, TransactionTestCase
from django.urls import reverse
from rest_framework.test import APIClient

from core.models import AuditLog, Sucursal, UserModuleAccess
from core.navigation import build_nav_groups
from reportes.models import (
    AreaPresupuesto,
    AreaPresupuestoResponsable,
    CategoriaGasto,
    LineaPresupuestoMensual,
    ReglaFuenteRubro,
    RubroPresupuesto,
)
from reportes.services_presupuesto_maestro import PresupuestoMaestroService, ensure_master_budget_areas
from reportes.services_presupuesto_catalogos import (
    SOURCE_AUTO_WITH_DATA,
    SOURCE_AUTO_WITHOUT_DATA,
    PresupuestoCatalogoService,
    normalize_account_code,
    normalize_display_name,
    source_state_for,
)
from reportes.views_presupuesto_catalogos import _safe_return_to


class PresupuestoCatalogosAccessTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.area_admin = AreaPresupuesto.objects.create(
            codigo="administracion", nombre="Administración", orden=10
        )
        self.area_ventas = AreaPresupuesto.objects.create(
            codigo="gastos-venta", nombre="Gastos de venta", orden=20
        )
        self.area_compras = AreaPresupuesto.objects.create(
            codigo="compras", nombre="Compras", orden=30
        )
        self.area_nomina = AreaPresupuesto.objects.create(
            codigo="nomina", nombre="Nómina", orden=40
        )
        self.area_produccion = AreaPresupuesto.objects.create(
            codigo="produccion", nombre="Producción", orden=50
        )
        self.yesenia = User.objects.create_user("yesenia", password="test")
        self.johana = User.objects.create_user("johana", password="test")
        self.paula = User.objects.create_user("paula", password="test")
        self.carolina = User.objects.create_user("carolina", password="test")
        self.jorge = User.objects.create_user("jorge", password="test")
        self.director = User.objects.create_user("director", password="test")
        self.report_manager = User.objects.create_user("report_manager", password="test")
        self.superuser = User.objects.create_superuser("support", password="test")
        AreaPresupuestoResponsable.objects.create(
            usuario=self.yesenia, area=self.area_admin, puede_capturar=True
        )
        AreaPresupuestoResponsable.objects.create(
            usuario=self.yesenia, area=self.area_compras, puede_capturar=True
        )
        AreaPresupuestoResponsable.objects.create(
            usuario=self.johana, area=self.area_ventas, puede_capturar=True
        )
        AreaPresupuestoResponsable.objects.create(
            usuario=self.paula, area=self.area_nomina, puede_capturar=True
        )
        AreaPresupuestoResponsable.objects.create(
            usuario=self.carolina, area=self.area_produccion, puede_capturar=True
        )
        UserModuleAccess.objects.create(
            user=self.director,
            module="reportes",
            access=UserModuleAccess.ACCESS_VIEW,
        )
        UserModuleAccess.objects.create(
            user=self.report_manager,
            module="reportes",
            access=UserModuleAccess.ACCESS_MANAGE,
        )

    def test_responsable_activa_ve_catalogo_y_menu(self):
        self.client.force_login(self.johana)

        response = self.client.get(reverse("reportes:presupuesto_catalogos"))
        groups = build_nav_groups(self.johana, "/reportes/presupuesto-real/catalogos/")

        self.assertEqual(response.status_code, 200)
        mi_trabajo = next(group for group in groups if group["key"] == "mi_trabajo")
        self.assertIn("Catálogos de presupuesto", [item["label"] for item in mi_trabajo["items"]])
        self.assertContains(response, "Solicita a Administración")
        self.assertNotContains(response, "Crear rubro")

    def test_sin_asignacion_y_lector_de_reportes_no_entran(self):
        for user in (self.jorge, self.director):
            self.client.force_login(user)
            response = self.client.get(reverse("reportes:presupuesto_catalogos"))
            self.assertEqual(response.status_code, 403)

            groups = build_nav_groups(user, "/dashboard/")
            labels = [item["label"] for group in groups for item in group["items"]]
            self.assertNotIn("Catálogos de presupuesto", labels)

    def test_acceso_depende_de_asignacion_activa_y_superuser_de_soporte(self):
        expected = (
            (self.paula, 200),
            (self.yesenia, 200),
            (self.carolina, 200),
            (self.johana, 200),
            (self.superuser, 200),
            (self.jorge, 403),
        )
        for user, status_code in expected:
            with self.subTest(user=user.username):
                self.client.force_login(user)
                response = self.client.get(reverse("reportes:presupuesto_catalogos"))
                self.assertEqual(response.status_code, status_code)

    def test_responsable_de_administracion_puede_dar_alta_auditada(self):
        categoria = CategoriaGasto.objects.create(
            codigo="RENTAS",
            nombre="Rentas",
            capa_objetivo=CategoriaGasto.CAPA_EMPRESA,
            bucket=CategoriaGasto.BUCKET_CORPORATIVO,
        )
        sucursal = Sucursal.objects.create(codigo="COLOSIO", nombre="Colosio")
        self.client.force_login(self.yesenia)

        response = self.client.post(
            reverse("reportes:presupuesto_catalogos"),
            {
                "action": "create_rubro",
                "area": self.area_admin.codigo,
                "categoria_id": categoria.id,
                "concepto": "  RENTA   LOCAL  ",
                "codigo_cuenta": " renta-local ",
                "tipo": RubroPresupuesto.TIPO_EGRESO,
                "sucursal_id": sucursal.id,
                "fuente_mode": "MANUAL",
                "year": 2026,
                "version": "ORIGINAL",
                "return_to": reverse("reportes:presupuesto_catalogos"),
            },
            HTTP_ACCEPT="application/json",
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        rubro = RubroPresupuesto.objects.get()
        self.assertEqual(rubro.concepto, "Renta local")
        self.assertEqual(rubro.codigo_cuenta, "RENTA-LOCAL")
        self.assertEqual(rubro.sucursal, sucursal)
        self.assertEqual(rubro.metadata["catalog_category_id"], categoria.id)
        self.assertEqual(rubro.lineas_mensuales.count(), 12)
        self.assertTrue(
            rubro.reglas_fuente.filter(
                tipo_fuente=ReglaFuenteRubro.FUENTE_MANUAL, activa=True
            ).exists()
        )
        self.assertTrue(
            AuditLog.objects.filter(
                user=self.yesenia,
                action="presupuesto_catalogo_rubro_creado",
                model="reportes.RubroPresupuesto",
                object_id=str(rubro.id),
            ).exists()
        )

    def test_return_to_rechaza_destinos_inseguros_y_ajenos_al_catalogo(self):
        request_factory = RequestFactory()
        fallback = reverse("reportes:presupuesto_catalogos")
        unsafe_targets = (
            "//evil.example/path",
            "/\\evil.example/path",
            "https://evil.example/path",
            f"{fallback}\r\nLocation: https://evil.example",
            reverse("reportes:presupuesto_maestro"),
        )

        for target in unsafe_targets:
            with self.subTest(target=target):
                request = request_factory.post(
                    fallback,
                    {"return_to": target},
                    HTTP_HOST="testserver",
                )
                self.assertEqual(_safe_return_to(request), fallback)

    def test_return_to_inseguro_usa_fallback_en_html_tradicional(self):
        self.client.force_login(self.yesenia)
        fallback = reverse("reportes:presupuesto_catalogos")

        response = self.client.post(
            fallback,
            {
                "action": "create_category",
                "codigo": "SEGURA_HTML",
                "nombre": "Segura HTML",
                "capa_objetivo": CategoriaGasto.CAPA_EMPRESA,
                "bucket": CategoriaGasto.BUCKET_CORPORATIVO,
                "return_to": "/\\evil.example/path",
            },
        )

        self.assertRedirects(
            response,
            f"{fallback}#catalog-actions",
            fetch_redirect_response=False,
        )

    def test_return_to_inseguro_usa_fallback_en_redirect_json(self):
        self.client.force_login(self.yesenia)
        fallback = reverse("reportes:presupuesto_catalogos")

        response = self.client.post(
            fallback,
            {
                "action": "create_category",
                "codigo": "SEGURA_JSON",
                "nombre": "Segura JSON",
                "capa_objetivo": CategoriaGasto.CAPA_EMPRESA,
                "bucket": CategoriaGasto.BUCKET_CORPORATIVO,
                "return_to": "https://evil.example/path",
            },
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["redirect"], f"{fallback}#catalog-actions")

    def test_responsable_de_administracion_crea_categoria_normalizada_y_auditada(self):
        self.client.force_login(self.yesenia)

        response = self.client.post(
            reverse("reportes:presupuesto_catalogos"),
            {
                "action": "create_category",
                "codigo": " servicios locales ",
                "nombre": "  SERVICIOS   LOCALES ",
                "capa_objetivo": CategoriaGasto.CAPA_EMPRESA,
                "bucket": CategoriaGasto.BUCKET_CORPORATIVO,
            },
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(response.status_code, 200)
        category = CategoriaGasto.objects.get()
        self.assertEqual(category.codigo, "SERVICIOS_LOCALES")
        self.assertEqual(category.nombre, "Servicios locales")
        self.assertTrue(
            AuditLog.objects.filter(
                action="presupuesto_catalogo_categoria_creada",
                object_id=str(category.id),
                user=self.yesenia,
            ).exists()
        )

    def test_responsable_de_otra_area_no_puede_dar_alta(self):
        self.client.force_login(self.johana)

        response = self.client.post(
            reverse("reportes:presupuesto_catalogos"),
            {
                "action": "create_rubro",
                "area": self.area_ventas.codigo,
                "concepto": "Publicidad",
                "tipo": RubroPresupuesto.TIPO_EGRESO,
            },
        )

        self.assertEqual(response.status_code, 403)
        self.assertFalse(RubroPresupuesto.objects.exists())

    def test_yesenia_no_puede_crear_fuera_de_sus_areas_gestionables(self):
        self.client.force_login(self.yesenia)

        response = self.client.post(
            reverse("reportes:presupuesto_catalogos"),
            {
                "action": "create_rubro",
                "area": self.area_produccion.codigo,
                "concepto": "Insumo fuera de alcance",
                "tipo": RubroPresupuesto.TIPO_COSTO,
            },
        )

        self.assertEqual(response.status_code, 403)
        self.assertFalse(RubroPresupuesto.objects.exists())

    def test_yesenia_puede_crear_en_compras_y_selector_limita_sus_areas(self):
        self.client.force_login(self.yesenia)
        page = self.client.get(reverse("reportes:presupuesto_catalogos"))

        self.assertEqual(
            {area.codigo for area in page.context["manageable_areas"]},
            {"administracion", "compras"},
        )
        response = self.client.post(
            reverse("reportes:presupuesto_catalogos"),
            {
                "action": "create_rubro",
                "area": self.area_compras.codigo,
                "concepto": "Fletes de compra",
                "tipo": RubroPresupuesto.TIPO_EGRESO,
            },
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(
            RubroPresupuesto.objects.filter(
                area=self.area_compras, concepto="Fletes de compra"
            ).exists()
        )

    def test_gestor_de_reportes_sin_responsabilidad_no_usa_alta_antigua_como_bypass(self):
        self.client.force_login(self.report_manager)

        response = self.client.post(
            reverse("reportes:presupuesto_maestro"),
            {
                "action": "add_rubro",
                "area": self.area_admin.codigo,
                "concepto": "Bypass",
                "tipo": RubroPresupuesto.TIPO_EGRESO,
            },
        )

        self.assertEqual(response.status_code, 403)
        self.assertFalse(RubroPresupuesto.objects.exists())


class PresupuestoCatalogosServiceTests(TestCase):
    def setUp(self):
        self.area = AreaPresupuesto.objects.create(
            codigo="produccion", nombre="Producción", orden=10
        )

    def test_alta_bloquea_equivalente_por_case_acento_puntuacion_y_espacios(self):
        RubroPresupuesto.objects.create(
            area=self.area,
            concepto="Agua purificada",
            codigo_cuenta="AGUA-01",
            tipo=RubroPresupuesto.TIPO_COSTO,
        )

        with self.assertRaisesRegex(ValueError, "Ya existe"):
            PresupuestoMaestroService().create_rubro_with_empty_year(
                area_code=self.area.codigo,
                concepto="  ÁGUA...   PURIFICADA ",
                codigo_cuenta="agua 01",
                tipo=RubroPresupuesto.TIPO_COSTO,
                year=2026,
                version="ORIGINAL",
            )

        self.assertEqual(RubroPresupuesto.objects.count(), 1)

    def test_mensaje_de_duplicado_inactivo_indica_como_localizarlo(self):
        RubroPresupuesto.objects.create(
            area=self.area,
            concepto="Agua purificada",
            codigo_cuenta="AGUA",
            tipo=RubroPresupuesto.TIPO_COSTO,
            activo=False,
        )

        with self.assertRaisesRegex(ValueError, "Inactivos"):
            PresupuestoCatalogoService().create_rubro(
                user=None,
                area_code=self.area.codigo,
                concepto="AGUA PURIFICADA",
                codigo_cuenta="agua",
                tipo=RubroPresupuesto.TIPO_COSTO,
                year=2026,
                version="ORIGINAL",
            )

    def test_listado_filtra_activos_inactivos_y_marca_equivalentes_globales(self):
        active = RubroPresupuesto.objects.create(
            area=self.area,
            concepto="Agua purificada",
            codigo_cuenta="AGUA",
            tipo=RubroPresupuesto.TIPO_COSTO,
            activo=True,
        )
        inactive = RubroPresupuesto.objects.create(
            area=self.area,
            concepto="ÁGUA... PURIFICADA",
            codigo_cuenta="agua",
            tipo=RubroPresupuesto.TIPO_COSTO,
            activo=False,
        )
        service = PresupuestoCatalogoService()

        active_rows = service.list_rows(
            period=date(2026, 7, 1), version="ORIGINAL"
        )
        inactive_rows = service.list_rows(
            period=date(2026, 7, 1), version="ORIGINAL", record_status="INACTIVOS"
        )
        all_rows = service.list_rows(
            period=date(2026, 7, 1), version="ORIGINAL", record_status="TODOS"
        )

        self.assertEqual([row["rubro"].id for row in active_rows], [active.id])
        self.assertEqual([row["rubro"].id for row in inactive_rows], [inactive.id])
        self.assertEqual({row["rubro"].id for row in all_rows}, {active.id, inactive.id})
        self.assertTrue(active_rows[0]["is_duplicate"])
        self.assertTrue(inactive_rows[0]["is_duplicate"])
        self.assertTrue(all(row["is_duplicate"] for row in all_rows))

    def test_pantalla_muestra_estado_textual_de_inactivos(self):
        inactive = RubroPresupuesto.objects.create(
            area=self.area,
            concepto="Rubro descontinuado",
            tipo=RubroPresupuesto.TIPO_COSTO,
            activo=False,
        )
        User = get_user_model()
        user = User.objects.create_user("responsable_inactivos", password="test")
        AreaPresupuestoResponsable.objects.create(usuario=user, area=self.area)
        self.client.force_login(user)

        response = self.client.get(
            reverse("reportes:presupuesto_catalogos"), {"status": "INACTIVOS"}
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, inactive.concepto)
        self.assertContains(response, "Inactivo")

    def test_normalizacion_preserva_siglas_operativas(self):
        self.assertEqual(
            normalize_display_name(
                "PAGO IMSS IVA ISR POS CEDIS CAPEX CFDI SAT ERP RRHH CFE PTU"
            ),
            "Pago IMSS IVA ISR POS CEDIS CAPEX CFDI SAT ERP RRHH CFE PTU",
        )

    def test_codigo_cuenta_conserva_guiones_y_normaliza_solo_formato(self):
        self.assertEqual(normalize_account_code("  renta-local / 01  "), "RENTA-LOCAL / 01")

    def test_servicio_rechaza_longitudes_antes_de_escribir(self):
        service = PresupuestoCatalogoService()
        category_cases = (
            ({"codigo": "X" * 51, "nombre": "Válida"}, "código"),
            ({"codigo": "VALIDA", "nombre": "X" * 161}, "nombre"),
        )
        for values, message in category_cases:
            with self.subTest(values=values):
                with self.assertRaisesRegex(ValueError, message):
                    service.create_category(
                        user=None,
                        capa_objetivo=CategoriaGasto.CAPA_EMPRESA,
                        bucket=CategoriaGasto.BUCKET_CORPORATIVO,
                        **values,
                    )

        rubro_cases = (
            ({"concepto": "X" * 201, "codigo_cuenta": "CUENTA"}, "concepto"),
            ({"concepto": "Rubro válido", "codigo_cuenta": "X" * 51}, "cuenta"),
        )
        for values, message in rubro_cases:
            with self.subTest(values=values):
                with self.assertRaisesRegex(ValueError, message):
                    service.create_rubro(
                        user=None,
                        area_code=self.area.codigo,
                        tipo=RubroPresupuesto.TIPO_COSTO,
                        year=2026,
                        version="ORIGINAL",
                        **values,
                    )

        self.assertFalse(CategoriaGasto.objects.exists())
        self.assertFalse(RubroPresupuesto.objects.exists())

    def test_endpoint_devuelve_400_por_longitud_invalida(self):
        User = get_user_model()
        manager = User.objects.create_user("manager_lengths", password="test")
        admin = AreaPresupuesto.objects.create(
            codigo="administracion", nombre="Administración", orden=1
        )
        AreaPresupuestoResponsable.objects.create(usuario=manager, area=admin)
        self.client.force_login(manager)

        response = self.client.post(
            reverse("reportes:presupuesto_catalogos"),
            {
                "action": "create_rubro",
                "area": admin.codigo,
                "concepto": "X" * 201,
                "codigo_cuenta": "CUENTA",
                "tipo": RubroPresupuesto.TIPO_COSTO,
                "year": 2026,
                "version": "ORIGINAL",
            },
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])
        self.assertFalse(RubroPresupuesto.objects.exists())

    def test_fuente_auto_exige_namespace_exacto_de_reglas_activas(self):
        rubro = RubroPresupuesto.objects.create(
            area=self.area,
            concepto="Fuentes combinadas",
            tipo=RubroPresupuesto.TIPO_COSTO,
        )
        ReglaFuenteRubro.objects.create(
            rubro=rubro,
            tipo_fuente=ReglaFuenteRubro.FUENTE_NOMINA,
        )
        ReglaFuenteRubro.objects.create(
            rubro=rubro,
            tipo_fuente=ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO,
        )
        line = LineaPresupuestoMensual(
            rubro=rubro,
            fuente_real="AUTO:GASTO_OPERATIVO+NOMINA",
        )
        rubro = RubroPresupuesto.objects.prefetch_related("reglas_fuente").get(pk=rubro.pk)
        line.rubro = rubro

        self.assertEqual(source_state_for(rubro, line), SOURCE_AUTO_WITH_DATA)
        for stale_source in (
            "AUTO:LEGADO",
            "AUTO:NOMINA+GASTO_OPERATIVO",
            "AUTO:GASTO_OPERATIVO",
            "AUTO:OTRA",
        ):
            with self.subTest(stale_source=stale_source):
                line.fuente_real = stale_source
                self.assertEqual(source_state_for(rubro, line), SOURCE_AUTO_WITHOUT_DATA)

    def test_listado_filtrado_no_crece_en_consultas_por_rubro(self):
        category = CategoriaGasto.objects.create(
            codigo="PERF",
            nombre="Rendimiento",
            capa_objetivo=CategoriaGasto.CAPA_EMPRESA,
            bucket=CategoriaGasto.BUCKET_CORPORATIVO,
        )
        for index in range(8):
            rubro = RubroPresupuesto.objects.create(
                area=self.area,
                concepto=f"Servicio {index}",
                tipo=RubroPresupuesto.TIPO_COSTO,
                metadata={"catalog_category_id": category.id},
            )
            ReglaFuenteRubro.objects.create(
                rubro=rubro,
                tipo_fuente=ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO,
            )

        with self.assertNumQueries(5):
            rows = PresupuestoCatalogoService().list_rows(
                period=date(2026, 7, 1),
                version="ORIGINAL",
                area_code=self.area.codigo,
                category_id=category.id,
                source_state=SOURCE_AUTO_WITHOUT_DATA,
                query="servicio",
            )
            self.assertEqual(len(rows), 8)

    def test_gasto_operativo_explica_que_se_calcula_desde_gastos_registrados(self):
        rubro = RubroPresupuesto.objects.create(
            area=self.area,
            concepto="Agua potable",
            tipo=RubroPresupuesto.TIPO_COSTO,
        )
        ReglaFuenteRubro.objects.create(
            rubro=rubro,
            tipo_fuente=ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO,
        )

        rows = PresupuestoCatalogoService().list_rows(
            period=date(2026, 7, 1),
            version="ORIGINAL",
            area_code=self.area.codigo,
        )

        row = next(item for item in rows if item["rubro"].id == rubro.id)
        self.assertEqual(row["source_state"], SOURCE_AUTO_WITHOUT_DATA)
        self.assertEqual(row["source_state_label"], "Aún no hay gastos registrados")
        self.assertEqual(
            row["source_help"],
            "Registra el recibo o gasto en Captura de gasto real; el importe de este rubro se actualizará solo.",
        )

        LineaPresupuestoMensual.objects.create(
            rubro=rubro,
            periodo=date(2026, 7, 1),
            version="ORIGINAL",
            monto_real=Decimal("875"),
            fuente_real="MANUAL:direccion",
        )
        manual_row = PresupuestoCatalogoService().list_rows(
            period=date(2026, 7, 1),
            version="ORIGINAL",
            area_code=self.area.codigo,
        )[0]
        self.assertEqual(manual_row["source_state_label"], "Manual")
        self.assertEqual(manual_row["source_help"], "")

    def test_rubro_conserva_categoria_historica_inactiva_en_jerarquia(self):
        category = CategoriaGasto.objects.create(
            codigo="HISTORICA",
            nombre="Categoría histórica",
            capa_objetivo=CategoriaGasto.CAPA_EMPRESA,
            bucket=CategoriaGasto.BUCKET_CORPORATIVO,
            activo=False,
        )
        rubro = RubroPresupuesto.objects.create(
            area=self.area,
            concepto="Servicio histórico",
            tipo=RubroPresupuesto.TIPO_COSTO,
            metadata={"catalog_category_id": category.id},
        )
        rubro_por_regla = RubroPresupuesto.objects.create(
            area=self.area,
            concepto="Servicio histórico por regla",
            tipo=RubroPresupuesto.TIPO_COSTO,
        )
        ReglaFuenteRubro.objects.create(
            rubro=rubro_por_regla,
            tipo_fuente=ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO,
            categoria_gasto=category,
        )

        rows = PresupuestoCatalogoService().list_rows(
            period=date(2026, 7, 1), version="ORIGINAL"
        )

        row = next(item for item in rows if item["rubro"].id == rubro.id)
        rule_row = next(item for item in rows if item["rubro"].id == rubro_por_regla.id)
        self.assertEqual(row["category_label"], "Categoría histórica")
        self.assertFalse(row["category"].activo)
        self.assertEqual(rule_row["category_label"], "Categoría histórica")
        self.assertFalse(rule_row["category"].activo)

    def test_pantalla_filtra_categorias_inactivas_y_selector_conserva_solo_activas(self):
        active = CategoriaGasto.objects.create(
            codigo="ACTIVA",
            nombre="Categoría activa",
            capa_objetivo=CategoriaGasto.CAPA_EMPRESA,
            bucket=CategoriaGasto.BUCKET_CORPORATIVO,
            activo=True,
        )
        inactive = CategoriaGasto.objects.create(
            codigo="INACTIVA",
            nombre="Categoría inactiva",
            capa_objetivo=CategoriaGasto.CAPA_EMPRESA,
            bucket=CategoriaGasto.BUCKET_CORPORATIVO,
            activo=False,
        )
        User = get_user_model()
        manager = User.objects.create_user("manager_categories", password="test")
        admin = AreaPresupuesto.objects.create(
            codigo="administracion", nombre="Administración", orden=1
        )
        AreaPresupuestoResponsable.objects.create(usuario=manager, area=admin)
        self.client.force_login(manager)

        response = self.client.get(
            reverse("reportes:presupuesto_catalogos"),
            {"category_status": "INACTIVAS"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(list(response.context["catalog_categories"]), [inactive])
        self.assertEqual(list(response.context["active_categories"]), [active])
        self.assertContains(response, "Categoría inactiva")
        self.assertContains(response, "Inactiva")

        all_response = self.client.get(
            reverse("reportes:presupuesto_catalogos"),
            {"category_status": "TODAS"},
        )
        self.assertEqual(
            list(all_response.context["catalog_categories"]),
            [active, inactive],
        )

    def test_colision_con_categoria_inactiva_indica_filtro_para_localizarla(self):
        CategoriaGasto.objects.create(
            codigo="SAT_SERV",
            nombre="Servicios SAT",
            capa_objetivo=CategoriaGasto.CAPA_EMPRESA,
            bucket=CategoriaGasto.BUCKET_CORPORATIVO,
            activo=False,
        )

        with self.assertRaisesRegex(ValueError, "Categorías: Inactivas"):
            PresupuestoCatalogoService().create_category(
                user=None,
                codigo="sat-serv",
                nombre="SERVICIOS SAT",
                capa_objetivo=CategoriaGasto.CAPA_EMPRESA,
                bucket=CategoriaGasto.BUCKET_CORPORATIVO,
            )

    def test_categoria_deriva_impactos_por_capa(self):
        service = PresupuestoCatalogoService()
        cases = (
            (
                "FAB",
                CategoriaGasto.CAPA_FABRICACION,
                CategoriaGasto.BUCKET_INDIRECTO,
                (True, False, True),
            ),
            (
                "SUC",
                CategoriaGasto.CAPA_SUCURSAL,
                CategoriaGasto.BUCKET_COMERCIAL,
                (False, True, True),
            ),
            (
                "EMP",
                CategoriaGasto.CAPA_EMPRESA,
                CategoriaGasto.BUCKET_CORPORATIVO,
                (False, False, True),
            ),
        )
        for code, layer, bucket, impacts in cases:
            with self.subTest(layer=layer):
                category = service.create_category(
                    user=None,
                    codigo=code,
                    nombre=f"Categoría {code}",
                    capa_objetivo=layer,
                    bucket=bucket,
                )
                self.assertEqual(
                    (
                        category.impacta_costo_producto,
                        category.impacta_contribucion_sucursal,
                        category.impacta_utilidad_empresa,
                    ),
                    impacts,
                )

    def test_categoria_rechaza_bucket_incompatible_con_capa(self):
        with self.assertRaisesRegex(ValueError, "no corresponde"):
            PresupuestoCatalogoService().create_category(
                user=None,
                codigo="INVALIDA",
                nombre="Categoría inválida",
                capa_objetivo=CategoriaGasto.CAPA_FABRICACION,
                bucket=CategoriaGasto.BUCKET_CORPORATIVO,
            )

    def test_listado_expone_los_cuatro_estados_sin_consolidar(self):
        categoria = CategoriaGasto.objects.create(
            codigo="SERVICIOS",
            nombre="Servicios",
            capa_objetivo=CategoriaGasto.CAPA_EMPRESA,
            bucket=CategoriaGasto.BUCKET_CORPORATIVO,
        )

        def rubro(nombre):
            return RubroPresupuesto.objects.create(
                area=self.area,
                concepto=nombre,
                tipo=RubroPresupuesto.TIPO_COSTO,
            )

        con_datos = rubro("Luz")
        sin_datos = rubro("Agua potable")
        manual = rubro("Renta")
        sin_configurar = rubro("Papelería")
        for item in (con_datos, sin_datos):
            ReglaFuenteRubro.objects.create(
                rubro=item,
                tipo_fuente=ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO,
                categoria_gasto=categoria,
            )
        ReglaFuenteRubro.objects.create(
            rubro=manual, tipo_fuente=ReglaFuenteRubro.FUENTE_MANUAL
        )
        LineaPresupuestoMensual.objects.create(
            rubro=con_datos,
            periodo=date(2026, 7, 1),
            version="ORIGINAL",
            monto_real=Decimal("900"),
            fuente_real="AUTO:GASTO_OPERATIVO",
        )
        LineaPresupuestoMensual.objects.create(
            rubro=sin_datos,
            periodo=date(2026, 7, 1),
            version="ORIGINAL",
            metadata={"sin_datos_fuente": True},
        )

        User = get_user_model()
        user = User.objects.create_user("carolina", password="test")
        AreaPresupuestoResponsable.objects.create(usuario=user, area=self.area)
        self.client.force_login(user)
        response = self.client.get(
            reverse("reportes:presupuesto_catalogos"),
            {"year": 2026, "month": 7, "area": self.area.codigo},
        )

        self.assertEqual(response.status_code, 200)
        rows = {row["rubro"].concepto: row for row in response.context["catalog_rows"]}
        self.assertEqual(rows["Luz"]["source_state"], "AUTO_CON_DATOS")
        self.assertEqual(rows["Agua potable"]["source_state"], "AUTO_SIN_DATOS")
        self.assertEqual(rows["Renta"]["source_state"], "MANUAL")
        self.assertEqual(rows["Papelería"]["source_state"], "SIN_CONFIGURAR")


class PresupuestoMaestroWritePermissionTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.reader = User.objects.create_user("reader", password="test")
        UserModuleAccess.objects.create(
            user=self.reader,
            module="reportes",
            access=UserModuleAccess.ACCESS_VIEW,
        )
        area = AreaPresupuesto.objects.create(codigo="ventas", nombre="Ventas")
        rubro = RubroPresupuesto.objects.create(
            area=area,
            concepto="Ventas",
            tipo=RubroPresupuesto.TIPO_INGRESO,
        )
        self.line = LineaPresupuestoMensual.objects.create(
            rubro=rubro,
            periodo=date(2026, 1, 1),
            monto_presupuesto=Decimal("10"),
        )
        self.client_api = APIClient()
        self.client_api.force_authenticate(self.reader)

    def test_lector_no_puede_actualizar_linea_por_api(self):
        response = self.client_api.put(
            reverse("api_presupuesto_linea", kwargs={"line_id": self.line.id}),
            {"monto_presupuesto": "999"},
            format="json",
        )

        self.assertEqual(response.status_code, 403)
        self.line.refresh_from_db()
        self.assertEqual(self.line.monto_presupuesto, Decimal("10"))

    def test_lector_no_puede_reescribir_las_lineas_de_un_rubro(self):
        response = self.client_api.post(
            reverse("api_presupuesto_rubro_lineas", kwargs={"rubro_id": self.line.rubro_id}),
            {"year": 2026, "01": "999"},
            format="json",
        )

        self.assertEqual(response.status_code, 403)
        self.line.refresh_from_db()
        self.assertEqual(self.line.monto_presupuesto, Decimal("10"))

    def test_lector_conserva_endpoints_get_y_no_ve_controles_de_escritura(self):
        rubros_response = self.client_api.get(reverse("api_presupuesto_rubros"))
        self.client.force_login(self.reader)
        maestro_response = self.client.get(reverse("reportes:presupuesto_maestro"))

        self.assertEqual(rubros_response.status_code, 200)
        self.assertEqual(maestro_response.status_code, 200)
        self.assertNotContains(maestro_response, "Agregar concepto")
        self.assertNotContains(maestro_response, 'data-line-id="')


class PresupuestoCatalogosConcurrencyTests(TransactionTestCase):
    reset_sequences = True

    def test_altas_concurrentes_equivalentes_crean_un_solo_rubro(self):
        # Prepara el catálogo base fuera de los hilos para que la carrera ejercite
        # exclusivamente el candado de deduplicación del alta de rubros.
        area = ensure_master_budget_areas()["produccion"]
        barrier = Barrier(2)
        result_lock = Lock()
        outcomes = []

        def worker(concepto, cuenta):
            close_old_connections()
            try:
                barrier.wait(timeout=5)
                result = PresupuestoCatalogoService().create_rubro(
                    user=None,
                    area_code=area.codigo,
                    concepto=concepto,
                    codigo_cuenta=cuenta,
                    tipo=RubroPresupuesto.TIPO_COSTO,
                    year=2026,
                    version="ORIGINAL",
                )
                outcome = ("created", result.rubro.id)
            except ValueError as exc:
                outcome = ("duplicate", str(exc))
            except Exception as exc:  # pragma: no cover - se reporta como fallo explícito abajo
                outcome = ("error", repr(exc))
            finally:
                connections.close_all()
            with result_lock:
                outcomes.append(outcome)

        threads = [
            Thread(target=worker, args=("Agua purificada", "AGUA-01")),
            Thread(target=worker, args=(" ÁGUA... PURIFICADA ", "agua 01")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertFalse(any(thread.is_alive() for thread in threads), outcomes)
        self.assertEqual(sorted(outcome[0] for outcome in outcomes), ["created", "duplicate"])
        self.assertEqual(RubroPresupuesto.objects.count(), 1)
        self.assertEqual(LineaPresupuestoMensual.objects.count(), 12)
