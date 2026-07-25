from datetime import timedelta
from decimal import Decimal
from pathlib import Path
import re
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.conf import settings
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from activos.models import Activo, OrdenMantenimiento, PlanMantenimiento
from core.access import ACCESS_MANAGE, ACCESS_VIEW
from core.models import Sucursal, UserModuleAccess
from core.navigation import build_nav_groups
from fallas.models import BitacoraFalla, CategoriaFalla, EvidenciaSeguimientoFalla, ReporteFalla
from logistica.models import Repartidor, ReporteUnidad, ServicioRealizadoUnidad, TipoServicioUnidad, Unidad
from mantenimiento.models import ProveedorServicio, SolicitudCancelacion
from maestros.models import Proveedor


class MantenimientoUnifiedAccessTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.mantenimiento = user_model.objects.create_user(
            username="jorge.isaac",
            email="jorge.isaac@example.com",
            password="test12345",
            first_name="Jorge",
            last_name="Isaac",
        )
        UserModuleAccess.objects.create(
            user=self.mantenimiento,
            module="mantenimiento",
            access=ACCESS_MANAGE,
        )
        branch = Sucursal.objects.create(codigo="MNTAC", nombre="Mantenimiento Acceso", activa=True)
        unidad = Unidad.objects.create(codigo="GS-MNT-AC", descripcion="Unidad acceso", sucursal=branch)
        Repartidor.objects.create(user=self.mantenimiento, sucursal=branch, unidad_asignada=unidad)
        self.compras = user_model.objects.create_user(username="compras_logistica", password="test12345")
        Group.objects.get_or_create(name="compras_logistica")[0].user_set.add(self.compras)

    def test_nav_shows_only_mantenimiento_for_maintenance_user(self):
        groups = build_nav_groups(self.mantenimiento, "/mantenimiento/")
        labels = [item["label"] for group in groups for item in group["items"]]

        self.assertIn("Mantenimiento", labels)
        self.assertNotIn("Logística", [group["label"] for group in groups])
        self.assertNotIn("Activos", labels)
        self.assertNotIn("Fallas", labels)

    def test_mantenimiento_permission_allows_portal_and_api(self):
        Proveedor.objects.create(nombre="Proveedor insumos QA", activo=True)
        ProveedorServicio.objects.create(nombre="Proveedor importado QA", activo=True)
        ProveedorServicio.objects.create(nombre="Taller mantenimiento QA", especialidad="Refrigeracion", activo=True)
        self.client.force_login(self.mantenimiento)

        portal = self.client.get(reverse("mantenimiento:dashboard"))
        perfil = self.client.get("/api/mantenimiento/me/")

        self.assertEqual(portal.status_code, 200)
        self.assertContains(portal, "Sucursales / CEDIS")
        self.assertContains(portal, "Logística")
        self.assertEqual(
            [p.nombre for p in portal.context["provider_options"]],
            ["Proveedor importado QA", "Taller mantenimiento QA"],
        )
        self.assertEqual(
            [p.nombre for p in portal.context["proveedores_todos"]],
            ["Proveedor importado QA", "Taller mantenimiento QA"],
        )
        self.assertEqual(perfil.status_code, 200)
        self.assertEqual(perfil.json()["username"], "jorge.isaac")

    def test_maintenance_pwa_serves_scoped_service_worker(self):
        self.client.force_login(self.mantenimiento)

        app = self.client.get(reverse("mantenimiento:app"))
        worker = self.client.get(reverse("mantenimiento:pwa-sw"))

        self.assertEqual(app.status_code, 200)
        self.assertContains(app, 'navigator.serviceWorker.register("/mantenimiento/sw.js?v=20260721-flota-fecha-v4", { scope: "/mantenimiento/" })')
        self.assertEqual(worker.status_code, 200)
        self.assertEqual(worker["Content-Type"], "application/javascript")
        worker_source = worker.content.decode()
        self.assertIn('const CACHE_PREFIX = "pollyanas-mantenimiento-pwa-";', worker_source)
        cache_version = re.search(r'const CACHE_VERSION = "([^"]+)";', worker_source).group(1)
        self.assertIn("const CACHE_NAME = `${CACHE_PREFIX}v20-${CACHE_VERSION}`;", worker_source)
        registration_source = app.content.decode()
        registration_version = re.search(r'/mantenimiento/sw\.js\?v=([^"&]+)', registration_source).group(1)
        self.assertEqual(cache_version, registration_version)
        self.assertIn("key.startsWith(CACHE_PREFIX) && key !== CACHE_NAME", worker_source)
        self.assertIn('url.pathname.startsWith("/api/")', worker_source)
        self.assertIn('url.pathname.startsWith("/media/")', worker_source)
        self.assertIn("event.respondWith(fetch(event.request));", worker_source)
        protected_branch = worker_source.split('event.request.mode === "navigate"', 1)[1].split("return;", 1)[0]
        self.assertNotIn("caches.match", protected_branch)
        self.assertIn('url.pathname.startsWith("/mantenimiento/")', protected_branch)
        self.assertIn("event.respondWith(fetch(event.request));", protected_branch)
        shell_assets = worker_source.split("const SHELL_ASSETS = [", 1)[1].split("];", 1)[0]
        self.assertNotIn('"/mantenimiento/app/"', shell_assets)
        self.assertIn('url.origin === self.location.origin', worker_source)
        self.assertIn('url.pathname.startsWith("/static/")', worker_source)
        self.assertIn('!event.request.headers.has("Authorization")', worker_source)
        activation_branch = worker_source.split('self.addEventListener("activate"', 1)[1].split('self.addEventListener("fetch"', 1)[0]
        self.assertIn("key.startsWith(CACHE_PREFIX)", activation_branch)
        self.assertNotIn("keys.filter((key) => key !== CACHE_NAME)", activation_branch)

    def test_provider_api_uses_service_provider_catalog(self):
        Proveedor.objects.create(nombre="Proveedor insumos QA", activo=True)
        ProveedorServicio.objects.create(nombre="Proveedor importado QA", activo=True)
        ProveedorServicio.objects.create(nombre="Taller mantenimiento QA", especialidad="Refrigeracion", activo=True)
        self.client.force_login(self.mantenimiento)

        response = self.client.get("/api/mantenimiento/proveedores/")

        self.assertEqual(response.status_code, 200)
        rows = response.json()
        self.assertEqual([row["nombre"] for row in rows], ["Proveedor importado QA", "Taller mantenimiento QA"])
        self.assertIn("telefono", rows[0])
        self.assertIn("especialidad", rows[0])

    def test_provider_api_can_create_mobile_provider(self):
        self.client.force_login(self.mantenimiento)

        response = self.client.post("/api/mantenimiento/proveedores/", {"nombre": "Taller móvil QA"}, content_type="application/json")

        self.assertEqual(response.status_code, 201)
        self.assertTrue(ProveedorServicio.objects.filter(nombre="Taller móvil QA", activo=True).exists())

        proveedor_id = response.json()["id"]
        update = self.client.patch(
            f"/api/mantenimiento/proveedores/{proveedor_id}/",
            {
                "nombre": "Taller móvil QA editado",
                "telefono": "6680000000",
                "especialidad": "Refrigeración",
                "activo": True,
            },
            content_type="application/json",
        )

        self.assertEqual(update.status_code, 200)
        self.assertTrue(ProveedorServicio.objects.filter(nombre="Taller móvil QA editado", telefono="6680000000").exists())

    def test_provider_api_can_import_general_provider(self):
        proveedor_general = Proveedor.objects.create(nombre="Taller general importable", activo=True)
        self.client.force_login(self.mantenimiento)

        listed = self.client.get("/api/mantenimiento/proveedores/importables/")
        imported = self.client.post(
            "/api/mantenimiento/proveedores/importar/",
            {"proveedor_ids": [proveedor_general.id]},
            content_type="application/json",
        )

        self.assertEqual(listed.status_code, 200)
        self.assertIn("Taller general importable", [row["nombre"] for row in listed.json()])
        self.assertEqual(imported.status_code, 201)
        self.assertTrue(ProveedorServicio.objects.filter(nombre="Taller general importable").exists())

    def test_compras_logistica_group_does_not_open_maintenance_without_permission(self):
        self.client.force_login(self.compras)

        portal = self.client.get(reverse("mantenimiento:dashboard"))
        perfil = self.client.get("/api/mantenimiento/me/")

        self.assertEqual(portal.status_code, 403)
        self.assertEqual(perfil.status_code, 403)

    def test_activos_access_can_open_maintenance_dashboard(self):
        user_model = get_user_model()
        activos_user = user_model.objects.create_user(username="activos_only", password="test12345")
        UserModuleAccess.objects.create(
            user=activos_user,
            module="activos",
            access=ACCESS_MANAGE,
        )
        self.client.force_login(activos_user)

        response = self.client.get(reverse("mantenimiento:dashboard"))

        self.assertEqual(response.status_code, 200)

    def test_mantenimiento_bandeja_access_can_open_maintenance_dashboard(self):
        user_model = get_user_model()
        bandeja_user = user_model.objects.create_user(username="mantenimiento_bandeja", password="test12345")
        UserModuleAccess.objects.create(
            user=bandeja_user,
            module="mantenimiento.bandeja",
            access=ACCESS_MANAGE,
        )
        self.client.force_login(bandeja_user)

        response = self.client.get(reverse("mantenimiento:dashboard"))

        self.assertEqual(response.status_code, 200)

    def test_dashboard_restores_active_tab_from_hash_changes(self):
        self.client.force_login(self.mantenimiento)

        response = self.client.get(reverse("mantenimiento:dashboard"))

        self.assertContains(response, 'window.addEventListener("hashchange", syncTabFromHash);')
        self.assertContains(response, 'if (location.hash === `#${tabId}`) {')
        self.assertContains(response, 'const button = event.target.closest("[data-open-follow]");')
        self.assertContains(response, 'const btn = event.target.closest("[data-open-cancelar]");')
        self.assertContains(response, 'if (event.target.closest("#btnNuevaFalla")) modal.classList.add("is-open");')
        self.assertContains(response, 'if (event.target.closest("#btnServicioRealizado")) open("realizado");')
        self.assertContains(response, 'reportMaintenanceInitFailure')
        self.assertContains(response, 'class="mant-money-prefix"')
        self.assertContains(response, 'v=20260721-mantenimiento-pruebas-v3')
        self.assertContains(response, 'evidence.classList.add("is-without-photo");')

    def test_pwa_shows_order_traceability_fields(self):
        self.client.force_login(self.mantenimiento)

        app = self.client.get(reverse("mantenimiento:app"))

        self.assertContains(app, "responsable_usuario_nombre")
        self.assertContains(app, "creado_por_nombre")
        self.assertContains(app, "ejecutado_por_nombre")

    def test_pwa_consumes_v2_inbox_history_and_lazy_detail_contracts(self):
        self.client.force_login(self.mantenimiento)

        app = self.client.get(reverse("mantenimiento:app"))

        self.assertContains(app, 'const API_V2 = `${API}/v2`;')
        self.assertContains(app, 'counts: {abiertos: 0, en_proceso: 0, criticos: 0, cerrados: 0}')
        self.assertContains(app, 'history: {periodo: "30d", tipo: "todo", estado: "todo", sucursal: "", unidad: "", page: 1')
        self.assertContains(app, "detailCache: new Map()")
        self.assertContains(app, "requestGeneration: {inbox: 0, history: 0, detail: 0}")
        self.assertContains(app, 'apiV2Fetch(`/items/${tipo}/${id}/`)')
        self.assertContains(app, 'periodo=${encodeURIComponent(state.history.periodo)}')
        self.assertContains(app, "Cargar más")
        self.assertContains(app, 'event.key === "Escape"')
        self.assertContains(app, 'aria-label="Cerrar imagen"')
        self.assertContains(app, 'prefers-reduced-motion: reduce')
        self.assertContains(app, "evidenceObjectUrls: new Set()")
        self.assertContains(app, "async function loadProtectedEvidence")
        self.assertContains(app, "await apiV2Fetch(url)")
        self.assertContains(app, "URL.createObjectURL(blob)")
        self.assertContains(app, "URL.revokeObjectURL(url)")
        self.assertContains(app, "Evidencia no disponible")
        self.assertContains(app, "historyLoading: false")
        self.assertContains(app, "const requestedPage = state.history.page")
        self.assertContains(app, "if (state.historyLoading) return")

    def test_pwa_history_can_filter_by_unit_and_show_authorized_costs(self):
        self.client.force_login(self.mantenimiento)

        app = self.client.get(reverse("mantenimiento:app"))

        self.assertContains(
            app,
            'history: {periodo: "30d", tipo: "todo", estado: "todo", sucursal: "", unidad: "", page: 1',
        )
        self.assertContains(app, "async function ensureUnidades()")
        self.assertContains(app, "await Promise.all([ensureSucursales(), ensureUnidades()])")
        self.assertContains(app, 'unidad=${encodeURIComponent(state.history.unidad)}')
        self.assertContains(app, "setHistoryFilter('unidad',this.value)")
        self.assertContains(app, "Todas las unidades")
        self.assertContains(app, "formatCurrency(item.costo)")
        self.assertContains(app, ".history-unit-filter{grid-column:1/-1}")
        self.assertContains(app, '<label class="history-unit-filter">Unidad')

    def test_pwa_service_scope_hides_and_disables_irrelevant_fields(self):
        self.client.force_login(self.mantenimiento)

        source = self.client.get(reverse("mantenimiento:app")).content.decode()
        mobile_theme = (Path(settings.BASE_DIR) / "static/operacion/app_theme.css").read_text()

        self.assertIn('id="servicio-sucursal-field"', source)
        self.assertIn(
            '<div class="field servicio-scope-field" id="servicio-sucursal-field">\n'
            '              <label for="servicio-sucursal">Sucursal</label>',
            source,
        )
        self.assertIn('id="servicio-activo-field"', source)
        self.assertIn('id="servicio-unidad-field"', source)
        self.assertIn('id="servicio-instalacion-field"', source)
        self.assertIn("setServicioFieldState", source)
        self.assertIn('const esFlota = alcance === "unidad";', source)
        self.assertIn("sucursal_id: esFlota ? null", source)
        self.assertIn("activo_id: alcance === \"activo\"", source)
        self.assertIn("instalacion_categoria: alcance === \"instalacion\"", source)
        self.assertIn('input[type="date"]', mobile_theme)
        self.assertIn("min-inline-size: 0", mobile_theme)
        self.assertIn("max-inline-size: 100%", mobile_theme)

    def test_pwa_does_not_duplicate_api_prefix_for_v2_requests(self):
        self.client.force_login(self.mantenimiento)

        app = self.client.get(reverse("mantenimiento:app"))

        self.assertContains(
            app,
            'path.startsWith("http") || path.startsWith("/api/") ? path : `${API}${path}`',
        )

    def test_pwa_closed_kpi_opens_30_day_history_and_does_not_bump_worker(self):
        self.client.force_login(self.mantenimiento)

        app = self.client.get(reverse("mantenimiento:app"))

        self.assertContains(app, "openClosedHistory()")
        self.assertContains(app, 'state.history.periodo = "30d"')
        self.assertContains(app, 'state.history.estado = "cerrado"')

    def test_pwa_history_catch_ignores_stale_request_before_mutating_ui(self):
        self.client.force_login(self.mantenimiento)

        source = self.client.get(reverse("mantenimiento:app")).content.decode()
        catch_start = source.index("catch (error) {", source.index("async function renderHistorial"))
        catch_end = source.index("}", source.index("return render(shell", catch_start))
        catch_source = source[catch_start:catch_end]

        guard = "if (generation !== state.requestGeneration.history) return;"
        mutation = "state.historyLoading = false;"
        render_error = "return render(shell"
        self.assertIn(guard, catch_source)
        self.assertLess(catch_source.index(guard), catch_source.index(mutation))
        self.assertLess(catch_source.index(guard), catch_source.index(render_error))

    def test_pwa_detail_and_evidence_async_contracts_are_stale_safe(self):
        self.client.force_login(self.mantenimiento)
        source = self.client.get(reverse("mantenimiento:app")).content.decode()

        self.assertIn('data-maintenance-uid="${esc(item.uid)}"', source)
        self.assertNotIn("onclick=\"openItemDetail('${esc(item.uid)}'", source)
        self.assertIn('event.target.closest("[data-maintenance-uid]")', source)
        detail_start = source.index("async function openItemDetail")
        generation = source.index("++state.requestGeneration.detail", detail_start)
        cache_read = source.index("state.detailCache.get(uid)", detail_start)
        self.assertLess(generation, cache_read)

        load_start = source.index("async function loadProtectedEvidence")
        await_blob = source.index("await evidenceBlob(url)", load_start)
        requery = source.index("container = document.getElementById(containerId)", await_blob)
        stale_guard = source.index("generation !== state.evidenceGeneration", requery)
        object_url = source.index("URL.createObjectURL(blob)", stale_guard)
        self.assertLess(await_blob, requery)
        self.assertLess(requery, stale_guard)
        self.assertLess(stale_guard, object_url)

    def test_pwa_viewer_focus_and_bounded_blob_cache_contracts(self):
        self.client.force_login(self.mantenimiento)
        source = self.client.get(reverse("mantenimiento:app")).content.decode()

        self.assertIn("closeImageViewer(false);", source)
        self.assertIn('event.key !== "Tab"', source)
        self.assertIn("event.shiftKey && document.activeElement === first", source)
        self.assertIn("state.viewerReturnFocus?.focus?.()", source)
        self.assertIn("state.evidenceBlobCache.size > 20", source)
        self.assertIn("state.evidenceBlobCache.keys().next().value", source)
        self.assertIn("state.evidenceBlobCache.clear()", source)
        self.assertIn('window.addEventListener("pagehide", clearEvidenceCache)', source)

    def test_pwa_inbox_thumbnails_use_authenticated_blob_pipeline(self):
        self.client.force_login(self.mantenimiento)
        source = self.client.get(reverse("mantenimiento:app")).content.decode()

        card_start = source.index("function bandejaItemCard")
        card_end = source.index("function statusClass", card_start)
        card_source = source[card_start:card_end]
        self.assertIn("loadProtectedThumbnail", card_source)
        self.assertNotIn('src="${esc(item.foto_inicial.url)}"', card_source)

        thumbnail_start = source.index("async function loadProtectedThumbnail")
        thumbnail_end = source.index("function revokeEvidenceUrls", thumbnail_start)
        thumbnail_source = source[thumbnail_start:thumbnail_end]
        self.assertIn("await evidenceBlob(url)", thumbnail_source)
        self.assertIn("generation !== state.evidenceGeneration", thumbnail_source)
        self.assertIn("URL.createObjectURL(blob)", thumbnail_source)
        self.assertIn("state.evidenceObjectUrls.add(objectUrl)", thumbnail_source)

    def test_pwa_detail_return_refinds_current_trigger_by_uid(self):
        self.client.force_login(self.mantenimiento)
        source = self.client.get(reverse("mantenimiento:app")).content.decode()

        return_start = source.index("async function returnFromDetail")
        return_end = source.index("function invalidateDetail", return_start)
        return_source = source[return_start:return_end]
        self.assertIn("await showScreen(target)", return_source)
        self.assertIn("state.detailReturn.uid", return_source)
        self.assertIn('document.querySelector(`[data-maintenance-uid="${CSS.escape(uid)}"]`)?.focus()', return_source)
        self.assertNotIn("detailReturn.focus", return_source)

        render_start = source.index("function render(html")
        render_end = source.index("function appGlyph", render_start)
        render_source = source[render_start:render_end]
        self.assertIn("return new Promise(resolve =>", render_source)
        self.assertIn("window.clearTimeout(pendingRenderTimer)", render_source)
        self.assertIn("pendingRenderResolve(false)", render_source)

        pending_start = source.index("async function renderPendientes")
        pending_end = source.index("async function renderNuevaFalla", pending_start)
        self.assertIn("return render(shell(`", source[pending_start:pending_end])

        history_start = source.index("async function renderHistorial")
        history_end = source.index("function detailRows", history_start)
        history_source = source[history_start:history_end]
        self.assertIn('return render(shell(`<button class="secondary-btn"', history_source)


class MantenimientoUnifiedInboxTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_user(username="mantenimiento", password="test12345")
        UserModuleAccess.objects.create(user=self.user, module="mantenimiento", access=ACCESS_MANAGE)
        self.reporter = user_model.objects.create_user(username="reporter", password="test12345")
        self.branch = Sucursal.objects.create(codigo="MNTQA", nombre="CEDIS QA", activa=True)
        self.activo = Activo.objects.create(
            nombre="Horno CEDIS",
            categoria="Hornos",
            ubicacion="Produccion CEDIS",
            sucursal=self.branch,
        )
        self.other_branch = Sucursal.objects.create(codigo="MNTQB", nombre="Sucursal QA B", activa=True)
        self.other_activo = Activo.objects.create(
            nombre="Vitrina Sucursal B",
            categoria="Vitrinas",
            ubicacion="Piso venta",
            sucursal=self.other_branch,
        )
        self.categoria = CategoriaFalla.objects.create(nombre="Equipo", tipo=CategoriaFalla.TIPO_EQUIPO)
        self.falla = ReporteFalla.objects.create(
            sucursal=self.branch,
            activo_relacionado=self.activo,
            categoria=self.categoria,
            titulo="Horno no calienta",
            descripcion="No llega a temperatura.",
            prioridad=ReporteFalla.PRIORIDAD_ALTA,
            foto_evidencia=SimpleUploadedFile("falla.jpg", b"img", content_type="image/jpeg"),
            reportado_por=self.reporter,
        )
        self.orden = OrdenMantenimiento.objects.create(
            activo_ref=self.activo,
            tipo=OrdenMantenimiento.TIPO_CORRECTIVO,
            prioridad=OrdenMantenimiento.PRIORIDAD_ALTA,
            descripcion="Revisar resistencia",
        )
        self.unidad = Unidad.objects.create(
            codigo="GS-PM1",
            descripcion="Panel logística",
            sucursal=self.branch,
            placa="ABC123",
        )
        self.other_unidad = Unidad.objects.create(
            codigo="GS-PM2",
            descripcion="Unidad sucursal B",
            sucursal=self.other_branch,
            placa="XYZ987",
        )
        self.repartidor = Repartidor.objects.create(user=self.reporter, sucursal=self.branch, unidad_asignada=self.unidad)
        ReporteUnidad.objects.bulk_create(
            [
                ReporteUnidad(
                    repartidor=self.repartidor,
                    unidad=self.unidad,
                    tipo=ReporteUnidad.TIPO_FALLA,
                    severidad=ReporteUnidad.SEVERIDAD_URGENTE,
                    descripcion="Ruido en motor",
                )
            ]
        )
        self.reporte_unidad = ReporteUnidad.objects.get(unidad=self.unidad, descripcion="Ruido en motor")

    def test_unified_inbox_keeps_branch_and_logistics_sources_separated(self):
        self.client.force_login(self.user)

        sucursales = self.client.get("/api/mantenimiento/bandeja/", {"origen": "sucursales"}).json()
        logistica = self.client.get("/api/mantenimiento/bandeja/", {"origen": "logistica"}).json()

        self.assertEqual({item["origen"] for item in sucursales["items"]}, {"sucursales"})
        self.assertEqual({item["origen"] for item in logistica["items"]}, {"logistica"})
        self.assertIn(f"falla:{self.falla.id}", [item["uid"] for item in sucursales["items"]])
        self.assertIn(f"orden:{self.orden.id}", [item["uid"] for item in sucursales["items"]])
        self.assertIn(f"unidad:{self.reporte_unidad.id}", [item["uid"] for item in logistica["items"]])

    def test_mobile_summary_includes_plan_and_fleet_agenda(self):
        today = timezone.localdate()
        plan = PlanMantenimiento.objects.create(
            activo_ref=self.activo,
            nombre="Limpieza profunda horno",
            tipo=PlanMantenimiento.TIPO_LIMPIEZA,
            frecuencia_dias=15,
            proxima_ejecucion=today - timedelta(days=2),
            responsable="Mantenimiento",
        )
        tipo = TipoServicioUnidad.objects.create(nombre="Cambio aceite", activo=True)
        ServicioRealizadoUnidad.objects.create(
            unidad=self.unidad,
            tipo_servicio=tipo,
            fecha_servicio=today,
            proxima_fecha=today + timedelta(days=5),
            proveedor="Taller QA",
            registrado_por=self.user,
        )
        self.client.force_login(self.user)

        response = self.client.get("/api/mantenimiento/resumen/")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["agenda_counts"]["vencidos"], 1)
        self.assertEqual(data["agenda_counts"]["urgentes"], 1)
        self.assertIn("sin_asignar", data["summary"])
        self.assertEqual([item["estado"] for item in data["agenda"][:2]], ["vencido", "urgente"])
        self.assertEqual({item["tipo"] for item in data["agenda"][:2]}, {"plan", "servicio_flota"})

        execute = self.client.post(
            f"/api/mantenimiento/resumen/planes/{plan.id}/ejecutar/",
            {"notas": "Hecho en recorrido"},
            content_type="application/json",
        )

        self.assertEqual(execute.status_code, 200)
        plan.refresh_from_db()
        self.assertEqual(plan.ultima_ejecucion, today)
        self.assertTrue(OrdenMantenimiento.objects.filter(plan_ref=plan, estatus=OrdenMantenimiento.ESTATUS_CERRADA).exists())

    def test_mobile_action_endpoints_create_operational_records(self):
        self.client.force_login(self.user)

        catalogos = self.client.get("/api/mantenimiento/catalogos/")
        falla = self.client.post(
            "/api/mantenimiento/fallas/",
            {
                "sucursal": self.branch.id,
                "categoria": self.categoria.id,
                "titulo": "Fuga en tarja",
                "descripcion": "Se detecta fuga durante recorrido.",
                "area": ReporteFalla.AREA_GENERAL,
                "prioridad": ReporteFalla.PRIORIDAD_MEDIA,
                "activo_id": self.activo.id,
            },
            content_type="application/json",
        )
        servicio = self.client.post(
            "/api/mantenimiento/servicios-puntuales/",
            {
                "modo_servicio": "pendiente",
                "alcance": "activo",
                "sucursal_id": self.branch.id,
                "activo_id": self.activo.id,
                "fecha_objetivo": timezone.localdate().isoformat(),
                "descripcion": "Programar cambio de empaque",
            },
            content_type="application/json",
        )
        unidad = self.client.post(
            "/api/mantenimiento/reportes-unidad/",
            {
                "unidad": self.unidad.id,
                "tipo": ReporteUnidad.TIPO_FALLA,
                "severidad": ReporteUnidad.SEVERIDAD_URGENTE,
                "descripcion": "Falla reportada desde mantenimiento móvil.",
            },
            content_type="application/json",
        )

        self.assertEqual(catalogos.status_code, 200)
        self.assertIn(
            {"id": self.user.id, "nombre": "mantenimiento"},
            catalogos.json()["responsables_mantenimiento"],
        )
        self.assertEqual(falla.status_code, 201)
        self.assertEqual(servicio.status_code, 201)
        self.assertEqual(unidad.status_code, 201)
        self.assertTrue(ReporteFalla.objects.filter(titulo="Fuga en tarja").exists())
        self.assertTrue(OrdenMantenimiento.objects.filter(descripcion="Programar cambio de empaque").exists())
        self.assertTrue(ReporteUnidad.objects.filter(descripcion="Falla reportada desde mantenimiento móvil.").exists())

    def test_mobile_fleet_service_does_not_require_branch(self):
        self.client.force_login(self.user)

        response = self.client.post(
            "/api/mantenimiento/servicios-puntuales/",
            {
                "modo_servicio": "pendiente",
                "alcance": "unidad",
                "unidad_id": self.other_unidad.id,
                "fecha_objetivo": (timezone.localdate() + timedelta(days=7)).isoformat(),
                "descripcion": "Revisión móvil sin sucursal artificial.",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        servicio = ServicioRealizadoUnidad.objects.get(
            tipo_servicio__nombre="Revisión móvil sin sucursal artificial."
        )
        self.assertEqual(servicio.unidad, self.other_unidad)

    def test_quick_asset_keeps_creator_and_exposes_author_name(self):
        self.client.force_login(self.user)

        response = self.client.post(
            "/api/mantenimiento/activos/rapido/",
            {
                "nombre": "Instalacion electrica QA",
                "sucursal": self.branch.id,
                "categoria": "Infraestructura",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        activo = Activo.objects.get(nombre="Instalacion electrica QA")
        self.assertEqual(activo.creado_por, self.user)
        self.assertEqual(response.json()["creado_por_nombre"], "mantenimiento")

    def test_service_assignment_and_close_keep_responsible_and_executor(self):
        self.client.force_login(self.user)

        create = self.client.post(
            "/api/mantenimiento/ordenes/",
            {
                "activo_ref": self.activo.id,
                "tipo": OrdenMantenimiento.TIPO_CORRECTIVO,
                "descripcion": "Cambiar resistencia",
                "responsable_usuario": self.reporter.id,
            },
            content_type="application/json",
        )

        self.assertEqual(create.status_code, 201)
        orden = OrdenMantenimiento.objects.get(pk=create.json()["id"])
        self.assertEqual(orden.creado_por, self.user)
        self.assertEqual(orden.responsable_usuario, self.reporter)
        self.assertEqual(create.json()["creado_por_nombre"], "mantenimiento")
        self.assertEqual(create.json()["responsable_usuario_nombre"], "reporter")

        close = self.client.patch(
            f"/api/mantenimiento/ordenes/{orden.id}/",
            {"estatus": OrdenMantenimiento.ESTATUS_CERRADA, "comentario": "Trabajo terminado"},
            content_type="application/json",
        )

        self.assertEqual(close.status_code, 200)
        orden.refresh_from_db()
        self.assertEqual(orden.ejecutado_por, self.user)
        self.assertEqual(close.json()["ejecutado_por_nombre"], "mantenimiento")

    def test_view_only_user_can_read_but_cannot_write_mobile_maintenance(self):
        user_model = get_user_model()
        view_user = user_model.objects.create_user(username="mantenimiento_view", password="test12345")
        UserModuleAccess.objects.create(user=view_user, module="mantenimiento.app", access=ACCESS_VIEW)
        self.client.force_login(view_user)

        read_response = self.client.get("/api/mantenimiento/resumen/")
        write_response = self.client.post(
            "/api/mantenimiento/fallas/",
            {
                "sucursal": self.branch.id,
                "categoria": self.categoria.id,
                "titulo": "Solo lectura no crea",
                "descripcion": "Este usuario no debe capturar.",
            },
            content_type="application/json",
        )

        self.assertEqual(read_response.status_code, 200)
        self.assertEqual(write_response.status_code, 403)
        self.assertFalse(ReporteFalla.objects.filter(titulo="Solo lectura no crea").exists())

    def test_mobile_failure_rejects_asset_from_other_branch(self):
        self.client.force_login(self.user)
        report_count = ReporteFalla.objects.count()

        response = self.client.post(
            "/api/mantenimiento/fallas/",
            {
                "sucursal": self.branch.id,
                "categoria": self.categoria.id,
                "titulo": "Activo cruzado",
                "descripcion": "No debe ligar activo de otra sucursal.",
                "activo_id": self.other_activo.id,
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(ReporteFalla.objects.count(), report_count)

    def test_mobile_admin_endpoints_manage_plans_and_cancellations(self):
        self.user.is_staff = True
        self.user.save(update_fields=["is_staff"])
        self.client.force_login(self.user)

        created = self.client.post(
            "/api/mantenimiento/planes/",
            {
                "activo_id": self.activo.id,
                "nombre": "Plan móvil horno",
                "tipo": PlanMantenimiento.TIPO_PREVENTIVO,
                "frecuencia_dias": 20,
                "proxima_ejecucion": timezone.localdate().isoformat(),
                "responsable": "Técnico móvil",
            },
            content_type="application/json",
        )
        plan_id = created.json()["id"]
        updated = self.client.patch(
            f"/api/mantenimiento/planes/{plan_id}/",
            {"nombre": "Plan móvil horno actualizado", "frecuencia_dias": 30},
            content_type="application/json",
        )
        deleted = self.client.delete(f"/api/mantenimiento/planes/{plan_id}/")
        solicitud = SolicitudCancelacion.objects.create(
            tipo=SolicitudCancelacion.TIPO_FALLA,
            objeto_id=self.falla.id,
            referencia=f"Falla #{self.falla.id}",
            motivo="Duplicado en pruebas",
            solicitado_por=self.reporter,
        )
        cancelaciones = self.client.get("/api/mantenimiento/cancelaciones/")
        resolved = self.client.post(
            f"/api/mantenimiento/cancelaciones/{solicitud.id}/resolver/",
            {"accion": "rechazar", "notas_resolucion": "Se conserva el reporte."},
            content_type="application/json",
        )

        self.assertEqual(created.status_code, 201)
        self.assertEqual(updated.status_code, 200)
        self.assertEqual(updated.json()["nombre"], "Plan móvil horno actualizado")
        self.assertEqual(deleted.status_code, 204)
        self.assertFalse(PlanMantenimiento.objects.get(pk=plan_id).activo)
        self.assertEqual(cancelaciones.status_code, 200)
        self.assertEqual(cancelaciones.json()["items"][0]["id"], solicitud.id)
        self.assertEqual(resolved.status_code, 200)
        solicitud.refresh_from_db()
        self.assertEqual(solicitud.estatus, SolicitudCancelacion.ESTATUS_RECHAZADA)

    def test_branch_failure_items_include_evidence_and_work_context(self):
        BitacoraFalla.objects.create(
            reporte=self.falla,
            usuario=self.user,
            estatus_anterior=ReporteFalla.ESTATUS_ABIERTO,
            estatus_nuevo=ReporteFalla.ESTATUS_REVISION,
            comentario="Se revisa evidencia antes de asignar proveedor.",
        )
        self.client.force_login(self.user)

        response = self.client.get("/api/mantenimiento/bandeja/", {"origen": "sucursales"})

        self.assertEqual(response.status_code, 200)
        item = next(row for row in response.json()["items"] if row["uid"] == f"falla:{self.falla.id}")
        self.assertIn("fallas/evidencias", item["foto_url"])
        self.assertEqual(item["reportado_por"], "reporter")
        self.assertEqual(item["ultimo_avance"], "Se revisa evidencia antes de asignar proveedor.")
        self.assertEqual(item["bitacora_total"], 1)

    def test_can_update_original_source_without_creating_duplicate_report(self):
        self.client.force_login(self.user)
        report_count = ReporteFalla.objects.count()

        response = self.client.post(
            "/api/mantenimiento/bandeja/falla/%s/actualizar/" % self.falla.id,
            {
                "estatus": ReporteFalla.ESTATUS_PROCESO,
                "costo_estimado": "1250.50",
                "proveedor_servicio": "Taller externo",
                "comentario": "Cotización recibida.",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.falla.refresh_from_db()
        self.assertEqual(self.falla.estatus, ReporteFalla.ESTATUS_PROCESO)
        self.assertEqual(str(self.falla.costo_estimado), "1250.50")
        self.assertEqual(ReporteFalla.objects.count(), report_count)

    def test_future_close_with_estimate_requires_explicit_final_amount_confirmation(self):
        self.falla.costo_estimado = Decimal("1250.50")
        self.falla.save(update_fields=["costo_estimado"])
        self.client.force_login(self.user)

        response = self.client.post(
            "/api/mantenimiento/bandeja/falla/%s/actualizar/" % self.falla.id,
            {"estatus": ReporteFalla.ESTATUS_RESUELTO, "comentario": "Trabajo terminado."},
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Confirma el importe final", response.json()["error"])
        self.falla.refresh_from_db()
        self.assertEqual(self.falla.estatus, ReporteFalla.ESTATUS_ABIERTO)
        self.assertIsNone(self.falla.costo_real)

    def test_future_close_can_confirm_estimate_as_final_without_recapturing_it(self):
        self.falla.costo_estimado = Decimal("1250.50")
        self.falla.save(update_fields=["costo_estimado"])
        self.client.force_login(self.user)

        response = self.client.post(
            "/api/mantenimiento/bandeja/falla/%s/actualizar/" % self.falla.id,
            {
                "estatus": ReporteFalla.ESTATUS_RESUELTO,
                "confirmar_costo_estimado": "true",
                "comentario": "Trabajo terminado por el importe cotizado.",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.falla.refresh_from_db()
        self.assertEqual(self.falla.estatus, ReporteFalla.ESTATUS_RESUELTO)
        self.assertEqual(self.falla.costo_real, Decimal("1250.50"))

    def test_pwa_future_close_sends_confirmation_or_explicit_real_cost(self):
        self.client.force_login(self.user)

        source = self.client.get(reverse("mantenimiento:app")).content.decode()

        self.assertIn('id="falla-costo-real"', source)
        self.assertIn("¿El importe final fue el mismo", source)
        self.assertIn("confirmar_costo_estimado", source)
        self.assertIn("costo_real: costoReal || null", source)
        self.assertIn('apiFetch(`/bandeja/falla/${id}/actualizar/`, {\n          method: "POST"', source)
        self.assertIn('apiFetch(`/bandeja/unidad/${id}/actualizar/`, {\n          method: "POST"', source)
        self.assertIn('value="${esc(String(item.costo_real || item.costo_estimado || ""))}"', source)
        self.assertIn('if (state.pantalla === "pendientes" && state.bandeja.some((item) => item.uid === uid))', source)
        self.assertIn("return abrirBandejaItemPorUid(uid);", source)

    def test_followup_uploads_public_evidence_for_falla_timeline(self):
        self.client.force_login(self.user)

        response = self.client.post(
            "/api/mantenimiento/bandeja/falla/%s/actualizar/" % self.falla.id,
            {
                "estatus": ReporteFalla.ESTATUS_CERRADO,
                "comentario": "Se entrega funcionando con foto final.",
                "evidencias_seguimiento": SimpleUploadedFile(
                    "foto-final.jpg", b"\xff\xd8\xffimagen", content_type="image/jpeg"
                ),
            },
        )

        self.assertEqual(response.status_code, 200)
        bitacora = BitacoraFalla.objects.get(reporte=self.falla, comentario="Se entrega funcionando con foto final.")
        evidencia = EvidenciaSeguimientoFalla.objects.get(bitacora=bitacora)
        self.assertEqual(evidencia.nombre, "foto-final.jpg")

    def test_followup_can_create_provider_and_asset_without_duplicate_report(self):
        self.client.force_login(self.user)
        self.falla.activo_relacionado = None
        self.falla.save(update_fields=["activo_relacionado"])
        report_count = ReporteFalla.objects.count()

        response = self.client.post(
            "/api/mantenimiento/bandeja/falla/%s/actualizar/" % self.falla.id,
            {
                "estatus": ReporteFalla.ESTATUS_REVISION,
                "proveedor_servicio": "Refrigeracion QA",
                "activo_nombre_nuevo": "Vitrina fria CEDIS QA",
                "activo_categoria_nueva": "Hornos",
                "activo_ubicacion_nueva": "Produccion CEDIS",
                "comentario": "Se registra activo faltante para seguimiento.",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.falla.refresh_from_db()
        self.assertEqual(ReporteFalla.objects.count(), report_count)
        self.assertEqual(self.falla.proveedor_servicio, "Refrigeracion QA")
        self.assertTrue(Proveedor.objects.filter(nombre="Refrigeracion QA", activo=True).exists())
        self.assertTrue(ProveedorServicio.objects.filter(nombre="Refrigeracion QA", activo=True).exists())
        self.assertIsNotNone(self.falla.activo_relacionado)
        self.assertEqual(self.falla.activo_relacionado.nombre, "Vitrina fria CEDIS QA")
        self.assertEqual(self.falla.activo_relacionado.proveedor_mantenimiento.nombre, "Refrigeracion QA")

    def test_followup_does_not_create_asset_with_uncataloged_category(self):
        self.client.force_login(self.user)
        self.falla.activo_relacionado = None
        self.falla.save(update_fields=["activo_relacionado"])

        response = self.client.post(
            "/api/mantenimiento/bandeja/falla/%s/actualizar/" % self.falla.id,
            {
                "estatus": ReporteFalla.ESTATUS_REVISION,
                "activo_nombre_nuevo": "Activo con categoria libre",
                "activo_categoria_nueva": "Refrigeracion con typo",
                "activo_ubicacion_nueva": "Produccion CEDIS",
                "comentario": "No debe crear catálogo nuevo por texto libre.",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.falla.refresh_from_db()
        self.assertIsNone(self.falla.activo_relacionado)
        self.assertFalse(Activo.objects.filter(nombre="Activo con categoria libre").exists())

    def test_logistics_followup_uses_final_cost_when_available(self):
        self.client.force_login(self.user)

        response = self.client.post(
            "/api/mantenimiento/bandeja/unidad/%s/actualizar/" % self.reporte_unidad.id,
            {
                "estatus": ReporteUnidad.ESTATUS_PROGRAMADO,
                "costo_estimado": "800.00",
                "costo_real": "975.25",
                "proveedor_servicio": "Taller Logistica QA",
                "comentario": "Factura recibida.",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.reporte_unidad.refresh_from_db()
        self.assertEqual(str(self.reporte_unidad.costo_servicio), "975.25")
        self.assertEqual(self.reporte_unidad.proveedor_servicio, "Taller Logistica QA")
        self.assertTrue(Proveedor.objects.filter(nombre="Taller Logistica QA", activo=True).exists())

    def test_can_register_completed_service_without_previous_order(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("mantenimiento:crear-servicio"),
            {
                "modo_servicio": "realizado",
                "sucursal_id": self.branch.id,
                "activo_id": self.activo.id,
                "tipo": OrdenMantenimiento.TIPO_CORRECTIVO,
                "prioridad": OrdenMantenimiento.PRIORIDAD_MEDIA,
                "origen": OrdenMantenimiento.ORIGEN_EMERGENCIA,
                "fecha_objetivo": timezone.localdate().isoformat(),
                "proveedor_servicio": "Taller Horno QA",
                "responsable": "Tecnico QA",
                "descripcion": "Cambio de resistencia sin reporte previo.",
                "costo_total": "1450.75",
                "nota_trabajo": "Equipo queda operativo.",
                "cerrar_servicio": "1",
            },
        )

        self.assertEqual(response.status_code, 302)
        orden = OrdenMantenimiento.objects.exclude(pk=self.orden.pk).get()
        self.assertEqual(orden.estatus, OrdenMantenimiento.ESTATUS_CERRADA)
        self.assertEqual(orden.origen, OrdenMantenimiento.ORIGEN_EMERGENCIA)
        self.assertEqual(str(orden.costo_otros), "1450.75")
        self.assertEqual(orden.proveedor_servicio.nombre, "Taller Horno QA")
        self.assertTrue(ProveedorServicio.objects.filter(nombre="Taller Horno QA", activo=True).exists())

    def test_one_off_future_service_is_shown_as_programmed(self):
        self.client.force_login(self.user)
        fecha_objetivo = timezone.localdate() + timedelta(days=30)

        response = self.client.post(
            reverse("mantenimiento:crear-servicio"),
            {
                "modo_servicio": "pendiente",
                "sucursal_id": self.branch.id,
                "activo_id": self.activo.id,
                "tipo": OrdenMantenimiento.TIPO_PREVENTIVO,
                "prioridad": OrdenMantenimiento.PRIORIDAD_MEDIA,
                "origen": OrdenMantenimiento.ORIGEN_INICIATIVA,
                "fecha_objetivo": fecha_objetivo.isoformat(),
                "descripcion": "Cambiar empaque de puerta antes de que falle.",
                "responsable": "Mantenimiento interno",
            },
        )

        self.assertEqual(response.status_code, 302)
        orden = OrdenMantenimiento.objects.get(descripcion="Cambiar empaque de puerta antes de que falle.")
        self.assertEqual(orden.estatus, OrdenMantenimiento.ESTATUS_PENDIENTE)
        self.assertEqual(orden.fecha_programada, fecha_objetivo)
        dashboard = self.client.get(reverse("mantenimiento:dashboard"))
        programado = next(col for col in dashboard.context["kanban_columns"] if col["key"] == "programado")
        self.assertIn(f"orden:{orden.id}", [item["uid"] for item in programado["items"]])

    def test_can_register_completed_logistics_unit_service_without_previous_report(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("mantenimiento:crear-servicio"),
            {
                "modo_servicio": "realizado",
                "alcance": "unidad",
                "sucursal_id": self.branch.id,
                "unidad_id": self.unidad.id,
                "fecha_objetivo": timezone.localdate().isoformat(),
                "proveedor_servicio": "Taller Unidad QA",
                "descripcion": "Cambio de aceite sin reporte previo.",
                "costo_total": "980.00",
                "nota_trabajo": "Servicio cerrado en ruta.",
            },
        )

        self.assertEqual(response.status_code, 302)
        servicio = ServicioRealizadoUnidad.objects.get(tipo_servicio__nombre="Cambio de aceite sin reporte previo.")
        self.assertEqual(servicio.unidad, self.unidad)
        self.assertEqual(str(servicio.costo), "980.00")
        self.assertEqual(servicio.proveedor, "Taller Unidad QA")

    def test_can_register_logistics_unit_service_without_branch(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("mantenimiento:crear-servicio"),
            {
                "modo_servicio": "realizado",
                "alcance": "flota",
                "unidad_id": self.other_unidad.id,
                "fecha_objetivo": timezone.localdate().isoformat(),
                "descripcion": "Afinación de unidad sin sucursal operativa.",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertNotIn(
            "Selecciona una sucursal.",
            [message.message for message in response.wsgi_request._messages],
        )
        servicio = ServicioRealizadoUnidad.objects.get(
            tipo_servicio__nombre="Afinación de unidad sin sucursal operativa."
        )
        self.assertEqual(servicio.unidad, self.other_unidad)

    def test_service_form_does_not_require_or_filter_branch_for_fleet(self):
        self.client.force_login(self.user)

        source = self.client.get(reverse("mantenimiento:dashboard")).content.decode()

        self.assertIn('id="ordenServicioSucursalField"', source)
        self.assertIn("sucursalField.hidden = esUnidad;", source)
        self.assertIn("sucursal.required = !esUnidad;", source)
        self.assertIn("sucursal.disabled = esUnidad;", source)
        self.assertIn('option.textContent = "Seleccionar unidad...";', source)
        self.assertIn("const visible = esUnidad ||", source)
        self.assertIn("20260715-mantenimiento-guardar-v2", source)
        css = (Path(settings.BASE_DIR) / "static/css/template_modules/templates-mantenimiento-dashboard.css").read_text()
        self.assertIn(".mant-field[hidden]{display:none}", css)

    def test_one_off_future_logistics_unit_service_is_scheduled(self):
        self.client.force_login(self.user)
        fecha_objetivo = timezone.localdate() + timedelta(days=15)

        response = self.client.post(
            reverse("mantenimiento:crear-servicio"),
            {
                "modo_servicio": "pendiente",
                "alcance": "unidad",
                "sucursal_id": self.branch.id,
                "unidad_id": self.unidad.id,
                "fecha_objetivo": fecha_objetivo.isoformat(),
                "descripcion": "Revisar balatas antes de ruta larga.",
                "responsable": "Mantenimiento interno",
            },
        )

        self.assertEqual(response.status_code, 302)
        servicio = ServicioRealizadoUnidad.objects.get(tipo_servicio__nombre="Revisar balatas antes de ruta larga.")
        self.assertEqual(servicio.unidad, self.unidad)
        self.assertEqual(servicio.proxima_fecha, fecha_objetivo)
        self.assertIsNone(servicio.costo)

    def test_active_service_rejects_asset_from_other_branch(self):
        self.client.force_login(self.user)
        ordenes_before = OrdenMantenimiento.objects.count()

        response = self.client.post(
            reverse("mantenimiento:crear-servicio"),
            {
                "modo_servicio": "realizado",
                "sucursal_id": self.branch.id,
                "activo_id": self.other_activo.id,
                "fecha_objetivo": timezone.localdate().isoformat(),
                "descripcion": "Intento cruzado de activo.",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(OrdenMantenimiento.objects.count(), ordenes_before)

    def test_unit_service_ignores_branch_assignment(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("mantenimiento:crear-servicio"),
            {
                "modo_servicio": "realizado",
                "alcance": "unidad",
                "sucursal_id": self.branch.id,
                "unidad_id": self.other_unidad.id,
                "fecha_objetivo": timezone.localdate().isoformat(),
                "descripcion": "Servicio de unidad independiente de sucursal.",
            },
        )

        self.assertEqual(response.status_code, 302)
        servicio = ServicioRealizadoUnidad.objects.get(
            tipo_servicio__nombre="Servicio de unidad independiente de sucursal."
        )
        self.assertEqual(servicio.unidad, self.other_unidad)

    def test_can_register_installation_service_by_branch_without_asset_selection(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("mantenimiento:crear-servicio"),
            {
                "modo_servicio": "realizado",
                "alcance": "instalacion",
                "sucursal_id": self.branch.id,
                "instalacion_categoria": "Plomería",
                "fecha_objetivo": timezone.localdate().isoformat(),
                "proveedor_servicio": "Plomero QA",
                "responsable": "Mantenimiento interno",
                "descripcion": "Reparación de fuga en baño.",
                "costo_total": "720.50",
                "cerrar_servicio": "1",
            },
        )

        self.assertEqual(response.status_code, 302)
        activo_instalacion = Activo.objects.get(nombre=f"Plomería - {self.branch.nombre}")
        orden = OrdenMantenimiento.objects.get(descripcion="Reparación de fuga en baño.")
        self.assertEqual(activo_instalacion.sucursal, self.branch)
        self.assertEqual(activo_instalacion.categoria, "Plomería")
        self.assertEqual(orden.activo_ref, activo_instalacion)
        self.assertEqual(orden.estatus, OrdenMantenimiento.ESTATUS_CERRADA)
        self.assertEqual(str(orden.costo_otros), "720.50")
        self.assertEqual(orden.proveedor_servicio.nombre, "Plomero QA")

    def test_one_off_future_installation_service_is_programmed(self):
        self.client.force_login(self.user)
        fecha_objetivo = timezone.localdate() + timedelta(days=20)

        response = self.client.post(
            reverse("mantenimiento:crear-servicio"),
            {
                "modo_servicio": "pendiente",
                "alcance": "instalacion",
                "sucursal_id": self.branch.id,
                "instalacion_categoria": "Pintura / obra civil",
                "fecha_objetivo": fecha_objetivo.isoformat(),
                "descripcion": "Pintar pared antes de temporada alta.",
                "responsable": "Mantenimiento interno",
            },
        )

        self.assertEqual(response.status_code, 302)
        activo_instalacion = Activo.objects.get(nombre=f"Pintura / obra civil - {self.branch.nombre}")
        orden = OrdenMantenimiento.objects.get(descripcion="Pintar pared antes de temporada alta.")
        self.assertEqual(orden.activo_ref, activo_instalacion)
        self.assertEqual(orden.estatus, OrdenMantenimiento.ESTATUS_PENDIENTE)
        self.assertEqual(orden.fecha_programada, fecha_objetivo)
        dashboard = self.client.get(reverse("mantenimiento:dashboard"))
        programado = next(col for col in dashboard.context["kanban_columns"] if col["key"] == "programado")
        self.assertIn(f"orden:{orden.id}", [item["uid"] for item in programado["items"]])

    def test_maintenance_can_open_unit_report_form_without_logistics_permission(self):
        self.client.force_login(self.user)

        response = self.client.get(reverse("mantenimiento:crear-reporte-unidad"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Nuevo reporte de unidad")
        self.assertContains(response, "Levanta desde Mantenimiento")
        self.assertContains(response, self.unidad.codigo)

    def test_maintenance_can_create_unit_report_for_missing_driver_capture(self):
        self.client.force_login(self.user)
        initial_count = ReporteUnidad.objects.count()

        with patch("logistica.signals.notificar_reporte_nuevo.delay") as notify_delay:
            response = self.client.post(
                reverse("mantenimiento:crear-reporte-unidad"),
                {
                    "unidad": str(self.unidad.id),
                    "repartidor": str(self.repartidor.id),
                    "tipo": ReporteUnidad.TIPO_LLANTA,
                    "severidad": ReporteUnidad.SEVERIDAD_CRITICO,
                    "descripcion": "Llanta trasera reportada por llamada, no se capturo en app.",
                    "kilometraje": "88210",
                },
            )

        self.assertRedirects(response, reverse("mantenimiento:dashboard"))
        self.assertEqual(ReporteUnidad.objects.count(), initial_count + 1)
        reporte = ReporteUnidad.objects.latest("id")
        notify_delay.assert_called_once_with(reporte.id)
        self.assertEqual(reporte.unidad, self.unidad)
        self.assertEqual(reporte.repartidor, self.repartidor)
        self.assertEqual(reporte.tipo, ReporteUnidad.TIPO_LLANTA)
        self.assertEqual(reporte.severidad, ReporteUnidad.SEVERIDAD_CRITICO)
        self.assertEqual(reporte.estatus, ReporteUnidad.ESTATUS_ABIERTO)
        self.assertEqual(reporte.kilometraje, 88210)
        self.assertEqual(reporte.asignado_a, self.user)
        self.assertIn("Mantenimiento", reporte.notas_compras)

    def test_maintenance_unit_report_form_does_not_create_invalid_report(self):
        self.client.force_login(self.user)
        initial_count = ReporteUnidad.objects.count()

        response = self.client.post(
            reverse("mantenimiento:crear-reporte-unidad"),
            {
                "unidad": "",
                "tipo": ReporteUnidad.TIPO_FALLA,
                "severidad": ReporteUnidad.SEVERIDAD_URGENTE,
                "descripcion": "",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(ReporteUnidad.objects.count(), initial_count)
        self.assertContains(response, "Selecciona una unidad.")
        self.assertContains(response, "La descripción es obligatoria.")

    def test_maintenance_unit_report_rejects_negative_kilometraje(self):
        self.client.force_login(self.user)
        initial_count = ReporteUnidad.objects.count()

        response = self.client.post(
            reverse("mantenimiento:crear-reporte-unidad"),
            {
                "unidad": str(self.unidad.id),
                "tipo": ReporteUnidad.TIPO_FALLA,
                "severidad": ReporteUnidad.SEVERIDAD_INFORMATIVO,
                "descripcion": "Validacion directa desde mantenimiento.",
                "kilometraje": "-1",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(ReporteUnidad.objects.count(), initial_count)
        self.assertContains(response, "El kilometraje no puede ser negativo.")

    def test_maintenance_unit_report_rejects_non_image_evidence(self):
        self.client.force_login(self.user)
        initial_count = ReporteUnidad.objects.count()

        response = self.client.post(
            reverse("mantenimiento:crear-reporte-unidad"),
            {
                "unidad": str(self.unidad.id),
                "tipo": ReporteUnidad.TIPO_FALLA,
                "severidad": ReporteUnidad.SEVERIDAD_INFORMATIVO,
                "descripcion": "Archivo no permitido desde mantenimiento.",
                "foto": SimpleUploadedFile("evidencia.txt", b"texto", content_type="text/plain"),
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(ReporteUnidad.objects.count(), initial_count)
        self.assertContains(response, "La evidencia debe ser una imagen JPG o PNG.")

    def test_dashboard_shows_maintenance_instruction_actions(self):
        self.client.force_login(self.user)

        response = self.client.get(reverse("mantenimiento:dashboard"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "+ Falla / imprevisto")
        self.assertContains(response, "+ Servicio sin orden")
        self.assertContains(response, "+ Programar servicio")


class MantenimientoServiceFormMarkupTests(TestCase):
    databases = []

    def test_direct_service_form_reports_client_validation_and_normalizes_scope(self):
        source = (Path(settings.BASE_DIR) / "templates/mantenimiento/dashboard.html").read_text()

        self.assertIn('id="ordenServicioForm"', source)
        self.assertIn('id="ordenServicioError"', source)
        self.assertIn('class="mant-form-error"', source)
        self.assertIn("form.addEventListener(\"submit\"", source)
        self.assertIn("setAlcance(alcance.value);", source)
        self.assertIn("form.checkValidity()", source)
        self.assertIn('unidad_id: "Unidad logística"', source)
        self.assertIn("field.name && !field.disabled", source)
        self.assertIn("syncSearchableState(sucursal, !esUnidad, !esUnidad);", source)
        base = (Path(settings.BASE_DIR) / "templates/base.html").read_text()
        service_worker = (Path(settings.BASE_DIR) / "static/erp-sw.js").read_text()
        searchable_selects = (Path(settings.BASE_DIR) / "static/js/searchable_selects.js").read_text()
        css = (Path(settings.BASE_DIR) / "static/css/template_modules/templates-mantenimiento-dashboard.css").read_text()
        self.assertIn("20260715-mantenimiento-guardar-v2", base)
        self.assertIn("20260721-mantenimiento-pruebas-v19", base)
        self.assertIn("pollyanas-erp-shell-v19-mantenimiento-pruebas", service_worker)
        self.assertIn("if (select.disabled || input.disabled) return;", searchable_selects)
        self.assertIn(".mant-form-error", css)
