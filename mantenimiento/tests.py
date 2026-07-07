from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from activos.models import Activo, OrdenMantenimiento
from core.access import ACCESS_MANAGE
from core.models import Sucursal, UserModuleAccess
from core.navigation import build_nav_groups
from fallas.models import BitacoraFalla, CategoriaFalla, EvidenciaSeguimientoFalla, ReporteFalla
from logistica.models import Repartidor, ReporteUnidad, ServicioRealizadoUnidad, Unidad
from mantenimiento.models import ProveedorServicio
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

    def test_provider_api_uses_service_provider_catalog(self):
        Proveedor.objects.create(nombre="Proveedor insumos QA", activo=True)
        ProveedorServicio.objects.create(nombre="Proveedor importado QA", activo=True)
        ProveedorServicio.objects.create(nombre="Taller mantenimiento QA", especialidad="Refrigeracion", activo=True)
        self.client.force_login(self.mantenimiento)

        response = self.client.get("/api/mantenimiento/proveedores/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            [
                {"id": ProveedorServicio.objects.get(nombre="Proveedor importado QA").id, "nombre": "Proveedor importado QA"},
                {"id": ProveedorServicio.objects.get(nombre="Taller mantenimiento QA").id, "nombre": "Taller mantenimiento QA"},
            ],
        )

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
        self.assertContains(response, 'v=20260707-mant-tabs-row-v9')
        self.assertContains(response, 'evidence.classList.add("is-without-photo");')


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

    def test_followup_uploads_public_evidence_for_falla_timeline(self):
        self.client.force_login(self.user)

        response = self.client.post(
            "/api/mantenimiento/bandeja/falla/%s/actualizar/" % self.falla.id,
            {
                "estatus": ReporteFalla.ESTATUS_CERRADO,
                "comentario": "Se entrega funcionando con foto final.",
                "evidencias_seguimiento": SimpleUploadedFile("foto-final.jpg", b"img", content_type="image/jpeg"),
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

    def test_unit_service_rejects_unit_from_other_branch(self):
        self.client.force_login(self.user)
        servicios_before = ServicioRealizadoUnidad.objects.count()

        response = self.client.post(
            reverse("mantenimiento:crear-servicio"),
            {
                "modo_servicio": "realizado",
                "alcance": "unidad",
                "sucursal_id": self.branch.id,
                "unidad_id": self.other_unidad.id,
                "fecha_objetivo": timezone.localdate().isoformat(),
                "descripcion": "Intento cruzado de unidad.",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(ServicioRealizadoUnidad.objects.count(), servicios_before)

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
