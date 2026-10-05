from datetime import date
from unittest.mock import patch
from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.test import TestCase
from django.urls import reverse
from activos.models import Activo
from core.models import AuditLog, UserModuleAccess
from maestros.models import Proveedor
from reportes.models import AreaPresupuesto, AreaPresupuestoResponsable
from compras import models as cm


class ProcedenciaAdquisicionTests(TestCase):
    def setUp(self):
        self.actor = get_user_model().objects.create_superuser(
            "procedencia", "p@example.test", "test"
        )
        self.otro = get_user_model().objects.create_superuser(
            "procedencia2", "p2@example.test", "test"
        )
        self.area = AreaPresupuesto.objects.create(
            nombre="Procedencia", codigo="procedencia"
        )
        self.solicitud = cm.SolicitudCompraDepartamental.objects.create(
            area=self.area, solicitante=self.actor, periodo=date(2026, 10, 1)
        )
        self.item = cm.ItemCompraDepartamental.objects.create(
            solicitud=self.solicitud,
            descripcion="Paquete de equipos",
            unidad="Paquete",
            estado="ORDENADO",
        )
        proveedor = Proveedor.objects.create(nombre="Proveedor procedencia")
        quote = cm.CotizacionCompraDepartamental.objects.create(
            item=self.item, proveedor=proveedor, cantidad_ofertada=2, costo_unitario=125
        )
        self.intento = cm.IntentoCompraDepartamental.objects.create(
            item=self.item, cotizacion=quote
        )
        oc = cm.OrdenCompraDepartamental.objects.create(
            folio="OC-PROC", proveedor=proveedor, creado_por=self.actor
        )
        self.linea = cm.LineaOrdenCompraDepartamental.objects.create(
            orden=oc,
            item=self.item,
            intento=self.intento,
            cotizacion=quote,
            cantidad=2,
            costo_unitario=125,
            total=250,
        )
        self.recepcion = cm.RecepcionItemDepartamental.objects.create(
            linea_orden=self.linea, cantidad_recibida=1, registrado_por=self.actor
        )
        self.intento.refresh_from_db()
        self.activo = Activo.objects.create(codigo="PROC-1", nombre="Equipo existente")

    def confirmar(self, **extra):
        from compras.services_procedencia_adquisicion import confirmar_procedencia

        datos = dict(
            user=self.actor,
            recepcion_id=self.recepcion.pk,
            activo_id=self.activo.pk,
            version=self.intento.version,
            referencia_unidad="Unidad A",
            motivo="Documento revisado",
            evidencia="Folio P1",
            confirmado=True,
        )
        datos.update(extra)
        return confirmar_procedencia(**datos)

    def test_huellas_replay_global_y_unidad(self):
        from inventario.models import ExistenciaInsumo, MovimientoInventario

        self.intento.reembolso_solicitado = 50
        self.intento.save(update_fields=["reembolso_solicitado"])
        cm.ReembolsoCompraDepartamental.objects.create(
            intento=self.intento,
            importe=25,
            fecha=date(2026, 10, 1),
            registrado_por=self.actor,
        )
        fuentes = [
            cm.SolicitudCompraDepartamental,
            cm.CompraRealizadaDepartamental,
            cm.HistorialCompraDepartamental,
            cm.ReembolsoCompraDepartamental,
            cm.HistorialCotizacionDepartamental,
            ExistenciaInsumo,
            MovimientoInventario,
            cm.ItemCompraDepartamental,
            cm.IntentoCompraDepartamental,
            cm.RecepcionItemDepartamental,
            cm.LineaOrdenCompraDepartamental,
            cm.CotizacionCompraDepartamental,
            cm.OrdenCompraDepartamental,
            Activo,
        ]
        before = {m.__name__: list(m.objects.values()) for m in fuentes}
        vinculo, creado = self.confirmar()
        replay, again = self.confirmar(user=self.otro, referencia_unidad="  Unidad A  ")
        self.assertTrue(creado)
        self.assertFalse(again)
        self.assertEqual(replay.pk, vinculo.pk)
        self.assertEqual(replay.autor_id, self.actor.pk)
        self.assertEqual(
            AuditLog.objects.filter(model="compras.ProcedenciaAdquisicion").count(), 1
        )
        self.assertEqual(
            before, {m.__name__: list(m.objects.values()) for m in fuentes}
        )
        for values in ({"referencia_unidad": "Unidad B"}, {"motivo": "Distinto"}):
            with self.assertRaises(ValidationError) as exc:
                self.confirmar(**values)
            self.assertEqual(exc.exception.code, "conflict")
        otro = Activo.objects.create(codigo="PROC-2", nombre="Otro equipo del paquete")
        self.confirmar(activo_id=otro.pk, referencia_unidad="Unidad B")

    def test_correccion_stale_y_lectura_no_revisa(self):
        from compras.services_procedencia_adquisicion import procedencias_visibles

        vinculo, _ = self.confirmar()
        self.recepcion.cantidad_recibida = 0.5
        self.recepcion.save(update_fields=["cantidad_recibida"])
        with self.assertRaises(ValidationError) as exc:
            self.confirmar()
        self.assertEqual(exc.exception.code, "conflict")
        visible = procedencias_visibles(self.actor, activo_id=self.activo.pk)[0]
        self.assertTrue(visible.revision_pendiente)
        vinculo.refresh_from_db()
        self.assertEqual(vinculo.version_confirmada, self.intento.version)

    def test_cambio_coherente_del_articulo_del_intento_pide_revision_sin_reescribir(
        self,
    ):
        from compras.services_procedencia_adquisicion import procedencias_visibles

        vinculo, _ = self.confirmar()
        original = cm.ProcedenciaAdquisicion.objects.get(pk=vinculo.pk)
        original_values = {
            field.attname: getattr(original, field.attname)
            for field in original._meta.fields
        }
        otro_item = cm.ItemCompraDepartamental.objects.create(
            solicitud=self.solicitud, descripcion="Otro artículo sintético"
        )
        otra_cotizacion = cm.CotizacionCompraDepartamental.objects.create(
            item=otro_item,
            proveedor=self.intento.cotizacion.proveedor,
            cantidad_ofertada=1,
            costo_unitario=125,
        )
        version = self.intento.version
        # Writer real: el par artículo/cotización cambia de forma coherente y
        # conserva la versión existente; no se edita la confirmación documental.
        self.intento.item = otro_item
        self.intento.cotizacion = otra_cotizacion
        self.intento.save(update_fields=["item", "cotizacion"])
        self.intento.refresh_from_db()
        self.assertEqual(self.intento.version, version)
        visible = procedencias_visibles(self.actor, activo_id=self.activo.pk)[0]
        self.assertTrue(visible.revision_pendiente)
        fuentes = [
            cm.ItemCompraDepartamental,
            cm.CotizacionCompraDepartamental,
            cm.IntentoCompraDepartamental,
            cm.LineaOrdenCompraDepartamental,
            cm.RecepcionItemDepartamental,
            Activo,
            AuditLog,
        ]
        self.client.force_login(self.actor)
        before = {
            model.__name__: list(model.objects.order_by("pk").values())
            for model in fuentes
        }
        response = self.client.get(
            reverse("compras:procedencia_del_activo", args=[self.activo.pk])
        )
        self.assertContains(response, "Revisión pendiente")
        self.assertEqual(
            before,
            {
                model.__name__: list(model.objects.order_by("pk").values())
                for model in fuentes
            },
        )
        vinculo.refresh_from_db()
        self.assertEqual(
            original_values,
            {
                field.attname: getattr(vinculo, field.attname)
                for field in vinculo._meta.fields
            },
        )

    def test_permiso_gestor_recepcion_y_revocado(self):
        user = get_user_model().objects.create_user("area-proc")
        AreaPresupuestoResponsable.objects.create(area=self.area, usuario=user)
        permiso = UserModuleAccess.objects.create(
            user=user, module="inventario", access="manage"
        )
        with self.assertRaises(PermissionDenied):
            self.confirmar(user=user)
        UserModuleAccess.objects.create(user=user, module="compras", access="manage")
        self.confirmar(user=user)
        permiso.access = "view"
        permiso.save(update_fields=["access"])
        with self.assertRaises(PermissionDenied):
            self.confirmar(user=user)
        get_user_model().objects.filter(pk=self.actor.pk).update(is_active=False)
        with self.assertRaises(PermissionDenied):
            self.confirmar()

    def test_rollback_y_fuente_eliminada_no_restaurada(self):
        with patch(
            "compras.services_procedencia_adquisicion.AuditLog.objects.create",
            side_effect=RuntimeError,
        ):
            with self.assertRaises(RuntimeError):
                self.confirmar()
        self.assertEqual(cm.ProcedenciaAdquisicion.objects.count(), 0)
        vinculo, _ = self.confirmar()
        original = self.recepcion.pk
        self.recepcion.delete()
        vinculo.refresh_from_db()
        self.assertIsNone(vinculo.recepcion_id)
        self.recepcion = cm.RecepcionItemDepartamental.objects.create(
            pk=original,
            linea_orden=self.linea,
            cantidad_recibida=0.25,
            registrado_por=self.actor,
        )
        self.intento.refresh_from_db()
        with self.assertRaises(ValidationError) as exc:
            self.confirmar()
        self.assertEqual(exc.exception.code, "conflict")
        otro = Activo.objects.create(codigo="PROC-PK", nombre="Otro destino")
        with self.assertRaises(ValidationError) as error:
            self.confirmar(activo_id=otro.pk, referencia_unidad="Nueva unidad")
        self.assertEqual(error.exception.code, "conflict")

    def test_json_formulario_lector_y_get(self):
        self.client.force_login(self.actor)
        url = reverse("compras:procedencia_adquisicion", args=[self.recepcion.pk])
        self.assertEqual(self.client.get(url).status_code, 200)
        self.assertEqual(cm.ProcedenciaAdquisicion.objects.count(), 0)
        from django.test import Client

        csrf = Client(enforce_csrf_checks=True)
        csrf.force_login(self.actor)
        denied = csrf.post(url, {}, HTTP_ACCEPT="application/json")
        self.assertIn(denied.status_code, (302, 403))
        self.assertEqual(cm.ProcedenciaAdquisicion.objects.count(), 0)
        data = {
            "activo_id": self.activo.pk,
            "version": self.intento.version,
            "referencia_unidad": "Unidad A",
            "motivo": "A",
            "evidencia": "B",
            "confirmado": "on",
        }
        response = self.client.post(url, data, HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 201)
        data["motivo"] = "Conservar este texto"
        response = self.client.post(url, data, HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code, 409)
        self.assertIn(data["motivo"], response.json()["html"])
        self.assertNotIn('role="status"', response.json()["html"])
        self.assertIn(
            "Abrir recepción actual para revisar versión", response.json()["html"]
        )

        lector = get_user_model().objects.create_user("lector-proc")
        AreaPresupuestoResponsable.objects.create(area=self.area, usuario=lector)
        UserModuleAccess.objects.create(user=lector, module="inventario", access="view")
        self.client.force_login(lector)
        self.assertNotContains(self.client.get(url), "data-async-action>")
        self.assertEqual(self.client.post(url, data).status_code, 403)

    def test_referencia_unicode_limitada_antes_de_insertar(self):
        with self.assertRaises(ValidationError):
            self.confirmar(referencia_unidad="ß" * 200)
        self.assertEqual(cm.ProcedenciaAdquisicion.objects.count(), 0)
        self.assertEqual(
            AuditLog.objects.filter(model="compras.ProcedenciaAdquisicion").count(), 0
        )

    def test_confirmacion_inmutable_y_recepcion_positiva(self):
        vinculo, _ = self.confirmar()
        vinculo.evidencia = "Nueva evidencia"
        with self.assertRaises(ValidationError):
            vinculo.save(update_fields=["evidencia"])
        vinculo.refresh_from_db()
        self.assertEqual(vinculo.evidencia, "Folio P1")
        with self.assertRaises(ValidationError):
            cm.ProcedenciaAdquisicion(pk=vinculo.pk).save()

        cm.RecepcionItemDepartamental.objects.filter(pk=self.recepcion.pk).update(
            cantidad_recibida=0
        )
        with self.assertRaises(ValidationError) as error:
            self.confirmar()
        self.assertEqual(error.exception.code, "conflict")

    def test_constraint_ids_y_unicidad_global(self):
        from django.db import IntegrityError, transaction

        vinculo, _ = self.confirmar()
        for updates in (
            {"version_confirmada": 0},
            {"item_original_id": self.item.pk + 1},
            {"unidad_normalizada": ""},
        ):
            with transaction.atomic(), self.assertRaises(IntegrityError):
                cm.ProcedenciaAdquisicion.objects.filter(pk=vinculo.pk).update(
                    **updates
                )
        valores = {
            field.attname: getattr(vinculo, field.attname)
            for field in vinculo._meta.fields
            if field.name not in ("id", "creado_en")
        }
        for updates in (
            {
                "autor": self.otro,
                "autor_id": self.otro.pk,
                "autor_original_id": self.otro.pk,
            },
            {"unidad_normalizada": "otra unidad", "referencia_unidad": "Otra unidad"},
        ):
            copia = valores.copy()
            copia.update(updates)
            copia.pop("autor", None)
            with transaction.atomic(), self.assertRaises(IntegrityError):
                cm.ProcedenciaAdquisicion.objects.create(**copia)

    def test_dos_origenes_y_estado_reembolso_no_reescriben(self):
        vinculo, _ = self.confirmar()
        segunda = cm.RecepcionItemDepartamental.objects.create(
            linea_orden=self.linea, cantidad_recibida=0.25, registrado_por=self.actor
        )
        self.intento.refresh_from_db()
        otro, _ = self.confirmar(
            recepcion_id=segunda.pk,
            version=self.intento.version,
            referencia_unidad="Otro origen documentado",
        )
        self.assertNotEqual(otro.pk, vinculo.pk)
        cm.IntentoCompraDepartamental.objects.filter(pk=self.intento.pk).update(
            estado="REEMBOLSADO"
        )
        before = list(cm.IntentoCompraDepartamental.objects.values())
        self.client.force_login(self.actor)
        url = reverse("compras:procedencia_del_activo", args=[self.activo.pk])
        self.assertContains(self.client.get(url), "Reembolsado")
        self.assertEqual(before, list(cm.IntentoCompraDepartamental.objects.values()))

    def test_qr_scope_missing_y_batch_300(self):
        from django.db import connection
        from django.template import Context, Template
        from django.test import RequestFactory
        from django.test.utils import CaptureQueriesContext
        from core.models import Sucursal, UserProfile

        self.confirmar()
        lector = get_user_model().objects.create_user("lector-batch-proc")
        AreaPresupuestoResponsable.objects.create(area=self.area, usuario=lector)
        UserModuleAccess.objects.create(user=lector, module="inventario", access="view")
        area = AreaPresupuesto.objects.create(nombre="Privada", codigo="privada-proc")
        solicitud = cm.SolicitudCompraDepartamental.objects.create(
            area=area, solicitante=self.actor, periodo=date(2026, 10, 1)
        )
        item = cm.ItemCompraDepartamental.objects.create(
            solicitud=solicitud, descripcion="Privado"
        )
        self.area.activa = False
        self.area.save(update_fields=["activa"])
        tag = Template(
            "{% load destinos_documentales %}{% for pk in filas %}{% with fila=pk %}{% consulta_procedencia_activo request.user fila %}{% endwith %}{% endfor %}"
        )

        def render(count):
            request = RequestFactory().get("/activos/")
            request.user = lector
            with CaptureQueriesContext(connection) as queries:
                html = tag.render(
                    Context({"request": request, "filas": [self.activo.pk] * count})
                )
            return len(queries), html

        single, _ = render(1)
        rows = []
        for i in range(299):
            visible = i % 2 == 0
            rows.append(
                cm.ProcedenciaAdquisicion(
                    recepcion=self.recepcion,
                    linea=self.linea,
                    intento=self.intento,
                    item=self.item if visible else item,
                    activo=self.activo,
                    recepcion_original_id=self.recepcion.pk + 100 + i,
                    linea_original_id=self.linea.pk,
                    intento_original_id=self.intento.pk,
                    item_original_id=self.item.pk if visible else item.pk,
                    activo_original_id=self.activo.pk,
                    version_confirmada=self.intento.version,
                    referencia_unidad=f"Unidad {i}",
                    unidad_normalizada=f"unidad {i}",
                    autor=self.actor,
                    autor_original_id=self.actor.pk,
                    motivo="Visible" if visible else "Privado",
                    evidencia="Documento",
                )
            )
        # La prueba batch usa fuentes sintéticas eliminadas para la identidad; FK
        # de recepción nula conserva el tombstone sin fingir una nueva recepción.
        for row in rows:
            row.recepcion = None
        cm.ProcedenciaAdquisicion.objects.bulk_create(rows)
        multiple, html = render(300)
        self.assertEqual(single, multiple)
        self.assertLessEqual(multiple, 10)
        expected = reverse("compras:procedencia_del_activo", args=[self.activo.pk])
        self.assertEqual(html.count(expected), 300)
        from compras.services_procedencia_adquisicion import procedencias_visibles

        self.assertTrue(
            all(v.item_id == self.item.pk for v in procedencias_visibles(lector))
        )
        self.client.force_login(lector)
        self.assertNotContains(self.client.get(expected), "Privado")
        missing = Template(
            "{% load destinos_documentales %}{% consulta_procedencia_activo request.user pk %}{% recepciones_procedencia_item request.user pk as rs %}{{ rs|length }}"
        )
        with self.assertNumQueries(0):
            self.assertEqual(
                missing.render(
                    Context(
                        {"request": RequestFactory().get("/"), "pk": self.activo.pk}
                    )
                ),
                "0",
            )
        branch = Sucursal.objects.create(codigo="PROC-QR", nombre="Procedencia QR")
        self.activo.sucursal = branch
        self.activo.save(update_fields=["sucursal"])
        qr = get_user_model().objects.create_user("proc-qr-only")
        UserProfile.objects.update_or_create(user=qr, defaults={"sucursal": branch})
        self.client.force_login(qr)
        url = reverse("operacion:activo_pasaporte", args=[self.activo.qr_token])
        self.assertNotContains(self.client.get(url), "Procedencia de adquisición")
        UserModuleAccess.objects.create(user=qr, module="inventario", access="view")
        AreaPresupuestoResponsable.objects.create(area=self.area, usuario=qr)
        self.assertContains(self.client.get(url), expected)


from django.test import TransactionTestCase


class ProcedenciaConcurrenteTests(TransactionTestCase):
    setUp = ProcedenciaAdquisicionTests.setUp
    confirmar = ProcedenciaAdquisicionTests.confirmar

    def test_replay_dos_actores_concurrentes(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from django.db import close_old_connections

        barrier = Barrier(2)

        def run(actor):
            close_old_connections()
            try:
                barrier.wait(timeout=10)
                relation, created = self.confirmar(user=actor)
                return relation.pk, created, relation.autor_original_id
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(run, [self.actor, self.otro]))
        self.assertEqual(results[0][0], results[1][0])
        self.assertEqual(results[0][2], results[1][2])
        self.assertEqual(sum(result[1] for result in results), 1)
        self.assertEqual(cm.ProcedenciaAdquisicion.objects.count(), 1)
        self.assertEqual(
            AuditLog.objects.filter(model="compras.ProcedenciaAdquisicion").count(), 1
        )

    def test_correccion_que_posee_lock_invalida_confirmacion(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Event
        from django.db import close_old_connections, transaction

        locked, release = Event(), Event()

        def correction():
            close_old_connections()
            try:
                with transaction.atomic():
                    cm.ItemCompraDepartamental.objects.select_for_update().get(
                        pk=self.item.pk
                    )
                    locked.set()
                    release.wait(timeout=10)
                    source = cm.RecepcionItemDepartamental.objects.get(
                        pk=self.recepcion.pk
                    )
                    source.cantidad_recibida = 0.5
                    source.save(update_fields=["cantidad_recibida"])
            finally:
                close_old_connections()

        def confirm():
            close_old_connections()
            try:
                try:
                    self.confirmar()
                except ValidationError as exc:
                    return exc.code
                return "created"
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as pool:
            source = pool.submit(correction)
            self.assertTrue(locked.wait(timeout=10))
            pending = pool.submit(confirm)
            release.set()
            source.result(timeout=20)
            self.assertEqual(pending.result(timeout=20), "conflict")
        self.assertEqual(cm.ProcedenciaAdquisicion.objects.count(), 0)
