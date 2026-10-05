from concurrent.futures import ThreadPoolExecutor
from datetime import date
from hashlib import sha256
import json
from threading import Barrier
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import close_old_connections, connection, transaction, IntegrityError
from django.template import Context, Template
from django.test import TestCase, TransactionTestCase, RequestFactory, Client
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone

from activos.models import Activo, OrdenMantenimiento, BitacoraMantenimiento
from core.models import Sucursal, AuditLog, UserModuleAccess, UserProfile
from fallas.models import CategoriaFalla, ReporteFalla
from reportes.models import (AreaPresupuesto, AreaPresupuestoResponsable, CentroCosto, CategoriaGasto,
    RubroPresupuesto, ObligacionGasto, GastoOperativoMensual, PagoObligacionGasto, ParcialidadObligacionGasto)
from sat_client.models import CfdiDescargado
from syncfy_client.models import CuentaBancaria, MovimientoBancario
from .models import DocumentoFinancieroTrabajo
from .services_documentos_financieros import confirmar_documento, documentos_visibles, fuentes_autorizadas


class DocumentosFinancierosTests(TestCase):
    def setUp(self):
        self.actor = get_user_model().objects.create_superuser("doc-gestor", password="test")
        self.otro = get_user_model().objects.create_superuser("doc-otro", password="test")
        self.branch = Sucursal.objects.create(codigo="DOC", nombre="Documental")
        self.asset = Activo.objects.create(codigo="DOC-1", nombre="Equipo", sucursal=self.branch)
        self.orden = OrdenMantenimiento.objects.create(folio="DOC-OM", activo_ref=self.asset)
        cat = CategoriaFalla.objects.create(nombre="Instalación", tipo="instalacion")
        self.falla = ReporteFalla.objects.create(sucursal=self.branch, categoria=cat, titulo="Instalación pendiente", descripcion="Revisar",
            reportado_por=self.actor, tipo_objetivo="INSTALACION", area_instalacion="Pared", justificacion_sin_foto="Síntesis", costo_real=None, costo_estimado=None)
        self.area = AreaPresupuesto.objects.create(codigo="doc-area", nombre="Área documental")
        self.centro = CentroCosto.objects.create(codigo="DOC", nombre="Centro", tipo="CORPORATIVO")
        self.categoria = CategoriaGasto.objects.create(codigo="DOC", nombre="Categoría", capa_objetivo="OPEX")
        self.rubro = RubroPresupuesto.objects.create(area=self.area, concepto="Trabajo", tipo="EGRESO")
        self.gasto = GastoOperativoMensual.objects.create(periodo=date(2026,10,1), centro_costo=self.centro, categoria_gasto=self.categoria, monto=125)
        self.obligacion = self.nueva_obligacion(gasto_operativo=self.gasto)
        self.cfdi = CfdiDescargado.objects.create(uuid="doc-cfdi", rfc_emisor="AAA010101AAA", rfc_receptor="BBB010101BBB", subtotal=100,total=116,tipo_comprobante="I",tipo_cfdi="recibido",fecha_emision=timezone.now())
        cuenta = CuentaBancaria.objects.create(banco="bbva", nombre_display="Banco", id_site_syncfy="doc")
        self.movimiento = MovimientoBancario.objects.create(id_transaction="doc-tx", cuenta=cuenta, descripcion="Pago documento", monto=116,tipo="cargo", fecha_transaccion=timezone.now(),fecha_refresh=timezone.now(), cfdi_relacionado=self.cfdi)
        self.data = dict(tipo_trabajo="orden", trabajo_id=self.orden.pk, tipo_documento="obligacion", documento_id=self.obligacion.pk, motivo="Trabajo revisado", evidencia="Documento folio D1", confirmado=True)
        self.url = reverse("mantenimiento:documentos-trabajo", args=["orden",self.orden.pk])

    def nueva_obligacion(self, **kw):
        return ObligacionGasto.objects.create(origen="VARIABLE", area=self.area,rubro=self.rubro,centro_costo=self.centro,categoria_gasto=self.categoria,concepto="Documento privado",periodo=date(2026,10,1),fecha_gasto=date(2026,10,1),fecha_vencimiento=date(2026,10,1),monto_reconocido=125,**kw)

    def confirmar(self, user=None, **kw):
        return confirmar_documento(user=user or self.actor, **{**self.data, **kw})

    def huellas(self):
        modelos = [Activo, OrdenMantenimiento, ReporteFalla, BitacoraMantenimiento, ObligacionGasto, GastoOperativoMensual, PagoObligacionGasto, ParcialidadObligacionGasto, CfdiDescargado, MovimientoBancario]
        return {m.__name__: sha256(json.dumps(list(m.objects.order_by("pk").values()), default=str, sort_keys=True).encode()).hexdigest() for m in modelos}

    def test_espejo_alias_replay_mn_fuentes_inalteradas(self):
        before = self.huellas()
        v, creado = self.confirmar()
        replay, creado2 = self.confirmar(user=self.otro,tipo_documento="gasto",documento_id=self.gasto.pk)
        self.assertTrue(creado); self.assertFalse(creado2); self.assertEqual(v.pk,replay.pk)
        self.assertEqual(replay.autor_id,self.actor.pk)
        self.confirmar(tipo_documento="cfdi", documento_id=self.cfdi.pk)
        self.confirmar(tipo_documento="movimiento", documento_id=self.movimiento.pk)
        self.confirmar(tipo_trabajo="falla", trabajo_id=self.falla.pk)
        self.assertEqual(DocumentoFinancieroTrabajo.objects.count(),4)
        self.assertEqual(AuditLog.objects.filter(model="mantenimiento.DocumentoFinancieroTrabajo").count(),4)
        self.assertEqual(before,self.huellas())
        self.falla.refresh_from_db(); self.assertIsNone(self.falla.costo_real); self.assertIsNone(self.falla.costo_estimado)
        with self.assertRaises(ValidationError) as exc: self.confirmar(evidencia="Otra")
        self.assertEqual(exc.exception.code,"conflict")

    def test_fuente_financiera_creada_despues_conserva_confirmacion_original(self):
        independiente = GastoOperativoMensual.objects.create(periodo=date(2026,10,1), centro_costo=self.centro,categoria_gasto=self.categoria,monto=33)
        v, _ = self.confirmar(tipo_documento="gasto",documento_id=independiente.pk)
        obligacion = self.nueva_obligacion(gasto_operativo=independiente)
        replay, creado = self.confirmar(tipo_documento="obligacion",documento_id=obligacion.pk)
        self.assertEqual(replay.pk,v.pk); self.assertFalse(creado); self.assertIsNone(replay.obligacion_original_id)
        self.assertEqual(DocumentoFinancieroTrabajo.objects.count(),1)
        # Una obligación sin espejo conserva su confirmación cuando su fuente añade el espejo.
        nueva = self.nueva_obligacion()
        primera, _ = self.confirmar(documento_id=nueva.pk)
        otro = GastoOperativoMensual.objects.create(periodo=date(2026,10,1),centro_costo=self.centro,categoria_gasto=self.categoria,monto=22)
        ObligacionGasto.objects.filter(pk=nueva.pk).update(gasto_operativo=otro)
        replay, creado = self.confirmar(tipo_documento="gasto",documento_id=otro.pk)
        self.assertFalse(creado); self.assertEqual(replay.pk,primera.pk); self.assertIsNone(replay.gasto_original_id)

    def test_permisos_actuales_revocacion_sql_scope_y_readonly(self):
        user = get_user_model().objects.create_user("doc-area")
        for module in ("mantenimiento", "reportes"):
            UserModuleAccess.objects.create(user=user,module=module,access="view" if module=="reportes" else "manage")
        AreaPresupuestoResponsable.objects.create(area=self.area,usuario=user)
        self.confirmar(user=user)
        self.assertEqual(documentos_visibles(user,"orden",self.orden.pk).count(),1)
        otra = AreaPresupuesto.objects.create(codigo="doc-private",nombre="Otra área")
        rubro = RubroPresupuesto.objects.create(area=otra,concepto="Privado",tipo="EGRESO")
        ObligacionGasto.objects.filter(pk=self.obligacion.pk).update(area=otra,rubro=rubro)
        self.assertFalse(fuentes_autorizadas(user,"gasto").filter(pk=self.gasto.pk).exists())
        self.assertEqual(documentos_visibles(user,"orden",self.orden.pk).count(),0)
        with self.assertRaises(PermissionDenied): self.confirmar(user=user)
        UserModuleAccess.objects.filter(user=user,module="mantenimiento").update(access="view")
        with self.assertRaises(PermissionDenied): self.confirmar(user=user,tipo_documento="cfdi",documento_id=self.cfdi.pk)
        user.groups.add(Group.objects.get_or_create(name="mantenimiento")[0])
        UserModuleAccess.objects.filter(user=user,module="mantenimiento").update(access="none")
        self.client.force_login(user); self.assertEqual(self.client.get(self.url).status_code,403)
        get_user_model().objects.filter(pk=self.actor.pk).update(is_active=False)
        with self.assertRaises(PermissionDenied): self.confirmar()

    def test_fuentes_independientes_gates_sat_banco_sin_inferencia_area(self):
        user = get_user_model().objects.create_user("doc-view")
        UserModuleAccess.objects.create(user=user,module="mantenimiento",access="manage")
        UserModuleAccess.objects.create(user=user,module="reportes",access="view")
        libre = GastoOperativoMensual.objects.create(periodo=date(2026,10,1),centro_costo=self.centro,categoria_gasto=self.categoria,monto=42)
        self.assertTrue(fuentes_autorizadas(user,"gasto").filter(pk=libre.pk).exists())
        with self.assertRaises(PermissionDenied): self.confirmar(user=user,tipo_documento="gasto",documento_id=libre.pk)
        for tipo, fuente in (("cfdi",self.cfdi),("movimiento",self.movimiento)):
            self.assertFalse(fuentes_autorizadas(user,tipo).exists())
            with self.assertRaises(PermissionDenied): self.confirmar(user=user,tipo_documento=tipo,documento_id=fuente.pk)
        UserModuleAccess.objects.create(user=user,module="conciliacion.fiscal",access="view")
        self.confirmar(user=user,tipo_documento="cfdi",documento_id=self.cfdi.pk)
        UserModuleAccess.objects.filter(user=user,module="conciliacion.fiscal").update(access="none")
        self.assertEqual(documentos_visibles(user,"orden",self.orden.pk).count(),0)

    def test_audit_rollback_tombstone_pk_reused_and_immutable(self):
        with patch("mantenimiento.services_documentos_financieros.AuditLog.objects.create",side_effect=RuntimeError):
            with self.assertRaises(RuntimeError): self.confirmar()
        self.assertEqual(DocumentoFinancieroTrabajo.objects.count(),0)
        v, _ = self.confirmar(tipo_documento="cfdi",documento_id=self.cfdi.pk)
        v.motivo="Otro"
        with self.assertRaises(ValidationError): v.save()
        pk = self.cfdi.pk; self.cfdi.delete(); v.refresh_from_db()
        self.assertIsNone(v.cfdi_id); self.assertEqual(v.cfdi_original_id,pk)
        nueva = CfdiDescargado.objects.create(pk=pk,uuid="nuevo-pk",rfc_emisor="A",rfc_receptor="B",subtotal=1,total=1,tipo_comprobante="I",tipo_cfdi="recibido",fecha_emision=timezone.now())
        with self.assertRaises(ValidationError) as exc: self.confirmar(tipo_trabajo="falla",trabajo_id=self.falla.pk,tipo_documento="cfdi",documento_id=nueva.pk)
        self.assertEqual(exc.exception.code,"conflict")
        self.assertEqual(documentos_visibles(self.actor,"orden",self.orden.pk).count(),1)

    def test_form_context_json_csrf_and_readonly_costs(self):
        self.client.force_login(self.actor)
        response=self.client.get(self.url)
        self.assertEqual(response.status_code,200)
        self.assertIn("no-store",response["Cache-Control"])
        data={**self.data,"confirmado":"on"}; data.pop("tipo_trabajo"); data.pop("trabajo_id")
        response=self.client.post(self.url,data,HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code,201)
        data["motivo"]="Conservar borrador"; response=self.client.post(self.url,data,HTTP_ACCEPT="application/json")
        self.assertEqual(response.status_code,409); self.assertIn("Conservar borrador",response.json()["html"])
        csrf=Client(enforce_csrf_checks=True); csrf.force_login(self.actor)
        self.assertIn(csrf.post(self.url,data).status_code,(302,403))
        user=get_user_model().objects.create_user("doc-cost-hidden")
        user.groups.add(Group.objects.get_or_create(name="mantenimiento")[0])
        UserModuleAccess.objects.create(user=user,module="reportes",access="view")
        self.client.force_login(user)
        response=self.client.get(self.url,{"tipo_documento":"obligacion","documento_id":self.obligacion.pk})
        self.assertContains(response,"Importe no disponible en tu alcance")
        self.assertNotContains(response,"125.00")

    def test_paginacion_scope_sucursal_y_tag_batch_anon_zero(self):
        self.client.force_login(self.actor)
        for i in range(24):
            GastoOperativoMensual.objects.create(periodo=date(2026,10,1),centro_costo=self.centro,categoria_gasto=self.categoria,monto=i)
        response=self.client.get(self.url,{"tipo_documento":"gasto"})
        self.assertEqual(len(response.context["opciones"]),20)
        user=get_user_model().objects.create_user("doc-branch")
        UserModuleAccess.objects.create(user=user,module="mantenimiento.app",access="view")
        UserModuleAccess.objects.create(user=user,module="reportes",access="view")
        UserProfile.objects.update_or_create(user=user,defaults={"sucursal":self.branch})
        otra=Sucursal.objects.create(codigo="DOCO",nombre="Otra")
        Activo.objects.filter(pk=self.asset.pk).update(sucursal=otra)
        self.client.force_login(user); self.assertEqual(self.client.get(self.url).status_code,404)
        tag=Template('{% load documentos_financieros %}{% for fila in filas %}{% consulta_documentos_trabajo request.user "orden" fila %}{% endfor %}')
        with self.assertNumQueries(0):
            self.assertEqual(tag.render(Context({"request":RequestFactory().get("/"),"filas":[self.orden.pk]})),"")
        def render(n):
            request=RequestFactory().get("/"); request.user=self.actor
            with CaptureQueriesContext(connection) as qs: html=tag.render(Context({"request":request,"filas":[self.orden.pk]*n}))
            return len(qs),html
        one,_=render(1); many,html=render(300)
        self.assertEqual(one,many); self.assertEqual(html.count(self.url),300)

    def test_constraint_originales_null_unicidad_y_trabajo_tombstone(self):
        v, _ = self.confirmar()
        for campos in ({"orden_original_id": None}, {"obligacion_original_id": None}, {"documento_original_id": self.obligacion.pk + 999}, {"gasto_original_id": self.gasto.pk + 999}):
            with transaction.atomic(), self.assertRaises(IntegrityError):
                DocumentoFinancieroTrabajo.objects.filter(pk=v.pk).update(**campos)
        pk = self.orden.pk
        self.orden.delete(); v.refresh_from_db()
        self.assertIsNone(v.orden_id); self.assertEqual(v.orden_original_id,pk)
        OrdenMantenimiento.objects.create(pk=pk,folio="DOC-REUSED",activo_ref=self.asset)
        with self.assertRaises(ValidationError) as exc:
            self.confirmar(tipo_documento="cfdi",documento_id=self.cfdi.pk)
        self.assertEqual(exc.exception.code,"conflict")

    def test_fresh_read_cuts_primed_permission_and_dual_alias_permission(self):
        user = get_user_model().objects.create_user("doc-fresh-read")
        for module in ("mantenimiento","reportes"):
            UserModuleAccess.objects.create(user=user,module=module,access="manage")
        self.confirmar(user=user)
        self.assertEqual(documentos_visibles(user,"orden",self.orden.pk).count(),1)
        UserModuleAccess.objects.filter(user=user,module="reportes").update(access="none")
        self.assertEqual(documentos_visibles(user,"orden",self.orden.pk).count(),0)
        self.client.force_login(user)
        self.assertEqual(self.client.get(self.url,{"documento_id":self.obligacion.pk}).status_code,403)
        self.assertEqual(self.client.get(reverse("mantenimiento:documentos-trabajo",args=["otro",self.orden.pk])).status_code,400)
        with self.assertRaises(PermissionDenied):
            self.confirmar(user=user,tipo_documento="gasto",documento_id=self.gasto.pk)

    def test_etapas_fuente_rep_banco_y_qr_protegido(self):
        from .services_documentos_financieros import detalle_fuente
        parcial = ParcialidadObligacionGasto.objects.create(obligacion=self.obligacion,numero=1,fecha_vencimiento=date(2026,10,10),monto=125)
        pago = PagoObligacionGasto.objects.create(obligacion=self.obligacion,parcialidad=parcial,fecha_pago=date(2026,10,5),monto=20,metodo_pago="TRANSFERENCIA",referencia="Etapa documentada")
        self.cfdi.tipo_comprobante="P"; self.cfdi.save(update_fields=["tipo_comprobante"])
        detalle = detalle_fuente("obligacion",self.obligacion,self.actor)
        self.assertEqual([e["id"] for e in detalle["etapas"]],[parcial.pk,pago.pk])
        self.assertIn("REP",detalle_fuente("cfdi",self.cfdi,self.actor)["naturaleza"])
        before=self.huellas(); self.confirmar(); self.assertEqual(before,self.huellas())
        self.client.force_login(self.actor)
        response=self.client.get(reverse("operacion:activo_pasaporte",args=[self.asset.qr_token]))
        self.assertContains(response,self.url)
        user=get_user_model().objects.create_user("doc-qr-scope")
        UserProfile.objects.update_or_create(user=user,defaults={"sucursal":self.branch})
        self.client.force_login(user)
        self.assertNotContains(self.client.get(reverse("operacion:activo_pasaporte",args=[self.asset.qr_token])),"Documentos financieros")

    def test_id_formulario_validado_antes_fuentes_get_post(self):
        self.client.force_login(self.actor)
        before = self.huellas()
        datos = {"tipo_documento": "obligacion", "motivo": "Borrador preservado", "evidencia": "Evidencia preservada", "confirmado": "on"}
        for value in ("9" * 5000, "-1", "0", str(2**63), "①", "²", "nan", "1e3"):
            with self.subTest(value=value[:20]):
                datos["documento_id"] = value
                with patch("mantenimiento.views_documentos_financieros.fuentes_autorizadas", side_effect=AssertionError("No consultar fuentes con un ID inválido")):
                    get = self.client.get(self.url, datos)
                    self.assertEqual(get.status_code, 400)
                    self.assertContains(get, datos["motivo"], status_code=400)
                    html = self.client.post(self.url, datos)
                    self.assertEqual(html.status_code, 400)
                    self.assertContains(html, datos["evidencia"], status_code=400)
                    post = self.client.post(self.url, datos, HTTP_ACCEPT="application/json")
                    self.assertEqual(post.status_code, 400)
                    self.assertFalse(post.json()["ok"])
                    self.assertIn(datos["motivo"], post.json()["html"])
        # Decimales Unicode válidos pasan por la misma normalización nativa.
        unicode_id = str(self.obligacion.pk).translate(str.maketrans("0123456789", "٠١٢٣٤٥٦٧٨٩"))
        self.assertEqual(self.client.get(self.url, {"documento_id": unicode_id}).status_code, 200)
        self.assertEqual(self.huellas(), before)
        self.assertEqual(DocumentoFinancieroTrabajo.objects.count(), 0)
        self.assertEqual(AuditLog.objects.filter(model="mantenimiento.DocumentoFinancieroTrabajo").count(), 0)

    def test_obligacion_no_disponible_al_bloquear_no_500_ni_escrituras(self):
        from django.db.models.query import QuerySet
        original_first = QuerySet.first
        def first(qs):
            if qs.model is ObligacionGasto and qs.query.select_for_no_key_update:
                return None
            return original_first(qs)
        before = self.huellas()
        with patch.object(QuerySet, "first", first):
            with self.assertRaises(PermissionDenied):
                self.confirmar()
        self.assertEqual(before, self.huellas())
        self.assertEqual(DocumentoFinancieroTrabajo.objects.count(), 0)

    def test_lectura_intersecta_todos_los_origenes_retenidos_espejo_desvinculado(self):
        self.confirmar()
        user = get_user_model().objects.create_user("doc-origen-retenido")
        UserModuleAccess.objects.create(user=user, module="mantenimiento", access="manage")
        AreaPresupuestoResponsable.objects.create(area=self.area, usuario=user)
        ObligacionGasto.objects.filter(pk=self.obligacion.pk).update(gasto_operativo=None)
        # La obligación es consultable, pero su gasto original aún retenido no.
        self.assertTrue(fuentes_autorizadas(user, "obligacion").filter(pk=self.obligacion.pk).exists())
        before = self.huellas()
        self.assertEqual(documentos_visibles(user, "orden", self.orden.pk).count(), 0)
        self.client.force_login(user)
        self.assertNotContains(self.client.get(self.url), self.data["evidencia"])
        self.assertEqual(before, self.huellas())
        self.assertEqual(documentos_visibles(self.actor, "orden", self.orden.pk).count(), 1)
        # Incluso con lectura general de Reportes, el gasto ahora ligado a una
        # obligación fuera del área hace invisible toda la confirmación original.
        UserModuleAccess.objects.create(user=user, module="reportes", access="view")
        area = AreaPresupuesto.objects.create(codigo="doc-ret-private", nombre="Origen retenido privado")
        rubro = RubroPresupuesto.objects.create(area=area, concepto="Fuente privada", tipo="EGRESO")
        otra = self.nueva_obligacion(gasto_operativo=self.gasto)
        ObligacionGasto.objects.filter(pk=otra.pk).update(area=area, rubro=rubro)
        self.assertFalse(fuentes_autorizadas(user, "gasto").filter(pk=self.gasto.pk).exists())
        self.assertEqual(documentos_visibles(user, "orden", self.orden.pk).count(), 0)
        self.assertNotContains(self.client.get(self.url), self.data["evidencia"])
        self.assertEqual(documentos_visibles(self.actor, "orden", self.orden.pk).count(), 1)


class DocumentosFinancierosConcurrencyTests(TransactionTestCase):
    setUp = DocumentosFinancierosTests.setUp
    nueva_obligacion = DocumentosFinancierosTests.nueva_obligacion
    confirmar = DocumentosFinancierosTests.confirmar

    def test_alias_concurrente_dos_actores_y_un_audit(self):
        barrier=Barrier(2)
        def run(alias):
            close_old_connections()
            try:
                barrier.wait(timeout=10)
                v,created=self.confirmar(user=self.actor if alias else self.otro,tipo_documento="gasto" if alias else "obligacion",documento_id=self.gasto.pk if alias else self.obligacion.pk)
                return v.pk,created,v.autor_original_id
            finally: close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool: resultados=list(pool.map(run,[True,False]))
        self.assertEqual(resultados[0][0],resultados[1][0]); self.assertEqual(resultados[0][2],resultados[1][2])
        self.assertEqual(sum(r[1] for r in resultados),1)
        self.assertEqual(AuditLog.objects.filter(model="mantenimiento.DocumentoFinancieroTrabajo").count(),1)

    def test_writer_cambia_espejo_mientras_confirmacion_espera_lock(self):
        from threading import Event
        from django.db.models.query import QuerySet
        locked, intentando = Event(), Event()
        otro_gasto = GastoOperativoMensual.objects.create(periodo=date(2026,10,1),centro_costo=self.centro,categoria_gasto=self.categoria,monto=44)
        original_first = QuerySet.first
        def first(qs, *args, **kwargs):
            if qs.model is ObligacionGasto and qs.query.select_for_no_key_update:
                intentando.set()
            return original_first(qs, *args, **kwargs)
        def writer():
            close_old_connections()
            try:
                with transaction.atomic():
                    ObligacionGasto.objects.select_for_update().filter(pk=self.obligacion.pk).first()
                    locked.set()
                    if not intentando.wait(timeout=10): raise RuntimeError("No se inició la confirmación")
                    ObligacionGasto.objects.filter(pk=self.obligacion.pk).update(gasto_operativo=otro_gasto)
            finally: close_old_connections()
        def confirm():
            close_old_connections()
            try:
                try: self.confirmar(tipo_documento="gasto",documento_id=self.gasto.pk)
                except ValidationError as exc: return exc.code
                return "created"
            finally: close_old_connections()
        with patch.object(QuerySet,"first",first), ThreadPoolExecutor(max_workers=2) as pool:
            correction=pool.submit(writer)
            self.assertTrue(locked.wait(timeout=10))
            pending=pool.submit(confirm)
            correction.result(timeout=20)
            self.assertEqual(pending.result(timeout=20),"conflict")
        self.assertEqual(DocumentoFinancieroTrabajo.objects.count(),0)
