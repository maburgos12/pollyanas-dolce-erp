from datetime import date
from importlib import import_module
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.test import TestCase, TransactionTestCase
from django.urls import reverse

from activos.models import Activo, OrdenMantenimiento
from core.models import AuditLog, UserModuleAccess
from reportes.models import AreaPresupuesto, AreaPresupuestoResponsable
from compras.models import ItemCompraDepartamental, SolicitudCompraDepartamental


class DestinosDocumentalesTests(TestCase):
    def setUp(self):
        self.actor = get_user_model().objects.create_superuser('destino', 'destino@example.test', 'test')
        self.otro = get_user_model().objects.create_superuser('otro', 'otro@example.test', 'test')
        self.area = AreaPresupuesto.objects.create(nombre='Destino prueba', codigo='destino-prueba')
        self.solicitud = SolicitudCompraDepartamental.objects.create(area=self.area, solicitante=self.actor, periodo=date(2026,10,1))
        self.item = ItemCompraDepartamental.objects.create(solicitud=self.solicitud, descripcion='Motor documental')
        self.activo = Activo.objects.create(codigo='DEST-1', nombre='Horno existente')
        self.orden = OrdenMantenimiento.objects.create(folio='DEST-OM1', activo_ref=self.activo)

    def service(self):
        return import_module('compras.services_destinos_documentales')

    def confirmar(self, **extra):
        return self.service().confirmar_destino(**dict(user=self.actor, item_id=self.item.pk, tipo='ACTIVO', destino_id=self.activo.pk, motivo='Destino indicado en solicitud', evidencia='Solicitud firmada folio D1', confirmado=True, **extra))

    def test_varios_destinos_reintento_y_huellas_intactas(self):
        before = list(ItemCompraDepartamental.objects.values()), list(Activo.objects.values()), list(OrdenMantenimiento.objects.values())
        vinculo, creado = self.confirmar()
        self.assertTrue(creado)
        original = (vinculo.autor_original_id, vinculo.creado_en, vinculo.motivo, vinculo.evidencia)
        retry, creado = self.service().confirmar_destino(user=self.otro, item_id=self.item.pk, tipo='ACTIVO', destino_id=self.activo.pk, motivo=vinculo.motivo, evidencia=vinculo.evidencia, confirmado=True)
        self.assertFalse(creado)
        self.assertEqual(retry.pk, vinculo.pk)
        self.assertEqual((retry.autor_original_id,retry.creado_en,retry.motivo,retry.evidencia), original)
        self.service().confirmar_destino(user=self.actor,item_id=self.item.pk,tipo='ORDEN',destino_id=self.orden.pk,motivo='Trabajo solicitado',evidencia='Orden folio OM1',confirmado=True)
        self.assertEqual(self.item.destinos_documentales.count(),2)
        self.assertEqual(AuditLog.objects.filter(model='compras.DestinoCompraDocumental').count(),2)
        self.assertEqual(before,(list(ItemCompraDepartamental.objects.values()),list(Activo.objects.values()),list(OrdenMantenimiento.objects.values())))

    def test_conflicto_y_rollback_auditoria(self):
        self.confirmar()
        with self.assertRaises(ValidationError) as error:
            self.service().confirmar_destino(user=self.actor,item_id=self.item.pk,tipo='ACTIVO',destino_id=self.activo.pk,motivo='Otro motivo',evidencia='Otra evidencia',confirmado=True)
        self.assertEqual(error.exception.code,'conflict')
        with patch('compras.services_destinos_documentales.AuditLog.objects.create', side_effect=RuntimeError('auditoria')):
            with self.assertRaises(RuntimeError):
                self.service().confirmar_destino(user=self.actor,item_id=self.item.pk,tipo='ORDEN',destino_id=self.orden.pk,motivo='Trabajo',evidencia='Documento',confirmado=True)
        self.assertEqual(self.item.destinos_documentales.count(),1)

    def test_actor_fresco_y_ambos_permisos(self):
        self.service()
        actor = get_user_model().objects.create_user('responsable')
        AreaPresupuestoResponsable.objects.create(area=self.area,usuario=actor)
        with self.assertRaises(PermissionDenied):
            self.service().confirmar_destino(user=actor,item_id=self.item.pk,tipo='ACTIVO',destino_id=self.activo.pk,motivo='A',evidencia='B',confirmado=True)
        UserModuleAccess.objects.create(user=actor,module='inventario',access='manage')
        self.service().confirmar_destino(user=actor,item_id=self.item.pk,tipo='ACTIVO',destino_id=self.activo.pk,motivo='A',evidencia='B',confirmado=True)
        UserModuleAccess.objects.filter(user=actor).update(access='view')
        with self.assertRaises(PermissionDenied):
            self.service().confirmar_destino(user=actor,item_id=self.item.pk,tipo='ACTIVO',destino_id=self.activo.pk,motivo='A',evidencia='B',confirmado=True)
        get_user_model().objects.filter(pk=self.actor.pk).update(is_active=False)
        with self.assertRaises(PermissionDenied):
            self.confirmar()

    def test_validacion_confirmacion_tipo_y_campos(self):
        self.service()
        for fields in ({'confirmado':False},{'motivo':' '},{'evidencia':''},{'tipo':'COMPRA'}):
            params=dict(user=self.actor,item_id=self.item.pk,tipo='ACTIVO',destino_id=self.activo.pk,motivo='A',evidencia='B',confirmado=True)
            params.update(fields)
            with self.assertRaises(ValidationError):
                self.service().confirmar_destino(**params)
        self.assertEqual(AuditLog.objects.filter(model='compras.DestinoCompraDocumental').count(),0)

    def test_procedencia_sobrevive_borrados_y_no_se_restaura(self):
        vinculo,_ = self.confirmar()
        ids = self.item.pk, self.activo.pk, self.otro.pk
        vinculo2,_ = self.service().confirmar_destino(user=self.otro,item_id=self.item.pk,tipo='ORDEN',destino_id=self.orden.pk,motivo='A',evidencia='B',confirmado=True)
        self.otro.delete()
        self.orden.delete()
        self.activo.delete()
        self.item.delete()
        vinculo.refresh_from_db(); vinculo2.refresh_from_db()
        self.assertIsNone(vinculo.item_id); self.assertIsNone(vinculo.activo_id)
        self.assertEqual((vinculo.item_original_id,vinculo.destino_original_id),ids[:2])
        self.assertIsNone(vinculo2.orden_id); self.assertIsNone(vinculo2.autor_id)
        self.assertEqual(vinculo2.autor_original_id,ids[2])

    def test_get_csrf_y_json_preservan_formulario(self):
        self.service()
        self.client.force_login(self.actor)
        url=reverse('compras:destinos_documentales',args=[self.item.pk])
        self.assertEqual(self.client.get(url).status_code,200)
        self.assertEqual(self.item.destinos_documentales.count(),0)
        from django.test import Client
        csrf=Client(enforce_csrf_checks=True); csrf.force_login(self.actor)
        rejection=csrf.post(url,{},HTTP_ACCEPT='application/json')
        self.assertRedirects(rejection,reverse('login'),fetch_redirect_response=False)
        self.assertEqual(self.item.destinos_documentales.count(),0)
        data={'tipo':'ACTIVO','destino_id':self.activo.pk,'motivo':'A','evidencia':'B','confirmado':'on'}
        response=self.client.post(url,data,HTTP_ACCEPT='application/json')
        self.assertEqual(response.status_code,201)
        self.assertEqual(response.json()['target'],'#destinos-documentales')
        data['motivo']='C'
        response=self.client.post(url,data,HTTP_ACCEPT='application/json')
        self.assertEqual(response.status_code,409)
        self.assertIn('C',response.json()['html'])

    def test_seleccion_preservada_y_sin_fugas_fuera_de_area(self):
        self.service()
        self.client.force_login(self.actor)
        url=reverse('compras:destinos_documentales',args=[self.item.pk])
        data={'destino':f'ACTIVO:{self.activo.pk}','motivo':'A','evidencia':'B','confirmado':'on'}
        response=self.client.post(url,data,HTTP_ACCEPT='application/json')
        self.assertIn(f'value="ACTIVO:{self.activo.pk}" selected',response.json()['html'])
        limited=get_user_model().objects.create_user('sin-compra')
        UserModuleAccess.objects.create(user=limited,module='inventario',access='manage')
        self.client.force_login(limited)
        response=self.client.get(url)
        self.assertEqual(response.status_code,403)
        self.assertNotContains(response,'Motor documental',status_code=403)
        response=self.client.get(reverse('compras:compras_del_destino',args=['ACTIVO',self.activo.pk]))
        self.assertEqual(response.status_code,403)
        self.assertNotContains(response,'Solicitud firmada',status_code=403)

    def test_mantenimiento_ambito_y_revocacion_legacy(self):
        from django.contrib.auth.models import Group
        from core.models import Sucursal, UserProfile
        self.service()
        branch=Sucursal.objects.create(codigo='DEST-A',nombre='Sucursal propia')
        otra=Sucursal.objects.create(codigo='DEST-B',nombre='Sucursal ajena')
        self.activo.sucursal=branch; self.activo.save(update_fields=['sucursal'])
        ajeno=Activo.objects.create(codigo='DEST-AJENO',nombre='Equipo ajeno',sucursal=otra)
        orden_ajena=OrdenMantenimiento.objects.create(folio='DEST-OM-AJENA',activo_ref=ajeno)
        actor=get_user_model().objects.create_user('tecnico-documental')
        UserProfile.objects.update_or_create(user=actor,defaults={'sucursal':branch})
        AreaPresupuestoResponsable.objects.create(area=self.area,usuario=actor)
        UserModuleAccess.objects.create(user=actor,module='mantenimiento.app',access='manage')
        params=dict(user=actor,item_id=self.item.pk,tipo='ORDEN',motivo='A',evidencia='B',confirmado=True)
        self.service().confirmar_destino(**params,destino_id=self.orden.pk)
        with self.assertRaises(PermissionDenied):
            self.service().confirmar_destino(**params,destino_id=orden_ajena.pk)
        actor.groups.add(Group.objects.get_or_create(name='mantenimiento')[0])
        UserModuleAccess.objects.create(user=actor,module='mantenimiento',access='none')
        with self.assertRaises(PermissionDenied):
            self.service().confirmar_destino(**params,destino_id=self.orden.pk)
        self.assertEqual(self.service().vinculos_visibles(self.service().actor_actual(actor),item=self.item),[])
        # Inventario es una capacidad independiente de Mantenimiento.
        UserModuleAccess.objects.create(user=actor,module='inventario',access='manage')
        self.service().confirmar_destino(**params,destino_id=orden_ajena.pk)

    def test_area_inactiva_y_lectura_no_autorizan_confirmar(self):
        self.service()
        actor=get_user_model().objects.create_user('area-documental')
        AreaPresupuestoResponsable.objects.create(area=self.area,usuario=actor)
        UserModuleAccess.objects.create(user=actor,module='inventario',access='manage')
        self.area.activa=False; self.area.save(update_fields=['activa'])
        with self.assertRaises(PermissionDenied):
            self.service().confirmar_destino(user=actor,item_id=self.item.pk,tipo='ACTIVO',destino_id=self.activo.pk,motivo='A',evidencia='B',confirmado=True)

    def test_varios_articulos_por_destino_y_no_restaurar_ids(self):
        self.service()
        vinculo,_=self.confirmar()
        other=ItemCompraDepartamental.objects.create(solicitud=self.solicitud,descripcion='Otro artículo')
        self.service().confirmar_destino(user=self.actor,item_id=other.pk,tipo='ACTIVO',destino_id=self.activo.pk,motivo='A',evidencia='B',confirmado=True)
        self.assertEqual(self.activo.compras_documentales.count(),2)
        original=self.item.pk
        self.item.delete()
        # Reutilizar una PK eliminada no acredita que sea la fuente original.
        ItemCompraDepartamental.objects.create(pk=original,solicitud=self.solicitud,descripcion='Otra fuente')
        with self.assertRaises(ValidationError) as error:
            self.service().confirmar_destino(user=self.actor,item_id=original,tipo='ACTIVO',destino_id=self.activo.pk,motivo=vinculo.motivo,evidencia=vinculo.evidencia,confirmado=True)
        self.assertEqual(error.exception.code,'conflict')
        vinculo.refresh_from_db(); self.assertIsNone(vinculo.item_id)

    def test_constraint_tipado_y_ids_originales(self):
        from django.db import IntegrityError, transaction
        self.service()
        vinculo,_=self.confirmar()
        with transaction.atomic(), self.assertRaises(IntegrityError):
            type(vinculo).objects.filter(pk=vinculo.pk).update(tipo='ORDEN')
        with transaction.atomic(), self.assertRaises(IntegrityError):
            type(vinculo).objects.filter(pk=vinculo.pk).update(item_original_id=self.item.pk+1)

    def test_huellas_flujo_compra_entrega_y_reembolso_intactas(self):
        from compras import models as cm
        from maestros.models import Proveedor
        self.item.estado='ORDENADO'; self.item.save(update_fields=['estado'])
        proveedor=Proveedor.objects.create(nombre='Proveedor documental')
        quote=cm.CotizacionCompraDepartamental.objects.create(item=self.item,proveedor=proveedor,cantidad_ofertada=2,costo_unitario=125)
        intento=cm.IntentoCompraDepartamental.objects.create(item=self.item,cotizacion=quote,reembolso_solicitado=50)
        oc=cm.OrdenCompraDepartamental.objects.create(folio='OC-DOC1',proveedor=proveedor,creado_por=self.actor)
        linea=cm.LineaOrdenCompraDepartamental.objects.create(orden=oc,item=self.item,intento=intento,cotizacion=quote,cantidad=2,costo_unitario=125,total=250)
        cm.RecepcionItemDepartamental.objects.create(linea_orden=linea,cantidad_recibida=1,registrado_por=self.actor)
        cm.ReembolsoCompraDepartamental.objects.create(intento=intento,importe=25,fecha=date(2026,10,1),registrado_por=self.actor)
        models=[cm.ItemCompraDepartamental,cm.IntentoCompraDepartamental,cm.LineaOrdenCompraDepartamental,cm.RecepcionItemDepartamental,cm.CotizacionCompraDepartamental,cm.ReembolsoCompraDepartamental,cm.OrdenCompraDepartamental,Proveedor,Activo,OrdenMantenimiento]
        before={m.__name__:list(m.objects.order_by('pk').values()) for m in models}
        self.confirmar()
        self.assertEqual(before,{m.__name__:list(m.objects.order_by('pk').values()) for m in models})


    def test_revocacion_parcial_no_revive_por_grupo_y_view_no_escribe(self):
        from django.contrib.auth.models import Group
        from core.models import Sucursal, UserProfile
        actor=get_user_model().objects.create_user('revocado-parcial')
        actor.groups.add(Group.objects.get_or_create(name='mantenimiento')[0])
        branch=Sucursal.objects.create(codigo='DEST-REV',nombre='Destino revocación')
        UserProfile.objects.update_or_create(user=actor,defaults={'sucursal':branch})
        self.activo.sucursal=branch; self.activo.save(update_fields=['sucursal'])
        AreaPresupuestoResponsable.objects.create(area=self.area,usuario=actor)
        permission=UserModuleAccess.objects.create(user=actor,module='mantenimiento.app',access='none')
        for access in ('none','view'):
            permission.access=access;permission.save(update_fields=['access'])
            with self.assertRaises(PermissionDenied):
                self.service().confirmar_destino(user=actor,item_id=self.item.pk,tipo='ORDEN',destino_id=self.orden.pk,motivo='A',evidencia='B',confirmado=True)
        permission.access='manage';permission.save(update_fields=['access'])
        self.service().confirmar_destino(user=actor,item_id=self.item.pk,tipo='ORDEN',destino_id=self.orden.pk,motivo='A',evidencia='B',confirmado=True)

    def test_lock_inventario_actual_rechaza_actor_cacheado(self):
        from core.models import UserProfile
        actor=get_user_model().objects.create_user('locked-documental')
        AreaPresupuestoResponsable.objects.create(area=self.area,usuario=actor)
        UserModuleAccess.objects.create(user=actor,module='inventario',access='manage')
        profile,_=UserProfile.objects.update_or_create(user=actor,defaults={'lock_inventario':False})
        self.service().destinos_autorizados(actor,'ACTIVO',escritura=True).exists()
        UserProfile.objects.filter(pk=profile.pk).update(lock_inventario=True)
        with self.assertRaises(PermissionDenied):
            self.service().confirmar_destino(user=actor,item_id=self.item.pk,tipo='ACTIVO',destino_id=self.activo.pk,motivo='A',evidencia='B',confirmado=True)


    def test_lectura_conjunta_filtra_articulos_ajenos_y_enlaces(self):
        from django.template import Context, Template
        vinculo,_=self.confirmar()
        other_area=AreaPresupuesto.objects.create(nombre='Area ajena documental',codigo='area-ajena-documental')
        other_request=SolicitudCompraDepartamental.objects.create(area=other_area,solicitante=self.actor,periodo=date(2026,10,1))
        other_item=ItemCompraDepartamental.objects.create(solicitud=other_request,descripcion='Articulo privado ajeno')
        self.service().confirmar_destino(user=self.actor,item_id=other_item.pk,tipo='ACTIVO',destino_id=self.activo.pk,motivo='Motivo privado',evidencia='Evidencia privada',confirmado=True)
        actor=get_user_model().objects.create_user('lector-destino')
        AreaPresupuestoResponsable.objects.create(area=self.area,usuario=actor)
        UserModuleAccess.objects.create(user=actor,module='inventario',access='view')
        self.client.force_login(actor)
        url=reverse('compras:compras_del_destino',args=['ACTIVO',self.activo.pk])
        response=self.client.get(url)
        self.assertEqual(response.status_code,200)
        self.assertContains(response,self.item.descripcion)
        self.assertNotContains(response,other_item.descripcion)
        self.assertNotContains(response,'Evidencia privada')
        item_url=reverse('compras:destinos_documentales',args=[self.item.pk])
        response=self.client.get(item_url)
        self.assertNotContains(response,'data-async-action>')
        forbidden=self.client.post(item_url,{'destino':f'ACTIVO:{self.activo.pk}','motivo':'A','evidencia':'B','confirmado':'on'})
        self.assertEqual(forbidden.status_code,403)
        tag=Template("{% load destinos_documentales %}{% consulta_compras_destino user 'ACTIVO' pk %}")
        self.assertEqual(tag.render(Context({'user':actor,'pk':self.activo.pk})),url)
        AreaPresupuestoResponsable.objects.filter(usuario=actor).delete()
        self.assertEqual(tag.render(Context({'user':actor,'pk':self.activo.pk})), '')

    def test_lock_compras_de_gestor_actual_y_revocacion_de_area(self):
        from core.models import UserProfile
        actor=get_user_model().objects.create_user('gestor-compras-lock')
        UserModuleAccess.objects.create(user=actor,module='compras',access='manage')
        UserModuleAccess.objects.create(user=actor,module='inventario',access='manage')
        UserProfile.objects.update_or_create(user=actor,defaults={'lock_compras':True})
        with self.assertRaises(PermissionDenied):
            self.service().confirmar_destino(user=actor,item_id=self.item.pk,tipo='ACTIVO',destino_id=self.activo.pk,motivo='A',evidencia='B',confirmado=True)
        UserModuleAccess.objects.filter(user=actor,module='compras').update(access='none')
        responsable=AreaPresupuestoResponsable.objects.create(area=self.area,usuario=actor)
        self.service().confirmar_destino(user=actor,item_id=self.item.pk,tipo='ACTIVO',destino_id=self.activo.pk,motivo='A',evidencia='B',confirmado=True)
        responsable.puede_capturar=False;responsable.save(update_fields=['puede_capturar'])
        with self.assertRaises(PermissionDenied):
            self.service().confirmar_destino(user=actor,item_id=self.item.pk,tipo='ACTIVO',destino_id=self.activo.pk,motivo='A',evidencia='B',confirmado=True)

    def test_entradas_contextuales_compra_y_expediente_trabajo(self):
        self.confirmar()
        self.service().confirmar_destino(user=self.actor,item_id=self.item.pk,tipo='ORDEN',destino_id=self.orden.pk,motivo='A',evidencia='B',confirmado=True)
        self.client.force_login(self.actor)
        response=self.client.get(reverse('compras:departamental_detalle',args=[self.solicitud.pk]))
        self.assertContains(response,reverse('compras:destinos_documentales',args=[self.item.pk]))
        response=self.client.get(reverse('activos:orden_evidencias',args=[self.orden.pk]))
        self.assertContains(response,reverse('compras:compras_del_destino',args=['ORDEN',self.orden.pk]))
        response=self.client.get(reverse('activos:activos'))
        self.assertContains(response,reverse('compras:compras_del_destino',args=['ACTIVO',self.activo.pk]))


    def test_pasaporte_qr_muestra_enlace_solo_con_permisos_conjuntos(self):
        from core.models import Sucursal, UserProfile
        branch=Sucursal.objects.create(codigo='DEST-QR',nombre='Sucursal QR')
        self.activo.sucursal=branch;self.activo.save(update_fields=['sucursal'])
        self.confirmar()
        qr_user=get_user_model().objects.create_user('qr-puro')
        UserProfile.objects.update_or_create(user=qr_user,defaults={'sucursal':branch})
        self.client.force_login(qr_user)
        url=reverse('operacion:activo_pasaporte',args=[self.activo.qr_token])
        response=self.client.get(url)
        self.assertEqual(response.status_code,200)
        self.assertNotContains(response,'Compras relacionadas')
        self.assertNotContains(response,'Destino indicado en solicitud')
        # Lectura QR no basta: es necesaria la lectura en cada fuente.
        UserModuleAccess.objects.create(user=qr_user,module='inventario',access='view')
        AreaPresupuestoResponsable.objects.create(area=self.area,usuario=qr_user)
        response=self.client.get(url)
        self.assertContains(response,reverse('compras:compras_del_destino',args=['ACTIVO',self.activo.pk]))


    def test_tags_for_with_no_multiplican_consultas_por_fila(self):
        from django.db import connection
        from django.template import Context, Template
        from django.test import RequestFactory
        from django.test.utils import CaptureQueriesContext
        template=Template("{% load destinos_documentales %}{% for pk in filas %}{% with fila=pk %}{% consulta_compras_destino user 'ACTIVO' fila %}{% puede_consultar_destinos user %}{% endwith %}{% endfor %}")
        def render(count):
            request=RequestFactory().get('/activos/activos/')
            with CaptureQueriesContext(connection) as queries:
                html=template.render(Context({'request':request,'user':self.actor,'filas':[self.activo.pk]*count}))
            return len(queries),html
        for with_link in (False,True):
            if with_link:
                self.confirmar()
            single,_=render(1)
            multiple,html=render(300)
            self.assertEqual(single,multiple)
            self.assertLessEqual(multiple,8)
            expected=reverse('compras:compras_del_destino',args=['ACTIVO',self.activo.pk])
            self.assertEqual(html.count(expected),300 if with_link else 0)

    def test_tags_cache_solo_peticion_y_actor_preview_actual(self):
        from django.template import Context, Template
        from django.test import RequestFactory
        self.confirmar()
        actor=get_user_model().objects.create_user('cache-documental')
        responsable=AreaPresupuestoResponsable.objects.create(area=self.area,usuario=actor)
        UserModuleAccess.objects.create(user=actor,module='inventario',access='view')
        tag=Template("{% load destinos_documentales %}{% consulta_compras_destino user 'ACTIVO' pk %}")
        def render(request,user):
            return tag.render(Context({'request':request,'user':user,'pk':self.activo.pk}))
        request=RequestFactory().get('/activos/activos/')
        url=reverse('compras:compras_del_destino',args=['ACTIVO',self.activo.pk])
        self.assertEqual(render(request,actor),url)
        preview=get_user_model().objects.create_user('preview-sin-compra')
        UserModuleAccess.objects.create(user=preview,module='inventario',access='view')
        self.assertEqual(render(request,preview),'')
        responsable.puede_capturar=False;responsable.save(update_fields=['puede_capturar'])
        self.assertEqual(render(RequestFactory().get('/activos/activos/'),actor),'')


    def test_lectura_batch_no_superuser_300_vinculos_y_pasaporte(self):
        from django.db import connection
        from django.template import Context, Template
        from django.test import RequestFactory
        from django.test.utils import CaptureQueriesContext
        from core.models import Sucursal, UserProfile
        from compras.models import DestinoCompraDocumental
        branch=Sucursal.objects.create(codigo='DEST-BATCH',nombre='Sucursal batch')
        self.activo.sucursal=branch; self.activo.save(update_fields=['sucursal'])
        self.confirmar()
        actor=get_user_model().objects.create_user('batch-responsable')
        responsable=AreaPresupuestoResponsable.objects.create(area=self.area,usuario=actor)
        UserModuleAccess.objects.create(user=actor,module='inventario',access='view')
        UserProfile.objects.update_or_create(user=actor,defaults={'sucursal':branch})
        self.area.activa=False; self.area.save(update_fields=['activa'])
        other_area=AreaPresupuesto.objects.create(nombre='Batch ajena',codigo='batch-ajena')
        other_request=SolicitudCompraDepartamental.objects.create(area=other_area,solicitante=self.actor,periodo=date(2026,10,1))
        tag=Template("{% load destinos_documentales %}{% for pk in filas %}{% with fila=pk %}{% consulta_compras_destino user 'ACTIVO' fila %}{% endwith %}{% endfor %}")
        def render():
            with CaptureQueriesContext(connection) as queries:
                html=tag.render(Context({'request':RequestFactory().get('/activos/activos/'),'user':actor,'filas':[self.activo.pk]*300}))
            return len(queries),html
        self.client.force_login(actor)
        qr_url=reverse('operacion:activo_pasaporte',args=[self.activo.qr_token])
        single,_=render()
        with CaptureQueriesContext(connection) as queries:
            self.client.get(qr_url)
        qr_single=len(queries)
        items=ItemCompraDepartamental.objects.bulk_create([
            ItemCompraDepartamental(solicitud=self.solicitud if i%2 else other_request,descripcion=f'Batch {i}') for i in range(299)
        ])
        DestinoCompraDocumental.objects.bulk_create([
            DestinoCompraDocumental(item=item,item_original_id=item.pk,tipo='ACTIVO',activo=self.activo,destino_original_id=self.activo.pk,autor=self.actor,autor_original_id=self.actor.pk,motivo='Batch visible' if item.solicitud_id==self.solicitud.pk else 'Batch privado',evidencia='Documento') for item in items
        ])
        multiple,html=render()
        self.assertEqual(single,multiple)
        self.assertLessEqual(multiple,12)
        with CaptureQueriesContext(connection) as queries:
            qr_response=self.client.get(qr_url)
        self.assertEqual(qr_single,len(queries))
        url=reverse('compras:compras_del_destino',args=['ACTIVO',self.activo.pk])
        self.assertEqual(html.count(url),300)
        self.assertContains(qr_response,url)
        visible=self.service().vinculos_visibles(self.service().actor_actual(actor))
        self.assertTrue(visible)
        self.assertTrue(all(v.item.solicitud_id==self.solicitud.pk for v in visible))
        self.assertTrue(all(v.motivo!='Batch privado' for v in visible))
        responsable.puede_capturar=False;responsable.save(update_fields=['puede_capturar'])
        _,html=render();self.assertNotIn(url,html)
        self.assertNotContains(self.client.get(qr_url),url)


class DestinosConcurrentesTests(TransactionTestCase):
    def test_mismo_par_concurrente_entre_actores(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from django.db import close_old_connections
        from compras.services_destinos_documentales import confirmar_destino
        from compras.models import DestinoCompraDocumental
        actors=[get_user_model().objects.create_superuser(f'concurrente-{i}',f'c{i}@example.test','test') for i in range(2)]
        area=AreaPresupuesto.objects.create(nombre='Concurrencia',codigo='concurrencia')
        solicitud=SolicitudCompraDepartamental.objects.create(area=area,solicitante=actors[0],periodo=date(2026,10,1))
        item=ItemCompraDepartamental.objects.create(solicitud=solicitud,descripcion='Concurrente')
        activo=Activo.objects.create(codigo='DEST-CONC',nombre='Destino concurrente')
        barrier=Barrier(2)
        def confirm(user):
            close_old_connections()
            try:
                barrier.wait(timeout=10)
                relation,created=confirmar_destino(user=user,item_id=item.pk,tipo='ACTIVO',destino_id=activo.pk,motivo='A',evidencia='B',confirmado=True)
                return relation.pk,created,relation.autor_original_id
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(confirm,actors))
        self.assertEqual(results[0][0],results[1][0])
        self.assertEqual(results[0][2],results[1][2])
        self.assertEqual(sum(created for _,created,_ in results),1)
        self.assertEqual(DestinoCompraDocumental.objects.count(),1)
        self.assertEqual(AuditLog.objects.filter(model='compras.DestinoCompraDocumental').count(),1)
