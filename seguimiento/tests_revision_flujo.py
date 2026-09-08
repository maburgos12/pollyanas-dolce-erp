"""Entrega, sincronización y decisión DG forman un solo flujo auditable."""
import os
from datetime import datetime, timezone as dt_timezone
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

from core.audit import log_event
from core.models import AuditLog
from core.notificaciones import _cuerpo_correo_seguimiento
from seguimiento.models import SeguimientoItem, SeguimientoChecklistItem
from seguimiento.services import upsert_agente_dg_payload


@override_settings(SECURE_SSL_REDIRECT=False, EMAIL_BACKEND='django.core.mail.backends.locmem.EmailBackend')
class RevisionFlujoTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user('colaborador.revision', email='revision@example.com')
        self.dg = get_user_model().objects.create_superuser('dg.revision', email='dg@example.com', password='local-test')
        self.item = SeguimientoItem.objects.create(tipo='MINUTA', titulo='Revisión de prueba', responsable_user=self.user,
            metadata={'source': 'agente_dg', 'source_table': 'minute_agreements', 'source_id': 86, 'source_status': 'OPEN'})
        self.check = SeguimientoChecklistItem.objects.create(seguimiento=self.item, titulo='Punto real', completado=True)
        self.client.force_login(self.user)
        self.addCleanup(patch.stopall)
        patch('seguimiento.views.notificar_seguimiento_entrega').start()
        patch('seguimiento.views.notificar_seguimiento_aprobado').start()
        patch('seguimiento.views.notificar_seguimiento_devuelto').start()
        patch('seguimiento.views.notificar_seguimiento_avance').start()
        patch.dict(os.environ, {'AGENTE_DG_WRITEBACK_ENABLED': 'false'}).start()

    def post(self, action, data=None):
        return self.client.post(f'/seguimiento/{self.item.pk}/{action}/', data or {}, HTTP_ACCEPT='application/json')

    def sync(self, status='OPEN'):
        upsert_agente_dg_payload({'source_table': 'minute_agreements', 'source_id':86,
            'record': {'titulo':self.item.titulo, 'status':status, 'user_email': self.user.email}})
        self.item.refresh_from_db()

    def test_entrega_sobrevive_sync_y_aparece_en_historial(self):
        response = self.post('entregar')
        self.assertEqual(response.status_code, 200)
        self.sync()
        self.assertEqual(self.item.estatus, 'EN_REVISION')
        self.client.force_login(self.dg)
        page = self.client.get(f'/seguimiento/panel/{self.item.pk}/')
        self.assertContains(page, 'Entregó para revisión')
        self.assertContains(page, 'Devolver para corrección')
        self.assertNotContains(page, 'Sin actividad registrada todavía.')

    def test_no_entrega_con_puntos_pendientes_ni_crea_evidencia(self):
        self.check.completado=False
        self.check.save()
        response=self.post('entregar', {'comentario':'Resultado parcial'})
        self.assertEqual(response.status_code, 409)
        self.assertFalse(self.item.comentarios.exists())
        self.assertFalse(AuditLog.objects.filter(action='seguimiento.entrega').exists())

    def test_doble_entrega_no_duplica_auditoria(self):
        self.post('entregar')
        self.post('entregar')
        self.assertEqual(AuditLog.objects.filter(action='seguimiento.entrega').count(),1)

    def test_checklist_completo_no_salta_aprobacion(self):
        self.post('completar')
        self.item.refresh_from_db()
        self.assertEqual(self.item.estatus,'PENDIENTE')
        page=self.client.get(f'/seguimiento/{self.item.pk}/')
        self.assertContains(page,'Enviar a revisión')
        self.assertNotContains(page,f'action="/seguimiento/{self.item.pk}/completar/"')

    def test_dg_no_resuelve_si_no_esta_en_revision(self):
        self.client.force_login(self.dg)
        response=self.post('resolver', {'accion':'aprobar','next':'detalle'})
        self.assertEqual(response.status_code,409)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estatus,'PENDIENTE')

    def test_devolucion_exige_motivo_y_conserva_revision(self):
        self.post('entregar')
        self.client.force_login(self.dg)
        response=self.post('resolver', {'accion':'devolver','next':'detalle'})
        self.assertEqual(response.status_code,400)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estatus,'EN_REVISION')

    def test_devolver_reentregar_y_retractar_nueva_entrega(self):
        self.post('entregar')
        self.client.force_login(self.dg)
        self.post('resolver', {'accion':'devolver','comentario':'Aclarar resultado'})
        self.client.force_login(self.user)
        self.post('entregar')
        response=self.post('retractar')
        self.assertEqual(response.status_code,200)
        self.sync()
        self.assertNotEqual(self.item.estatus,'EN_REVISION')

    def test_avance_no_es_entrega(self):
        self.post('retroalimentacion', {'comentario':'Todavía trabajando'})
        self.item.refresh_from_db()
        self.assertEqual(self.item.estatus,'EN_PROCESO')

    def test_checklist_no_cambia_durante_revision(self):
        self.post('entregar')
        self.client.post(f'/seguimiento/{self.item.pk}/checklist/{self.check.pk}/')
        self.check.refresh_from_db()
        self.assertTrue(self.check.completado)

    def test_tercero_no_entrega(self):
        other=get_user_model().objects.create_user('ajeno.revision')
        self.client.force_login(other)
        self.assertEqual(self.post('entregar').status_code,404)

    def test_fuente_cancelada_no_resucita_revision(self):
        self.post('entregar')
        self.sync('CANCELLED')
        self.assertEqual(self.item.estatus,'CANCELADO')

    def test_correo_usa_hora_local_explicita(self):
        self.item.fecha_limite=datetime(2026,7,3,21,tzinfo=dt_timezone.utc)
        with timezone.override('America/Mazatlan'):
            body=_cuerpo_correo_seguimiento(self.item, encabezado='Entrega',actor=self.user)
        self.assertIn('03/07/2026 14:00',body)
        self.assertIn('America/Mazatlan',body)

    def test_auditoria_historica_visible_sin_comentarios(self):
        log_event(self.user,'seguimiento.entrega','SeguimientoItem',self.item.pk,{'entrega':True})
        self.client.force_login(self.dg)
        page=self.client.get(f'/seguimiento/panel/{self.item.pk}/')
        self.assertContains(page,'Entregó para revisión')

    def test_revision_erp_no_se_oculta_si_proyecto_origen_termino(self):
        self.post('entregar')
        self.sync('COMPLETED')
        self.client.force_login(self.dg)
        page=self.client.get('/seguimiento/panel/?estado=revision')
        self.assertContains(page,self.item.titulo)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estatus,'EN_REVISION')

    def test_recuperacion_solo_restaura_entrega_auditada(self):
        from django.core.management import call_command
        from io import StringIO
        log_event(self.user,'seguimiento.entrega','SeguimientoItem',self.item.pk,{'entrega':True})
        out=StringIO()
        call_command('recuperar_entrega_seguimiento',item=self.item.pk,actor=self.dg.username,stdout=out)
        self.item.refresh_from_db()
        self.assertEqual(self.item.estatus,'PENDIENTE')
        call_command('recuperar_entrega_seguimiento',item=self.item.pk,actor=self.dg.username,apply=True,stdout=out)
        self.sync()
        self.assertEqual(self.item.estatus,'EN_REVISION')
        self.check.refresh_from_db()
        self.assertTrue(self.check.completado)
        call_command('recuperar_entrega_seguimiento',item=self.item.pk,actor=self.dg.username,apply=True,stdout=out)
        self.assertEqual(AuditLog.objects.filter(action='seguimiento.recuperar_entrega').count(),1)

    def test_recuperacion_rechaza_entrega_ya_resuelta(self):
        from django.core.management import call_command, CommandError
        log_event(self.user,'seguimiento.entrega','SeguimientoItem',self.item.pk,{'entrega':True})
        log_event(self.dg,'seguimiento.devolver','SeguimientoItem',self.item.pk,{})
        with self.assertRaises(CommandError):
            call_command('recuperar_entrega_seguimiento',item=self.item.pk,actor=self.dg.username,apply=True)

    def test_bandeja_enlaza_al_detalle_dg(self):
        self.post('entregar')
        self.client.force_login(self.dg)
        page=self.client.get('/seguimiento/revision/')
        self.assertContains(page,f'href="/seguimiento/panel/{self.item.pk}/"')

    def test_error_writeback_conserva_entrega_y_comentario_async(self):
        from seguimiento.agente_dg_client import AgenteDGError
        self.post('entregar')
        self.client.force_login(self.dg)
        with patch('seguimiento.views._writeback_agente_dg_item',side_effect=AgenteDGError('sin respuesta')):
            response=self.post('resolver',{'accion':'aprobar','comentario':'Mi observación'})
        self.assertEqual(response.status_code,502)
        self.assertNotIn('html',response.json())
        self.item.refresh_from_db()
        self.assertEqual(self.item.estatus,'EN_REVISION')
        self.assertFalse(AuditLog.objects.filter(action='seguimiento.aprobar').exists())

    def test_devolucion_se_muestra_como_correccion_incluso_con_checklist_completo(self):
        self.post('entregar')
        self.client.force_login(self.dg)
        self.post('resolver',{'accion':'devolver','comentario':'Aclarar resultado'})
        self.sync('IN_PROGRESS')
        page=self.client.get(f'/seguimiento/panel/{self.item.pk}/')
        self.assertEqual(page.context['item'].estado_operativo_label,'Devuelto para corrección')

    def test_dg_ve_su_accion_no_texto_del_colaborador(self):
        self.post('entregar')
        self.client.force_login(self.dg)
        page=self.client.get(f'/seguimiento/panel/{self.item.pk}/')
        self.assertContains(page,'Entrega por revisar')
        self.assertNotContains(page,'Ya enviaste este acuerdo.')


from django.test import TransactionTestCase, Client
from django.db import close_old_connections
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier


@override_settings(SECURE_SSL_REDIRECT=False)
class RevisionConcurrenteTests(TransactionTestCase):
    def test_dos_decisiones_simultaneas_solo_aplican_una(self):
        dg=get_user_model().objects.create_superuser('dg.concurrente',email='dg@example.com',password='test-local')
        item=SeguimientoItem.objects.create(tipo='MINUTA',titulo='Concurrencia',estatus='EN_REVISION')
        barrier=Barrier(2)

        def decide(action):
            close_old_connections()
            try:
                client=Client()
                client.force_login(dg)
                barrier.wait(timeout=10)
                return client.post(f'/seguimiento/{item.pk}/resolver/',
                    {'accion':action,'comentario':'Decisión de prueba'},HTTP_ACCEPT='application/json').status_code
            finally:
                close_old_connections()

        with patch('seguimiento.views.notificar_seguimiento_aprobado'), patch('seguimiento.views.notificar_seguimiento_devuelto'):
            with ThreadPoolExecutor(max_workers=2) as pool:
                statuses=list(pool.map(decide,['aprobar','devolver']))
        self.assertEqual(sorted(statuses),[200,409])
        self.assertEqual(AuditLog.objects.filter(model='SeguimientoItem',object_id=str(item.pk),
            action__in=['seguimiento.aprobar','seguimiento.devolver']).count(),1)
