"""Regresión: los puntos de la minuta deben sobrevivir al viaje ERP-origen-ERP."""
import copy
import json
import os
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from core.models import AuditLog
from seguimiento.models import SeguimientoItem, SeguimientoChecklistItem
from seguimiento.services import upsert_agente_dg_payload


@override_settings(SECURE_SSL_REDIRECT=False, EMAIL_BACKEND='django.core.mail.backends.locmem.EmailBackend')
class ChecklistSyncTests(TestCase):
    def setUp(self):
        self.user=get_user_model().objects.create_user('puntos.test',email='puntos@example.com')
        self.item=SeguimientoItem.objects.create(tipo='MINUTA',titulo='Minuta prueba',responsable_user=self.user,
            metadata={'source':'agente_dg','source_table':'minute_agreements','source_id':86})
        self.check=SeguimientoChecklistItem.objects.create(seguimiento=self.item,titulo='CFE',orden=1)
        self.source={'id':86,'status':'OPEN','updated_at':'2026-09-08T20:00:00+00:00',
            'checklist_items':[{'id':'original-1','text':'CFE','completed':False,'completed_at':None}]}
        self.client.force_login(self.user)
        self.addCleanup(patch.stopall)
        patch.dict(os.environ,{'AGENTE_DG_WRITEBACK_ENABLED':'true'}).start()
        patch('seguimiento.agente_dg_client.is_configured',return_value=True).start()
        patch('seguimiento.views.notificar_seguimiento_avance').start()
        patch('seguimiento.views.notificar_seguimiento_entrega').start()
        self.http=patch('seguimiento.agente_dg_client._request',side_effect=self.api).start()

    def api(self,method,path,**kwargs):
        if method=='PATCH':
            self.source['checklist_items']=copy.deepcopy(kwargs['json']['checklist_items'])
            self.source['updated_at']='2026-09-08T21:00:00+00:00'
        return Mock(status_code=200,json=lambda:copy.deepcopy([self.source] if method=='GET' else self.source))

    def mark(self,value='1'):
        return self.client.post(f'/seguimiento/{self.item.pk}/checklist/{self.check.pk}/',
            {'completado':value},HTTP_ACCEPT='application/json')

    def sync(self,completed,updated_at='2026-09-08T20:00:00+00:00'):
        upsert_agente_dg_payload({'source_table':'minute_agreements','source_id':86,'record':{
            'titulo':self.item.titulo,'status':'OPEN','user_email':self.user.email,'updated_at':updated_at,
            'checklist_items_json':json.dumps([{'id':'original-1','text':'CFE','completed':completed}])}})
        self.check.refresh_from_db()
        self.item.refresh_from_db()

    def test_marcar_publica_origen_y_audita_estado(self):
        self.mark()
        self.assertTrue(self.source['checklist_items'][0]['completed'])
        event=AuditLog.objects.get(action='seguimiento.checklist')
        self.assertTrue(event.payload['completado'])

    def test_fallo_origen_no_finge_guardado_local(self):
        from seguimiento.agente_dg_client import AgenteDGError
        self.http.side_effect=AgenteDGError('sin conexión')
        response=self.mark()
        self.check.refresh_from_db()
        self.assertFalse(self.check.completado)
        self.assertEqual(response.status_code,502)

    def test_reintentar_mismo_valor_no_desmarca(self):
        self.mark()
        self.mark()
        self.check.refresh_from_db()
        self.assertTrue(self.check.completado)

    def test_deshacer_no_se_revierte_por_sync_atrasado(self):
        self.mark()
        self.mark('0')
        self.sync(True)
        self.assertFalse(self.check.completado)

    def test_sync_mas_nuevo_si_puede_reabrir_punto(self):
        self.mark()
        self.sync(False,'2026-09-08T22:00:00+00:00')
        self.assertFalse(self.check.completado)

    def test_no_reemplaza_puntos_remotos_ajenos(self):
        self.source['checklist_items'].append({'id':'extra','text':'Seguro','completed':False})
        self.mark()
        self.assertEqual(self.source['checklist_items'][1],{'id':'extra','text':'Seguro','completed':False})

    def test_punto_ambiguo_no_envia_patch(self):
        self.source['checklist_items'].append({'id':'duplicado','text':'CFE','completed':False})
        self.mark()
        self.check.refresh_from_db()
        self.assertFalse(self.check.completado)
        self.assertFalse(any(c.args[0]=='PATCH' for c in self.http.call_args_list))

    def test_entrega_guarda_puntos_y_no_cambia_con_sync(self):
        self.mark()
        response=self.client.post(f'/seguimiento/{self.item.pk}/entregar/',HTTP_ACCEPT='application/json')
        self.assertEqual(response.status_code,200)
        event=AuditLog.objects.get(action='seguimiento.entrega')
        self.assertEqual(event.payload['checklist'][0]['titulo'],'CFE')
        self.assertTrue(event.payload['checklist'][0]['completado'])
        self.sync(False,'2026-09-08T23:00:00+00:00')
        self.assertTrue(self.check.completado)
        self.assertEqual(self.item.estatus,'EN_REVISION')

    def test_checkpoint_avanza_para_no_resucitar_estado_antiguo(self):
        self.mark()
        self.sync(False,'2026-09-08T22:00:00+00:00')
        self.sync(True,'2026-09-08T20:00:00+00:00')
        self.assertFalse(self.check.completado)

    def test_entrega_no_omite_punto_nuevo_del_origen(self):
        self.mark()
        self.source['checklist_items'].append({'id':'extra','text':'Seguro','completed':False})
        response=self.client.post(f'/seguimiento/{self.item.pk}/entregar/',HTTP_ACCEPT='application/json')
        self.assertEqual(response.status_code,502)
        self.assertFalse(AuditLog.objects.filter(action='seguimiento.entrega').exists())

    def test_punto_en_revision_responde_json_sin_mutar(self):
        self.item.estatus='EN_REVISION'
        self.item.save()
        response=self.mark()
        self.assertEqual(response.status_code,409)
        self.assertFalse(self.http.called)
