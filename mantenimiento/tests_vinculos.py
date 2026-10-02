"""Vínculos de atención: no consolidan trabajos, estados ni importes."""
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from decimal import Decimal
from threading import Barrier, Event
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.db import close_old_connections, transaction
from django.db.models.deletion import ProtectedError
from django.test import SimpleTestCase, TestCase, TransactionTestCase
from django.conf import settings
from pathlib import Path
from django.urls import reverse

from activos.models import Activo, OrdenMantenimiento
from core.models import AuditLog, Sucursal, UserModuleAccess, UserProfile
from fallas.models import CategoriaFalla, ReporteFalla
from mantenimiento.models import SolicitudCancelacion


class VinculosFixture:
    def setUp(self):
        self.admin = get_user_model().objects.create_superuser('links-admin', 'links@example.com', 'test')
        self.branch = Sucursal.objects.create(codigo='LINK', nombre='Propia')
        self.other_branch = Sucursal.objects.create(codigo='LNK2', nombre='Ajena')
        self.asset = Activo.objects.create(nombre='Horno', sucursal=self.branch)
        self.other_asset = Activo.objects.create(nombre='Otro horno', sucursal=self.other_branch)
        self.category = CategoriaFalla.objects.create(nombre='Equipo vínculo', tipo=CategoriaFalla.TIPO_EQUIPO)
        self.report = self.new_report()
        self.order = self.new_order()
        self.client.force_login(self.admin)

    def new_report(self, **changes):
        fields = dict(sucursal=self.branch, activo_relacionado=self.asset, categoria=self.category,
                      titulo='Horno no calienta', descripcion='Revisar', reportado_por=self.admin,
                      foto_evidencia='fallas/evidencias/conservar.jpg', costo_real=Decimal('123.45'))
        fields.update(changes)
        return ReporteFalla.objects.create(**fields)

    def new_order(self, **changes):
        fields = dict(activo_ref=self.asset, descripcion='Atención', creado_por=self.admin,
                      costo_repuestos=Decimal('234.56'), factura_archivo='activos/facturas/conservar.pdf')
        fields.update(changes)
        return OrdenMantenimiento.objects.create(**fields)

    def user(self, name, access='view', branch=None):
        user = get_user_model().objects.create_user(name, password='test')
        UserProfile.objects.create(user=user, sucursal=branch or self.branch)
        UserModuleAccess.objects.create(user=user, module='mantenimiento.dashboard', access=access)
        return user

    def url(self, tipo='falla', pk=None):
        return f'/api/mantenimiento/v2/items/{tipo}/{pk or self.report.pk}/vinculos/'

    def link(self, report=None, order=None, user=None, motivo='Atención documentada'):
        from mantenimiento.services_vinculos import crear_vinculo
        return crear_vinculo(user or self.admin, (order or self.order).pk, (report or self.report).pk, motivo)

    def audits(self):
        return AuditLog.objects.filter(model='mantenimiento.VinculoAtencionEquipo')


class VinculosTests(VinculosFixture, TestCase):
    def test_api_creates_retries_and_preserves_both_sources(self):
        url = self.url()
        payload = {'orden_id': self.order.pk, 'motivo': '  Atención real  '}
        first = self.client.post(url, payload, content_type='application/json')
        self.assertEqual(first.status_code, 201)
        again = self.client.post(url, payload, content_type='application/json')
        self.assertEqual(again.status_code, 200)
        self.assertEqual(first.json()['id'], again.json()['id'])
        self.assertEqual(self.audits().filter(action='CREATE').count(), 1)
        self.order.refresh_from_db(); self.report.refresh_from_db()
        self.assertEqual(self.order.costo_repuestos, Decimal('234.56'))
        self.assertEqual(self.report.costo_real, Decimal('123.45'))
        self.assertEqual(self.order.estatus, OrdenMantenimiento.ESTATUS_PENDIENTE)
        self.assertEqual(self.report.estatus, ReporteFalla.ESTATUS_ABIERTO)

    def test_many_real_same_day_jobs_and_reports_remain_distinct(self):
        second_order = self.new_order()
        second_report = self.new_report(titulo='Otra falla')
        self.link(); self.link(order=second_order); self.link(report=second_report)
        response = self.client.get(self.url()).json()
        self.assertEqual(response['pagination']['total'], 2)
        self.assertEqual({row['documento']['id'] for row in response['results']}, {self.order.pk, second_order.pk})
        self.assertEqual(OrdenMantenimiento.objects.count(), 2)
        self.assertEqual(ReporteFalla.objects.count(), 2)

    def test_validation_targets_reasons_duplicates_and_scopes(self):
        invalid_reports = [self.new_report(tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION), self.new_report(activo_relacionado=None), self.new_report(activo_relacionado=self.other_asset),
                           self.new_report(sucursal=self.other_branch), self.new_report(duplicado_de=self.report)]
        for report in invalid_reports:
            with self.subTest(report=report.pk):
                response = self.client.post(self.url('orden', self.order.pk), {'reporte_id': report.pk, 'motivo':'Revisión'})
                self.assertEqual(response.status_code, 400)
        for motivo in ['', '   ', None, 123, 'x' * 2001]:
            with self.subTest(motivo=motivo):
                self.assertEqual(self.client.post(self.url(), {'orden_id': self.order.pk, 'motivo': motivo}, content_type='application/json').status_code, 400)
        self.assertEqual(self.client.post(self.url(), {'orden_id': 'x', 'motivo':'Revisión'}).status_code, 400)
        for target in [1.5, True, [], {}, '1.5']:
            with self.subTest(target=target):
                self.assertEqual(self.client.post(self.url(), {'orden_id':target, 'motivo':'Revisión'}, content_type='application/json').status_code, 400)
        self.assertEqual(self.client.get(self.url('unidad')).status_code, 400)
        self.assertEqual(self.client.post(self.url(), {'orden_id': 999999, 'motivo':'Revisión'}).status_code, 404)
        reader = self.user('reader')
        self.client.force_login(reader)
        self.assertFalse(self.client.get(self.url()).json()['can_manage'])
        self.assertEqual(self.client.post(self.url(), {'orden_id':self.order.pk, 'motivo':'Revisión'}).status_code, 403)
        self.client.force_login(self.user('writer', 'manage'))
        foreign = self.new_order(activo_ref=self.other_asset)
        self.assertEqual(self.client.post(self.url(), {'orden_id':foreign.pk, 'motivo':'Revisión'}).status_code, 404)
        self.assertEqual(self.client.get(self.url('orden', foreign.pk)).status_code, 404)

    def test_get_scopes_both_parents_even_after_moving_and_never_exposes_costs(self):
        self.link()
        self.client.force_login(self.user('list-reader'))
        response = self.client.get(self.url()).json()
        self.assertEqual(response['pagination']['total'], 1)
        self.assertNotIn('costo', str(response))
        self.order.activo_ref = self.other_asset; self.order.save()
        response = self.client.get(self.url()).json()
        self.assertEqual(response['results'], [])
        self.assertEqual(response['pagination']['total'], 0)

    def test_candidates_same_asset_branch_no_duplicates_paginated(self):
        good = self.new_report(titulo='Candidato')
        self.new_report(duplicado_de=self.report)
        self.new_report(activo_relacionado=None)
        self.new_report(sucursal=self.other_branch)
        self.link()
        response = self.client.get(self.url('orden', self.order.pk), {'candidatos':1, 'page_size':1000}).json()
        self.assertEqual(response['pagination']['page_size'], 100)
        self.assertEqual([row['id'] for row in response['results']], [good.pk])
        self.assertNotIn('costo', str(response))
        self.assertEqual(self.client.get(self.url(), {'page':'x'}).status_code, 400)

    def test_audit_failure_rolls_back_create_and_removal(self):
        from mantenimiento.models import VinculoAtencionEquipo
        with patch('mantenimiento.services_vinculos.log_event', side_effect=RuntimeError('audit unavailable')):
            with self.assertRaises(RuntimeError): self.link()
        self.assertFalse(VinculoAtencionEquipo.objects.exists())
        link, _ = self.link()
        from mantenimiento.services_vinculos import retirar_vinculo
        with patch('mantenimiento.services_vinculos.log_event', side_effect=RuntimeError('audit unavailable')):
            with self.assertRaises(RuntimeError): retirar_vinculo(self.admin, link.pk, 'Corrección')
        self.assertTrue(VinculoAtencionEquipo.objects.filter(pk=link.pk).exists())

    def test_remove_requires_reason_and_write_scope_and_keeps_evidence_costs(self):
        link, _ = self.link()
        url = f'/api/mantenimiento/v2/vinculos/{link.pk}/'
        self.assertEqual(self.client.delete(url, {'motivo':' '}, content_type='application/json').status_code, 400)
        self.client.force_login(self.user('remove-reader'))
        self.assertEqual(self.client.delete(url, {'motivo':'Corrección'}, content_type='application/json').status_code, 403)
        self.client.force_login(self.user('remove-foreign', 'manage', self.other_branch))
        self.assertEqual(self.client.delete(url, {'motivo':'Corrección'}, content_type='application/json').status_code, 404)
        self.client.force_login(self.admin)
        self.assertEqual(self.client.delete(url, {'motivo':'Corrección'}, content_type='application/json').status_code, 200)
        self.assertEqual(self.client.delete(url, {'motivo':'Corrección'}, content_type='application/json').status_code, 404)
        self.order.refresh_from_db(); self.report.refresh_from_db()
        self.assertEqual(self.order.factura_archivo.name, 'activos/facturas/conservar.pdf')
        self.assertEqual(self.report.foto_evidencia.name, 'fallas/evidencias/conservar.jpg')
        self.assertEqual(self.report.costo_real, Decimal('123.45'))
        self.assertEqual(self.audits().filter(action='DELETE').count(), 1)

    def test_model_protects_both_parents(self):
        self.link()
        for source in [self.order, self.report]:
            with self.assertRaises(ProtectedError): source.delete()

    def test_cancellation_all_paths_keep_pending_request_and_source(self):
        self.link()
        message = 'Este documento tiene trabajos o reportes vinculados. Revisa los vínculos antes de eliminarlo.'
        for tipo, source in [('falla', self.report), ('orden', self.order)]:
            for path in ['mobile', 'html', 'direct']:
                with self.subTest(tipo=tipo, path=path):
                    request = SolicitudCancelacion.objects.create(tipo=tipo, objeto_id=source.pk, referencia='Documento', motivo='Revisar', solicitado_por=self.admin)
                    if path == 'mobile':
                        response = self.client.post(f'/api/mantenimiento/cancelaciones/{request.pk}/resolver/', {'accion':'aprobar'})
                        self.assertEqual(response.status_code, 400); self.assertIn(message, response.json()['error'])
                    else:
                        url = reverse('mantenimiento:mant-resolver-cancelacion', args=[request.pk]) if path == 'html' else reverse('mantenimiento:mant-cancelar', args=[tipo,source.pk])
                        response = self.client.post(url, {'accion':'aprobar'})
                        self.assertEqual(response.status_code, 302)
                        self.assertIn(message, [str(m) for m in response.wsgi_request._messages])
                    request.refresh_from_db()
                    self.assertEqual(request.estatus, SolicitudCancelacion.ESTATUS_PENDIENTE)
                    self.assertIsNone(request.resuelto_en)
                    self.assertTrue(type(source).objects.filter(pk=source.pk).exists())
        response = self.client.post(reverse('fallas:pwa-eliminar-reporte', args=[self.report.pk]))
        self.assertEqual(response.status_code, 302)
        self.assertIn(message, [str(m) for m in response.wsgi_request._messages])

    def test_unlinked_cancellation_retains_existing_behavior(self):
        for tipo, source in [('falla', self.report), ('orden', self.order)]:
            request = SolicitudCancelacion.objects.create(tipo=tipo, objeto_id=source.pk, referencia='Documento', motivo='Revisar')
            response = self.client.post(f'/api/mantenimiento/cancelaciones/{request.pk}/resolver/', {'accion':'aprobar'})
            self.assertEqual(response.status_code, 200)
            request.refresh_from_db()
            self.assertEqual(request.estatus, SolicitudCancelacion.ESTATUS_APROBADA)
            self.assertFalse(type(source).objects.filter(pk=source.pk).exists())


    def test_admin_single_and_bulk_delete_show_protection_without_deleting_sources(self):
        self.link()
        for model, source in [('activos_ordenmantenimiento', self.order), ('fallas_reportefalla', self.report)]:
            with self.subTest(model=model):
                response = self.client.post(reverse(f'admin:{model}_delete', args=[source.pk]), {'post':'yes'})
                self.assertEqual(response.status_code, 200)
                self.assertTrue(type(source).objects.filter(pk=source.pk).exists())
                response = self.client.post(reverse(f'admin:{model}_changelist'), {
                    'action':'delete_selected', '_selected_action':[source.pk], 'post':'yes',
                })
                self.assertEqual(response.status_code, 200)
                self.assertTrue(type(source).objects.filter(pk=source.pk).exists())

    def test_admin_unlinked_single_and_bulk_delete_retain_standard_behavior(self):
        for model, creator in [('activos_ordenmantenimiento', self.new_order), ('fallas_reportefalla', self.new_report)]:
            with self.subTest(model=model):
                source = creator()
                self.assertEqual(self.client.post(reverse(f'admin:{model}_delete', args=[source.pk]), {'post':'yes'}).status_code, 302)
                self.assertFalse(type(source).objects.filter(pk=source.pk).exists())
                sources = [creator(), creator()]
                response = self.client.post(reverse(f'admin:{model}_changelist'), {
                    'action':'delete_selected', '_selected_action':[obj.pk for obj in sources], 'post':'yes',
                })
                self.assertEqual(response.status_code, 302)
                self.assertFalse(type(source).objects.filter(pk__in=[obj.pk for obj in sources]).exists())

    def test_document_disappearing_before_locked_reread_returns_404_without_delete_logs(self):
        from django.contrib.admin.models import DELETION, LogEntry
        cases = [
            (self.report, reverse('fallas:pwa-eliminar-reporte', args=[self.report.pk]), {}),
            (self.report, reverse('admin:fallas_reportefalla_delete', args=[self.report.pk]), {'post':'yes'}),
            (self.order, reverse('admin:activos_ordenmantenimiento_delete', args=[self.order.pk]), {'post':'yes'}),
        ]
        self.client.raise_request_exception = False
        for source, url, data in cases:
            with self.subTest(url=url):
                before_logs = LogEntry.objects.filter(action_flag=DELETION).count()
                # La lectura inicial encuentra el documento; la relectura con
                # bloqueo simula que otro eliminador lo borró mientras esperaba.
                locked = Mock(spec=['get', 'model'])
                locked.model = type(source)
                locked.get.side_effect = type(source).DoesNotExist
                with patch('django.db.models.query.QuerySet.select_for_update', return_value=locked):
                    response = self.client.post(url, data)
                self.assertEqual(response.status_code, 404)
                locked.get.assert_called_once_with(pk=source.pk)
                self.assertEqual(LogEntry.objects.filter(action_flag=DELETION).count(), before_logs)
                self.assertTrue(type(source).objects.filter(pk=source.pk).exists())

    def test_admin_late_protection_rolls_back_delete_logs_and_returns_controlled_message(self):
        from django.contrib.admin.models import DELETION, LogEntry
        message = 'Este documento tiene trabajos o reportes vinculados. Revisa los vínculos antes de eliminarlo.'
        self.client.raise_request_exception = False
        for model, source in [('activos_ordenmantenimiento', self.order), ('fallas_reportefalla', self.report)]:
            for bulk in [False, True]:
                with self.subTest(model=model, bulk=bulk):
                    method = 'delete_queryset' if bulk else 'delete_model'
                    url = reverse(f'admin:{model}_changelist') if bulk else reverse(f'admin:{model}_delete', args=[source.pk])
                    data = {'action':'delete_selected', '_selected_action':[source.pk], 'post':'yes'} if bulk else {'post':'yes'}
                    before_logs = LogEntry.objects.filter(action_flag=DELETION).count()
                    with patch(f'django.contrib.admin.options.ModelAdmin.{method}', side_effect=ProtectedError(message, [source])):
                        response = self.client.post(url, data)
                    self.assertEqual(response.status_code, 302)
                    self.assertIn(message, [str(m) for m in response.wsgi_request._messages])
                    self.assertEqual(LogEntry.objects.filter(action_flag=DELETION).count(), before_logs)
                    self.assertTrue(type(source).objects.filter(pk=source.pk).exists())

    def test_unlinked_html_direct_and_owner_deletes_retain_existing_behavior(self):
        for path in ['html', 'direct', 'owner']:
            for tipo in (['falla'] if path == 'owner' else ['falla', 'orden']):
                with self.subTest(path=path, tipo=tipo):
                    source = self.new_report() if tipo == 'falla' else self.new_order()
                    request = SolicitudCancelacion.objects.create(tipo=tipo, objeto_id=source.pk, referencia='Documento', motivo='Revisar')
                    if path == 'html':
                        url = reverse('mantenimiento:mant-resolver-cancelacion', args=[request.pk])
                    elif path == 'direct':
                        url = reverse('mantenimiento:mant-cancelar', args=[tipo, source.pk])
                    else:
                        url = reverse('fallas:pwa-eliminar-reporte', args=[source.pk])
                    self.assertEqual(self.client.post(url, {'accion':'aprobar'}).status_code, 302)
                    self.assertFalse(type(source).objects.filter(pk=source.pk).exists())
                    request.refresh_from_db()
                    self.assertEqual(request.estatus, SolicitudCancelacion.ESTATUS_APROBADA if path == 'html' else SolicitudCancelacion.ESTATUS_PENDIENTE)


class VinculosConcurrencyTests(VinculosFixture, TransactionTestCase):
    def test_same_pair_concurrent_requests_create_one_link_and_one_audit(self):
        barrier = Barrier(2)
        def create():
            close_old_connections()
            try:
                barrier.wait(timeout=10)
                user = get_user_model().objects.get(pk=self.admin.pk)
                return self.link(user=user)[1]
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool:
            created = list(pool.map(lambda _: create(), range(2)))
        self.assertEqual(sorted(created), [False, True])
        self.assertEqual(self.audits().filter(action='CREATE').count(), 1)


    def test_link_creation_racing_cancellation_blocks_delete_and_keeps_request_pending(self):
        from core.audit import log_event as real_log
        from mantenimiento.views import _resolver_cancelacion_obj
        for tipo, source in [('orden', self.order), ('falla', self.report)]:
            with self.subTest(tipo=tipo):
                entered = Event()
                release = Event()
                request = SolicitudCancelacion.objects.create(tipo=tipo, objeto_id=source.pk, referencia='Carrera', motivo='Eliminar')
                def paused_audit(*args, **kwargs):
                    real_log(*args, **kwargs)
                    entered.set()
                    if not release.wait(10): raise RuntimeError('creation timed out')
                def create():
                    close_old_connections()
                    try:
                        return self.link(user=get_user_model().objects.get(pk=self.admin.pk))
                    finally:
                        close_old_connections()
                def delete():
                    close_old_connections()
                    try:
                        actor = get_user_model().objects.get(pk=self.admin.pk)
                        pending = SolicitudCancelacion.objects.get(pk=request.pk)
                        return _resolver_cancelacion_obj(pending, actor, 'aprobar')
                    finally:
                        close_old_connections()
                with patch('mantenimiento.services_vinculos.log_event', side_effect=paused_audit):
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        creation = pool.submit(create)
                        self.assertTrue(entered.wait(10))
                        deletion = pool.submit(delete)
                        try:
                            with self.assertRaises(TimeoutError): deletion.result(timeout=.2)
                        finally:
                            release.set()
                        link, created = creation.result(timeout=10)
                        self.assertTrue(created)
                        with self.assertRaises(ProtectedError): deletion.result(timeout=10)
                request.refresh_from_db()
                self.assertEqual(request.estatus, SolicitudCancelacion.ESTATUS_PENDIENTE)
                self.assertIsNone(request.resuelto_en)
                self.assertTrue(type(source).objects.filter(pk=source.pk).exists())
                link.delete()

    def test_delete_winning_race_leaves_no_link_or_creation_audit(self):
        from django.http import Http404
        from mantenimiento.views import _resolver_cancelacion_obj
        for tipo in ['orden', 'falla']:
            with self.subTest(tipo=tipo):
                order = self.new_order()
                report = self.new_report()
                source = order if tipo == 'orden' else report
                request = SolicitudCancelacion.objects.create(tipo=tipo, objeto_id=source.pk, referencia='Carrera', motivo='Eliminar')
                def create():
                    close_old_connections()
                    try:
                        return self.link(order=order, report=report, user=get_user_model().objects.get(pk=self.admin.pk))
                    finally:
                        close_old_connections()
                with ThreadPoolExecutor(max_workers=1) as pool:
                    with transaction.atomic():
                        type(source).objects.select_for_update().get(pk=source.pk)
                        creation = pool.submit(create)
                        with self.assertRaises(TimeoutError): creation.result(timeout=.2)
                        self.assertTrue(_resolver_cancelacion_obj(request, self.admin, 'aprobar'))
                    with self.assertRaises(Http404): creation.result(timeout=10)
                self.assertFalse(self.audits().filter(action='CREATE').exists())
                request.refresh_from_db()
                self.assertEqual(request.estatus, SolicitudCancelacion.ESTATUS_APROBADA)


class VinculosUiContractTests(SimpleTestCase):
    def test_detail_has_independent_links_with_root_return_context(self):
        source = (Path(settings.BASE_DIR) / 'templates/mantenimiento/pwa.html').read_text()
        self.assertIn('Trabajos vinculados', source)
        self.assertIn('Reportes atendidos', source)
        self.assertIn('loadDetailLinks(uid, generation)', source)
        self.assertIn('if (!fromLinked)', source)
        self.assertIn('window.scrollTo(0, context.scroll)', source)
        self.assertIn('if (generation !== state.requestGeneration.detail)', source)
        self.assertIn('Selecciona un documento', source)
        self.assertIn('Vincular orden existente', source)
        self.assertIn('Retirar vínculo', source)
        for renderer, end in [('renderFallaDetalle', 'guardarSeguimientoFalla'), ('renderOrdenDetalle', 'guardarSeguimientoOrden')]:
            body = source[source.index('async function '+renderer):source.index('async function '+end)]
            self.assertIn('id="detail-links"', body)
            self.assertIn('loadDetailLinks(', body)
