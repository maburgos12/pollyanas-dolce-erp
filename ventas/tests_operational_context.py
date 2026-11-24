from datetime import date
from io import StringIO
import json
from tempfile import NamedTemporaryFile
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError, PermissionDenied
from django.core.management import call_command
from django.test import TestCase
from ventas import models


class OperationalContextTests(TestCase):
    def test_documentary_source_exists(self):
        self.assertTrue(hasattr(models, 'ContextoComercial'), 'Falta fuente documental comercial inmutable')

    def setUp(self):
        self.actor = get_user_model().objects.create_user('context-dg', is_superuser=True)
        self.entries = [dict(clave='SEGURIDAD-20261120', fecha='2026-11-20', tipo='afectacion_parcial', titulo='Afectación comercial', descripcion='Algunas sucursales dejaron de vender; horas y sucursales exactas desconocidas.', evidencia=['Declaración DG 2026-11-24'])]

    def service(self):
        from ventas.services import operational_context
        return operational_context

    def apply(self, entries=None):
        service = self.service()
        entries = entries or self.entries
        preview = service.register_contexts(entries=entries, actor_id=self.actor.pk)
        return service.register_contexts(entries=entries, actor_id=self.actor.pk, apply=True, digest=preview['digest'])

    def test_preview_apply_idempotency_and_json_read(self):
        service = self.service()
        preview = service.register_contexts(entries=self.entries, actor_id=self.actor.pk)
        self.assertEqual(models.ContextoComercial.objects.count(), 0)
        self.apply()
        self.apply()
        self.assertEqual(models.ContextoComercial.objects.count(), 1)
        rows = service.contexts_for_range(start_date=date(2026,11,20), end_date=date(2026,11,21))
        self.assertEqual(rows[0]['fecha'], '2026-11-20')
        self.assertEqual(rows[0]['alcance'], 'empresa_general_informativo')
        json.dumps(rows)
        self.assertEqual(service.contexts_for_range(start_date=date(2026,10,1), end_date=date(2026,10,31)), [])
        with self.assertRaises(ValidationError):
            service.register_contexts(entries=self.entries, actor_id=self.actor.pk, apply=True, digest=preview['digest'])

    def test_conflicts_invalid_evidence_and_atomic_batch(self):
        self.apply()
        for changed in [dict(self.entries[0], descripcion='Otro contenido'), dict(self.entries[0], evidencia=[])]:
            with self.assertRaises(ValidationError):
                self.service().register_contexts(entries=[dict(self.entries[0], clave='NEW'), changed], actor_id=self.actor.pk, apply=True, digest='bad')
        self.assertEqual(models.ContextoComercial.objects.count(), 1)

    def test_actor_is_revalidated_and_bound_to_preview(self):
        service = self.service()
        preview = service.register_contexts(entries=self.entries, actor_id=self.actor.pk)
        other = get_user_model().objects.create_user('other-dg', is_superuser=True)
        with self.assertRaises(ValidationError):
            service.register_contexts(entries=self.entries, actor_id=other.pk, apply=True, digest=preview['digest'])
        self.actor.is_active = False
        self.actor.save()
        with self.assertRaises(PermissionDenied):
            service.register_contexts(entries=self.entries, actor_id=self.actor.pk, apply=True, digest=preview['digest'])

    def test_supported_mutations_are_blocked(self):
        self.apply()
        obj = models.ContextoComercial.objects.get()
        obj.descripcion = 'edit'
        for action in [obj.save, obj.delete, lambda: models.ContextoComercial.objects.update(descripcion='edit'), lambda: models.ContextoComercial.objects.all().delete(), lambda: models.ContextoComercial.objects.bulk_update([obj], ['descripcion'])]:
            with self.assertRaises(ValidationError):
                action()

    def test_command_defaults_to_preview(self):
        with NamedTemporaryFile(mode='w', suffix='.json') as file:
            json.dump(self.entries, file); file.flush()
            output = StringIO()
            call_command('registrar_contexto_comercial', archivo=file.name, actor=self.actor.pk, stdout=output)
            self.assertEqual(models.ContextoComercial.objects.count(), 0)
            preview = json.loads(output.getvalue())
            call_command('registrar_contexto_comercial', archivo=file.name, actor=self.actor.pk, aplicar=True, digest=preview['digest'], stdout=StringIO())
        self.assertEqual(models.ContextoComercial.objects.count(), 1)

class SalesContextMetadataTests(TestCase):
    def test_metadata_preserves_every_existing_value_and_product_bulk(self):
        from core.models import Sucursal
        from ventas.models import VentaAutoritativaPoint
        from ventas.services import sales_read_service as sales
        branch = Sucursal.objects.create(codigo='CTX', nombre='Contexto')
        actor = get_user_model().objects.create_user('sales-dg', is_superuser=True)
        day = date(2026,11,21)
        VentaAutoritativaPoint.objects.create(branch=branch, sale_date=day, product_code='ONE', total_amount=123, quantity=2)
        calls = [lambda: sales.get_daily_sales(branch, day), lambda: sales.get_sales_range(start_date=day,end_date=day), lambda: sales.get_sales_range_grouped(start_date=day,end_date=day,dimension='branch'), lambda: sales.get_daily_sales_bulk(fechas=[day],dimension='branch',include_indicators=True), lambda: sales.get_daily_sales_bulk(fechas=[day],dimension='product')]
        before = [fn() for fn in calls]
        self.assertIn('operational_contexts', before[0], 'Falta metadata informativa')
        from ventas.services.operational_context import register_contexts
        entries = [dict(clave='CLOSURE',fecha=day.isoformat(),tipo='cierre_empresa',titulo='Cierre declarado',descripcion='No altera ventas observadas',evidencia=['Declaración DG'])]
        preview=register_contexts(entries=entries,actor_id=actor.pk)
        register_contexts(entries=entries,actor_id=actor.pk,apply=True,digest=preview['digest'])
        def without_notes(value):
            if isinstance(value,dict): return {k:without_notes(v) for k,v in value.items() if k!='operational_contexts'}
            if isinstance(value,list): return [without_notes(v) for v in value]
            return value
        for fn, baseline in zip(calls,before):
            after=fn()
            self.assertEqual(without_notes(after), without_notes(baseline))
            notes=next(iter(after['dates'].values()))['operational_contexts'] if 'dates' in after else after['operational_contexts']
            self.assertEqual(len(notes),1)
        empty=sales.get_daily_sales(branch,date(2026,11,22))
        self.assertEqual(empty['operational_contexts'],[])

class ContextIntegrityRegressionTests(OperationalContextTests):
    def test_explicit_primary_key_cannot_overwrite_document(self):
        self.apply()
        original = models.ContextoComercial.objects.get()
        replacement = models.ContextoComercial(pk=original.pk, autor=self.actor, **dict(self.entries[0], descripcion='overwrite'))
        with self.assertRaises(ValidationError):
            replacement.save()
        original.refresh_from_db()
        self.assertEqual(original.descripcion, self.entries[0]['descripcion'])

    def test_compact_date_is_rejected(self):
        with self.assertRaises(ValidationError):
            self.service().register_contexts(entries=[dict(self.entries[0], fecha='20261120')], actor_id=self.actor.pk)

    def test_absent_sales_with_closure_note_remain_missing(self):
        from core.models import Sucursal
        from ventas.services.sales_read_service import get_daily_sales, get_sales_range
        branch = Sucursal.objects.create(codigo='MISSINGCTX', nombre='Sin datos')
        self.apply([dict(self.entries[0], fecha='2026-11-21', tipo='cierre_empresa')])
        for result in [get_daily_sales(branch, date(2026,11,21)), get_sales_range(start_date=date(2026,11,21),end_date=date(2026,11,21))]:
            self.assertEqual(len(result['operational_contexts']), 1)
            self.assertEqual(result['source'], 'none')

from django.db import connection, transaction, close_old_connections
from django.test import TransactionTestCase
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier


class ContextTransactionTests(TransactionTestCase):
    def setUp(self):
        self.actor = get_user_model().objects.create_user('readonly-dg',is_superuser=True)
        self.entries=[dict(clave='TX-CONTEXT',fecha='2026-11-21',tipo='cierre_empresa',titulo='Cierre declarado',descripcion='Referencia general',evidencia=['DG'])]

    def test_preview_works_in_repeatable_read_read_only(self):
        from ventas.services.operational_context import register_contexts
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
            result=register_contexts(entries=self.entries,actor_id=self.actor.pk)
            self.assertFalse(result['aplicado'])
        self.assertEqual(models.ContextoComercial.objects.count(),0)

    def test_two_dg_actors_cannot_race_the_same_new_key(self):
        from ventas.services.operational_context import register_contexts
        other=get_user_model().objects.create_user('concurrent-dg',is_superuser=True)
        previews={actor:register_contexts(entries=self.entries,actor_id=actor) for actor in [self.actor.pk,other.pk]}
        barrier=Barrier(2)
        def apply(actor):
            close_old_connections()
            barrier.wait(timeout=10)
            try:
                register_contexts(entries=self.entries,actor_id=actor,apply=True,digest=previews[actor]['digest'])
                return 'created'
            except ValidationError:
                return 'stale'
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes=list(pool.map(apply,previews))
        self.assertCountEqual(outcomes,['created','stale'])
        self.assertEqual(models.ContextoComercial.objects.count(),1)

class ContextQueryBudgetTests(TestCase):
    def test_one_context_query_per_selected_day_or_range(self):
        from django.test.utils import CaptureQueriesContext
        from ventas.services import sales_read_service as sales
        from core.models import Sucursal
        branch=Sucursal.objects.create(codigo='QUERYCTX',nombre='Consultas')
        day=date(2026,11,21)
        calls=[(lambda:sales.get_daily_sales(branch,day),1),
               (lambda:sales.get_sales_range(start_date=day,end_date=day),1),
               (lambda:sales.get_sales_range_grouped(start_date=day,end_date=day,dimension='product'),1),
               (lambda:sales.get_daily_sales_bulk(fechas=[day,date(2026,11,22)],dimension='product'),2)]
        for call, expected in calls:
            with CaptureQueriesContext(connection) as queries:
                call()
            notes=[q for q in queries if 'ventas_contextocomercial' in q['sql']]
            self.assertEqual(len(notes),expected)

class ContextSourceBoundaryTests(TestCase):
    def test_apply_and_four_readers_never_write_sales_or_rrhh(self):
        from django.test.utils import CaptureQueriesContext
        from core.models import Sucursal
        from pos_bridge.models import PointBranch, PointProduct, PointDailySale
        from reportes.models import FactVentaDiaria
        from rrhh.models import PermisoContingencia
        from ventas.services.operational_context import register_contexts
        from ventas.services import sales_read_service as sales
        day=date(2026,11,21)
        actor=get_user_model().objects.create_user('boundary-dg',is_superuser=True)
        branch=Sucursal.objects.create(codigo='BOUNDARY',nombre='Sucursal sintética')
        point_branch=PointBranch.objects.create(external_id='BOUNDARY',name='Sucursal sintética',erp_branch=branch)
        point_product=PointProduct.objects.create(external_id='BOUNDARY-PROD',name='Producto sintético')
        PointDailySale.objects.create(branch=point_branch,product=point_product,sale_date=day,quantity=2,total_amount=123,source_endpoint=sales.OFFICIAL_POINT_SOURCE)
        models.VentaAutoritativaPoint.objects.create(branch=branch,sale_date=day,product_code='BOUNDARY-PROD',total_amount=123,quantity=2)
        FactVentaDiaria.objects.create(fecha=day,sucursal=branch,producto_clave='BOUNDARY-PROD',producto_nombre='Producto sintético',cantidad=2,venta_total=123,source_kind='AUTHORITATIVE')
        permit=PermisoContingencia.objects.create(clave='CONTINGENCIA-SEGURIDAD-BOUNDARY',fecha_inicio=day,fecha_fin=day,motivo='seguridad',descripcion='Autorización laboral separada',evidencia=['Acta sintética'],alcance='empresa',tratamiento_salarial='conservar',estado='aprobado',creado_por=actor)
        protected=[PointBranch,PointProduct,PointDailySale,models.VentaAutoritativaPoint,FactVentaDiaria,PermisoContingencia]
        snapshots={model._meta.db_table:list(model.objects.order_by('pk').values()) for model in protected}
        entries=[dict(clave='SOURCE-BOUNDARY',fecha=day.isoformat(),tipo='cierre_empresa',titulo='Cierre declarado',descripcion='No modifica asistencia ni ventas observadas',evidencia=[permit.clave])]
        preview=register_contexts(entries=entries,actor_id=actor.pk)
        with CaptureQueriesContext(connection) as captured:
            register_contexts(entries=entries,actor_id=actor.pk,apply=True,digest=preview['digest'])
        writes=[query['sql'] for query in captured if query['sql'].lstrip().upper().startswith(('INSERT','UPDATE','DELETE'))]
        self.assertEqual(len(writes),1)
        self.assertIn('INSERT INTO "ventas_contextocomercial"',writes[0])
        calls=[lambda:sales.get_daily_sales(branch,day),lambda:sales.get_sales_range(start_date=day,end_date=day),lambda:sales.get_daily_sales_bulk(fechas=[day],dimension='product'),lambda:sales.get_sales_range_grouped(start_date=day,end_date=day,dimension='branch')]
        with CaptureQueriesContext(connection) as captured:
            for call in calls:
                call()
        self.assertFalse([query['sql'] for query in captured if query['sql'].lstrip().upper().startswith(('INSERT','UPDATE','DELETE'))])
        for model in protected:
            self.assertEqual(list(model.objects.order_by('pk').values()),snapshots[model._meta.db_table],model._meta.db_table)
        permit.refresh_from_db()
        self.assertEqual(permit.estado,'aprobado')
        self.assertEqual(permit.tratamiento_salarial,'conservar')
