from datetime import timedelta
from django.test import TestCase, TransactionTestCase
from django.utils import timezone
from pos_bridge.models import PointSyncJob, PointBranch, PointSalesExtractionTask, PointSalesRawStaging, PointSalesNormalized, PointSalesQualityAlert, PointSalesDailyProductFact
from pos_bridge.management.commands.point_archive_inventory import eligible_task
from io import StringIO, BytesIO
import json
from unittest.mock import patch
from django.core.management.base import CommandError


class ArchiveInventoryTests(TestCase):
    def setUp(self):
        self.old=timezone.now()-timedelta(days=40)
        self.branch=PointBranch.objects.create(external_id='archive-test',name='ARCHIVE')
        self.job=PointSyncJob.objects.create(job_type='SALES',status='SUCCESS',finished_at=self.old)
        self.task=PointSalesExtractionTask.objects.create(sync_job=self.job,branch=self.branch,sale_date=self.old.date(),status='SUCCESS',finished_at=self.old,source_file='/app/storage/pos_bridge/raw_exports/test.xls',source_hash='a'*64,row_count=1)
        self.raw=PointSalesRawStaging.objects.create(sync_job=self.job,task=self.task,row_number=1,source_file=self.task.source_file,source_hash=self.task.source_hash,row_hash='b'*64)
        self.normal=PointSalesNormalized.objects.create(sync_job=self.job,task=self.task,raw_row=self.raw,branch=self.branch,fecha=self.old.date(),source_file=self.task.source_file,source_hash=self.task.source_hash)
    def test_closed_matching_source_is_eligible_without_mutation(self):
        self.assertTrue(eligible_task(self.task));self.assertEqual(PointSalesRawStaging.objects.count(),1)
    def test_job_level_alert_preserved_without_blocking_intact_archive(self):
        PointSalesQualityAlert.objects.create(sync_job=self.job,task=None,severity='CRITICAL',fecha=self.old.date())
        self.assertTrue(eligible_task(self.task))
        self.assertEqual(PointSalesQualityAlert.objects.get().severity, 'CRITICAL')
    def test_incomplete_reference_blocks(self):
        self.job.status='RUNNING';self.job.save();self.task.refresh_from_db()
        self.assertFalse(eligible_task(self.task))

    def test_extra_different_hash_blocks_even_if_matching_count_exists(self):
        PointSalesRawStaging.objects.create(sync_job=self.job,task=self.task,row_number=2,source_file=self.task.source_file,source_hash='c'*64,row_hash='d'*64)
        self.assertFalse(eligible_task(self.task))
    def test_orphan_fact_blocks(self):
        PointSalesDailyProductFact.objects.create(source_file=self.task.source_file,source_hash=self.task.source_hash,sale_date=self.old.date(),sync_job=None)
        self.assertFalse(eligible_task(self.task))

    def test_failed_other_source_same_day_preserved_without_blocking_archive(self):
        other_job=PointSyncJob.objects.create(job_type='SALES',status='FAILED',finished_at=self.old)
        other=PointSalesExtractionTask.objects.create(sync_job=other_job,branch=self.branch,
            sale_date=self.task.sale_date,status='FAILED',source_file='/app/storage/pos_bridge/raw_exports/other.xls')
        self.assertTrue(eligible_task(self.task))
        other.refresh_from_db()
        self.assertEqual(other.status,'FAILED')

    def test_failed_reference_to_same_source_still_blocks(self):
        other_job=PointSyncJob.objects.create(job_type='SALES',status='FAILED',finished_at=self.old)
        PointSalesExtractionTask.objects.create(sync_job=other_job,branch=self.branch,
            sale_date=self.task.sale_date,status='FAILED',source_file=self.task.source_file,source_hash=self.task.source_hash)
        self.assertFalse(eligible_task(self.task))

    def test_missing_normalized_row_still_blocks(self):
        self.normal.delete()
        self.assertFalse(eligible_task(self.task))

    def test_authoritative_reference_preserved_with_intact_category_source(self):
        from core.models import Sucursal
        from ventas.models import VentaAutoritativaPoint
        branch = Sucursal.objects.create(codigo='ARCHIVE', nombre='ARCHIVE')
        row = VentaAutoritativaPoint.objects.create(branch=branch, sale_date=self.old.date(),
            product_code='100', source_file=self.task.source_file)
        self.assertTrue(eligible_task(self.task))
        row.refresh_from_db()
        self.assertEqual(row.source_file, self.task.source_file)
        self.assertEqual(VentaAutoritativaPoint.objects.count(), 1)
        self.normal.delete()
        self.assertFalse(eligible_task(self.task))
        self.assertEqual(VentaAutoritativaPoint.objects.count(), 1)

    def test_other_report_domains_remain_excluded(self):
        from pos_bridge.models import PointProductHistoryImport, PointMonthlySalesOfficial
        history = PointProductHistoryImport.objects.create(file_hash='e'*64, source_filename='test.xls',
            report_path=self.task.source_file, product_name='Test')
        self.assertFalse(eligible_task(self.task))
        history.delete()
        PointMonthlySalesOfficial.objects.create(month_start=self.old.date().replace(day=1),
            month_end=self.old.date(), report_path=self.task.source_file)
        self.assertFalse(eligible_task(self.task))


class ArchiveStreamTests(TestCase):
    def test_stream_calls_fresh_handler_and_bounds_requests(self):
        from pos_bridge.management.commands.point_archive_inventory import Command
        request=lambda n:json.dumps({'id':n,'args':['--after-id','0']}).encode()+b'\n'
        output=StringIO();command=Command(stdout=output);calls=[]
        def handle(*args,**options):
            calls.append(options)
            command.stdout.write(json.dumps({'entries':[len(calls)]}))
        with patch.object(command,'handle',side_effect=handle),patch('sys.stdin',type('Input',(),{'buffer':BytesIO(request(1)+request(2))})()):
            command.stream()
        self.assertEqual([json.loads(x)['result']['entries'] for x in output.getvalue().splitlines()],[[1],[2]])
        self.assertEqual(len(calls),2)

    def test_stream_rejects_unknown_arguments_before_handler(self):
        from pos_bridge.management.commands.point_archive_inventory import Command
        command=Command(stdout=StringIO())
        invalid=[{'id':1,'args':['--delete','all']},{'id':True,'args':['--after-id','0']},{'id':1,'args':['--after-id','-1']},{'id':1,'args':['--name','x'],'extra':True}]
        for value in invalid:
            with self.subTest(value=value),patch.object(command,'handle') as handle,patch('sys.stdin',type('Input',(),{'buffer':BytesIO(json.dumps(value).encode()+b'\n')})()):
                with self.assertRaises(CommandError):command.stream()
                handle.assert_not_called()

    def test_stream_does_not_mix_parser_warnings_with_response(self):
        from pos_bridge.management.commands.point_archive_inventory import Command
        output=StringIO();command=Command(stdout=output)
        def handle(*a,**k):
            print('parser warning')
            command.stdout.write('{"rows":1}')
        request=b'{"id":1,"args":["--parse","/app/storage/pos_bridge/archive/staging/a.xls"]}\n'
        with patch.object(command,'handle',side_effect=handle),patch('sys.stdin',type('Input',(),{'buffer':BytesIO(request)})()):command.stream()
        self.assertEqual(json.loads(output.getvalue()),{'id':1,'result':{'rows':1}})


class ArchiveStreamDatabaseTests(TransactionTestCase):
    setUp=ArchiveInventoryTests.setUp

    def test_each_request_observes_changed_reference_and_closes_real_connection(self):
        from django.db import connection
        from pos_bridge.management.commands.point_archive_inventory import Command
        from pos_bridge.services.archive_access import ArchiveStore
        name='20260101_010101_point_sales_category_2025-12-01_2025-12-31_1_null.xls'
        source=str(ArchiveStore().sources_root/name)
        for model in (PointSalesExtractionTask,PointSalesRawStaging,PointSalesNormalized):
            model.objects.filter(source_file=self.task.source_file).update(source_file=source)
        request=lambda n:json.dumps({'id':n,'args':['--name',name]}).encode()+b'\n'
        owner=self
        class Input(BytesIO):
            reads=0
            def readline(self,*args):
                if self.reads==1:
                    owner.assertIsNone(connection.connection)
                    PointSalesExtractionTask.objects.filter(pk=owner.task.pk).update(status='FAILED')
                self.reads+=1
                return super().readline(*args)
        output=StringIO();command=Command(stdout=output)
        with patch('sys.stdin',type('Stdin',(),{'buffer':Input(request(1)+request(2))})()):command.stream()
        responses=[json.loads(x) for x in output.getvalue().splitlines()]
        self.assertEqual([len(x['result']['entries']) for x in responses],[1,0])
        self.assertIsNone(connection.connection)
