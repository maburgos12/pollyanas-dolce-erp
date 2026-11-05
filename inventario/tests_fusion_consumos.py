from datetime import date
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor

from django.contrib.auth import get_user_model
from django.contrib.messages import get_messages
from django.core.exceptions import ValidationError
from django.db import connection, OperationalError, transaction
from django.test import TestCase, TransactionTestCase
from django.urls import reverse
from unittest.mock import patch

from inventario.models import ExistenciaInsumo, MovimientoInventario, UBICACION_CEDIS
from inventario.services_consumo_bom import ConsumoInsumoAutoService, bloquear_consumos_fusion
from inventario.services_auditoria_insumos import ConsumoInsumoAuditService
from inventario.views import _merge_insumo_into_target
from maestros.models import Insumo, InsumoAlias, UnidadMedida
from pos_bridge.models import PointBranch, PointProductionLine
from recetas.models import LineaReceta, Receta


class FusionConsumosTests(TestCase):
    def setUp(self):
        self.unidad = UnidadMedida.objects.create(codigo='pza', nombre='Pieza')
        self.source = Insumo.objects.create(nombre='Pan Lotus', unidad_base=self.unidad)
        self.target = Insumo.objects.create(nombre='Pan Lotus', unidad_base=self.unidad)
        self.alias = InsumoAlias.objects.create(nombre='Pan Lotus anterior', insumo=self.source)
        self.service = ConsumoInsumoAutoService()
        self.actor = get_user_model().objects.create_superuser('fusion', 'fusion@example.com', 'test')
        self.client.force_login(self.actor)

    def consumo(self, insumo, production='901', **kwargs):
        return MovimientoInventario.objects.create(
            tipo=MovimientoInventario.TIPO_CONSUMO, insumo=insumo, cantidad=Decimal('8'),
            referencia='PROD-' + production,
            source_hash=self.service._source_hash('BOM', production, insumo.pk), **kwargs)

    def test_reproceso_despues_de_fusion_no_crea_otro_consumo_ni_descuenta_stock(self):
        receta = Receta.objects.create(nombre='Pastel Lotus')
        LineaReceta.objects.create(receta=receta, insumo=self.source, cantidad=1,
                                  unidad=self.unidad, insumo_texto='Pan Lotus', unidad_texto='PZA')
        branch = PointBranch.objects.create(external_id='fusion-cedis', name='CEDIS')
        PointProductionLine.objects.create(branch=branch, receta=receta,
            production_external_id='901', detail_external_id='1', source_hash='fusion-production',
            production_date=date(2026, 5, 10), item_name=receta.nombre, produced_quantity=8)
        ExistenciaInsumo.objects.create(insumo=self.source, almacen=UBICACION_CEDIS, stock_actual=100)
        ExistenciaInsumo.objects.create(insumo=self.target, almacen=UBICACION_CEDIS, stock_actual=30)
        self.service.generar_consumos_produccion(date(2026, 5, 10), date(2026, 5, 10))
        original = MovimientoInventario.objects.get(insumo=self.source)
        old_hash = original.source_hash
        _merge_insumo_into_target(self.source, self.target, acted_by=self.actor)
        original.refresh_from_db()
        stock = ExistenciaInsumo.objects.get(insumo=self.target, almacen=UBICACION_CEDIS)
        self.assertEqual(original.source_hash, self.service._source_hash('BOM', '901', self.target.pk))
        self.assertIn(old_hash, str(original.trazabilidad))
        self.assertEqual(stock.stock_actual, Decimal('122'))
        summary = self.service.generar_consumos_produccion(date(2026, 5, 10), date(2026, 5, 10))
        stock.refresh_from_db()
        self.assertEqual(summary.movimientos_creados, 0)
        self.assertEqual(summary.movimientos_actualizados, 0)
        self.assertEqual(MovimientoInventario.objects.filter(tipo='CONSUMO').count(), 1)
        self.assertEqual(stock.stock_actual, Decimal('122'))

    def test_colision_bloquea_todas_las_relaciones_antes_de_fusionar(self):
        old = self.consumo(self.source)
        self.consumo(self.target)
        snapshot = list(MovimientoInventario.objects.order_by('pk').values())
        with self.assertRaisesMessage(ValidationError, 'conciliar'):
            _merge_insumo_into_target(self.source, self.target, acted_by=self.actor)
        self.source.refresh_from_db();self.alias.refresh_from_db();old.refresh_from_db()
        self.assertTrue(self.source.activo)
        self.assertEqual(self.alias.insumo_id, self.source.pk)
        self.assertEqual(old.insumo_id, self.source.pk)
        self.assertEqual(list(MovimientoInventario.objects.order_by('pk').values()), snapshot)

    def test_origen_no_verificable_bloquea_sin_reescribir_la_clave(self):
        old = self.consumo(self.source)
        MovimientoInventario.objects.filter(pk=old.pk).update(source_hash='legado-no-verificable')
        with self.assertRaisesMessage(ValidationError, 'verificable'):
            _merge_insumo_into_target(self.source, self.target)
        old.refresh_from_db()
        self.assertEqual(old.source_hash, 'legado-no-verificable')
        self.assertEqual(old.insumo_id, self.source.pk)

    def test_exclusion_auditada_impide_invalidar_su_evidencia(self):
        old = self.consumo(self.source, trazabilidad={'exclusion_costeo': {'motivo': 'Conciliación'}})
        with self.assertRaisesMessage(ValidationError, 'exclusión'):
            _merge_insumo_into_target(self.source, self.target)
        old.refresh_from_db()
        self.assertIn('exclusion_costeo', old.trazabilidad)
        self.assertEqual(old.insumo_id, self.source.pk)

    def test_consumo_por_venta_sin_clave_reconstruible_requiere_conciliacion(self):
        MovimientoInventario.objects.create(tipo='CONSUMO', insumo=self.source, cantidad=1,
            referencia='VENTA-SERV-2026-05-10', source_hash='consumo-venta-origen')
        with self.assertRaises(ValidationError):
            _merge_insumo_into_target(self.source, self.target)
        self.source.refresh_from_db()
        self.assertTrue(self.source.activo)

    def test_entrada_conserva_clave_y_evidencia_ajenas_a_bom(self):
        mov = MovimientoInventario.objects.create(tipo='ENTRADA', insumo=self.source, cantidad=5,
            referencia='FACTURA-1', source_hash='compra-original', trazabilidad={'factura': 'F1'})
        _merge_insumo_into_target(self.source, self.target)
        mov.refresh_from_db()
        self.assertEqual((mov.insumo_id, mov.source_hash, mov.trazabilidad),
                         (self.target.pk, 'compra-original', {'factura': 'F1'}))

    def test_formularios_individuales_muestran_conflicto_sin_error500(self):
        self.consumo(self.source);self.consumo(self.target)
        for name, extra in [('inventario:aliases_catalog', {'action': 'resolve_duplicate_insumo'}),
                            ('maestros:insumo_resolve_duplicate', {})]:
            with self.subTest(name=name):
                response = self.client.post(reverse(name), {**extra,
                    'source_insumo_id': self.source.pk, 'target_insumo_id': self.target.pk})
                self.assertEqual(response.status_code, 302)
                self.assertTrue(any('conciliar' in str(m) for m in get_messages(response.wsgi_request)))
                self.source.refresh_from_db()
                self.assertTrue(self.source.activo)

    def test_conflicto_en_grupo_revierte_tambien_el_primer_miembro(self):
        first = Insumo.objects.create(nombre='Pan Lotus', unidad_base=self.unidad)
        self.consumo(self.source, '900')
        self.consumo(first);self.consumo(self.target)
        before = list(MovimientoInventario.objects.order_by('pk').values())
        for name, extra in [('inventario:aliases_catalog', {'action': 'resolve_duplicate_insumo_group'}),
                            ('maestros:insumo_resolve_duplicate_group', {})]:
            with self.subTest(name=name):
                response = self.client.post(reverse(name), {**extra,
                    'duplicate_key': self.source.nombre_normalizado, 'target_insumo_id': self.target.pk})
                self.assertEqual(response.status_code, 302)
                self.assertTrue(any('conciliar' in str(m) for m in get_messages(response.wsgi_request)))
                self.assertEqual(list(MovimientoInventario.objects.order_by('pk').values()), before)
                self.assertEqual(Insumo.objects.filter(pk__in=[first.pk,self.source.pk],activo=True).count(),2)


class FusionConsumosConcurrencyTests(TransactionTestCase):
    def test_generador_espera_la_fusion_antes_de_leer_el_bom(self):
        def generar_en_otra_conexion():
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET lock_timeout = '100ms'")
                with patch.object(ConsumoInsumoAutoService, '_build_production_consumption') as build:
                    with self.assertRaises(OperationalError):
                        ConsumoInsumoAutoService().generar_consumos_produccion(date(2026,5,1),date(2026,5,1))
                    return build.called
            finally:
                connection.close()
        with transaction.atomic():
            bloquear_consumos_fusion()
            with ThreadPoolExecutor(max_workers=1) as pool:
                self.assertFalse(pool.submit(generar_en_otra_conexion).result(timeout=5))

    def test_cierre_mensual_espera_antes_de_reconstruir_consumos(self):
        def cerrar_en_otra_conexion():
            try:
                with connection.cursor() as cursor:cursor.execute("SET lock_timeout = '100ms'")
                with patch.object(ConsumoInsumoAuditService, '_produccion_por_receta') as build:
                    with self.assertRaises(OperationalError):
                        ConsumoInsumoAuditService().calcular_periodo(date(2026,5,1))
                    return build.called
            finally:connection.close()
        with transaction.atomic():
            bloquear_consumos_fusion()
            with ThreadPoolExecutor(max_workers=1) as pool:
                self.assertFalse(pool.submit(cerrar_en_otra_conexion).result(timeout=5))
