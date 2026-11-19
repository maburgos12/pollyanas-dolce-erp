from datetime import date
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

from django.test import SimpleTestCase, TestCase, TransactionTestCase
from django.core.management import call_command
from django.core.management.base import CommandError

from reportes.models import AreaPresupuesto, LineaPresupuestoMensual, RubroPresupuesto
from reportes.services_presupuesto_real import PresupuestoRealConsolidacionService


class CoberturaFinancieraTests(SimpleTestCase):
    def test_respaldo_rechaza_symlink_y_escape_del_almacen_privado(self):
        from reportes.management.commands.aplicar_ajustes_rentabilidad_octubre_2026 import ruta_respaldo, escribir_respaldo
        import json, stat
        with TemporaryDirectory() as carpeta:
            raiz = Path(carpeta).resolve() / 'repo'
            privado = raiz / 'storage/private/conciliaciones'
            privado.mkdir(parents=True, mode=0o700)
            externo = Path(carpeta).resolve() / 'externo'
            externo.mkdir(mode=0o700)
            with patch('pathlib.Path.cwd', return_value=raiz):
                self.assertEqual(ruta_respaldo(privado/'archivo.json'), privado/'archivo.json')
                with self.assertRaisesMessage(CommandError, 'escapes'):
                    ruta_respaldo(privado/'../../../../fuera.json')
                (privado/'enlace').symlink_to(externo, target_is_directory=True)
                with self.assertRaisesMessage(CommandError, 'simbólicos'):
                    ruta_respaldo(privado/'enlace/archivo.json')
                ruta = ruta_respaldo(privado/'archivo.json')
                escribir_respaldo(ruta, {'cinco_lineas': [1, 2, 3, 4, 5], 'importe': D('17084.67')})
                self.assertEqual(stat.S_IMODE(ruta.stat().st_mode), 0o600)
                self.assertEqual(json.loads(ruta.read_text())['importe'], '17084.67')

    def regla(self, tipo='OBLIGACION_GASTO'):
        return SimpleNamespace(pk=7, rubro_id=42, tipo_fuente=tipo,
            filtros={'cobertura_mensual_documentada': '2026-10'}, signo=1)

    @patch('reportes.services_rentabilidad_gastos.leer_gastos_mensuales')
    def test_mensualiza_una_sola_vez_y_excluye_otra_regla(self, lector):
        lector.return_value = {'filas': [
            dict(regla_id=7, estado='COMPLETO', monto_mensual=D('1224.76')),
            dict(regla_id=8, estado='COMPLETO', monto_mensual=D('999'))], 'pendientes': []}
        servicio = PresupuestoRealConsolidacionService()
        monto, valido = servicio._monto_regla(self.regla(), date(2026, 10, 1), {'obligacion_gasto': {}})
        self.assertEqual((monto, valido), (D('1224.76'), True))
        lector.assert_called_once_with(date(2026, 10, 1), incluir_indirectos_produccion=True)

    @patch('reportes.services_rentabilidad_gastos.leer_gastos_mensuales')
    def test_pendiente_del_mismo_destino_impide_publicar_importe_parcial(self, lector):
        lector.return_value = {'filas': [dict(regla_id=7, estado='COMPLETO', monto_mensual=D('10'))],
            'pendientes': [dict(regla_id=7, detalle='Sin cobertura') ]}
        with self.assertRaisesMessage(ValueError, 'incompleta'):
            PresupuestoRealConsolidacionService()._monto_regla(self.regla(), date(2026, 10, 1), {'obligacion_gasto': {}})

    @patch('reportes.services_rentabilidad_gastos.leer_gastos_mensuales')
    def test_signo_del_lector_no_se_aplica_dos_veces(self, lector):
        lector.return_value = {'filas': [dict(regla_id=7, estado='COMPLETO', monto_mensual=D('-10'))], 'pendientes': []}
        regla = self.regla()
        regla.signo = -1
        monto, valido = PresupuestoRealConsolidacionService()._monto_regla(regla, date(2026, 10, 1), {})
        self.assertTrue(valido)
        self.assertEqual(monto * regla.signo, D('-10'))

    @patch('reportes.services_rentabilidad_gastos.leer_gastos_mensuales')
    def test_cobertura_autorizada_no_cambia_meses_distintos(self, lector):
        self.assertEqual(PresupuestoRealConsolidacionService()._monto_regla(
            self.regla(), date(2026, 9, 1), {'obligacion_gasto': {}}), (D('0'), False))
        lector.assert_not_called()

    @patch.object(PresupuestoRealConsolidacionService, '_monto_nomina', return_value=(D('15'), True))
    @patch.object(PresupuestoRealConsolidacionService, '_build_nomina_index')
    def test_regla_rrhh_acotada_no_consulta_nomina_fuera_del_mes(self, indice, monto):
        regla = SimpleNamespace(tipo_fuente='NOMINA', rubro=SimpleNamespace(area=SimpleNamespace(codigo='administracion')),
            filtros={'desde': '2026-10', 'hasta': '2026-10'})
        self.assertEqual(PresupuestoRealConsolidacionService()._monto_regla(
            regla, date(2026, 9, 1), {}), (D('0'), False))
        indice.assert_not_called()


class ConsolidacionAcotadaTests(TestCase):
    def test_licencia_anual_usa_fuente_real_y_reserva_residuo_sin_doblar_obligacion(self):
        from django.core.files.base import ContentFile
        from django.core.files.storage import default_storage
        from django.test import override_settings
        from reportes.models import CategoriaGasto, CentroCosto, GastoOperativoMensual, ObligacionGasto, ReglaFuenteRubro as R
        with TemporaryDirectory() as media, override_settings(MEDIA_ROOT=media):
            archivo = default_storage.save('recibos/licencia.pdf', ContentFile(b'soporte sintetico'))
            area = AreaPresupuesto.objects.create(codigo='administracion', nombre='Administración')
            cat = CategoriaGasto.objects.create(codigo='SISTEMAS_CORP', nombre='Licencia', capa_objetivo='EMPRESA')
            centro = CentroCosto.objects.create(codigo='CORP', nombre='Corporativo', tipo='CORPORATIVO')
            rubro = RubroPresupuesto.objects.create(area=area, concepto='Licencia', tipo='EGRESO')
            linea = LineaPresupuestoMensual.objects.create(rubro=rubro, periodo=date(2026, 10, 1), monto_presupuesto=0)
            gasto = GastoOperativoMensual.objects.create(periodo=date(2026, 3, 1), categoria_gasto=cat,
                centro_costo=centro, monto=D('14697.20'), cobertura_mes_inicio=date(2026, 3, 1),
                cobertura_mes_fin=date(2027, 2, 1), archivo_soporte=archivo)
            ObligacionGasto.objects.create(origen='VARIABLE', area=area, rubro=rubro, centro_costo=centro,
                categoria_gasto=cat, concepto='Licencia', periodo=date(2026, 3, 1),
                fecha_gasto=date(2026, 3, 1), fecha_vencimiento=date(2026, 3, 1),
                monto_reconocido=D('14697.20'), gasto_operativo=gasto, archivo_soporte=archivo)
            regla = R.objects.create(rubro=rubro, tipo_fuente='OBLIGACION_GASTO',
                filtros={'cobertura_mensual_documentada': '2026-10', 'desde': '2026-09'})
            servicio = PresupuestoRealConsolidacionService()
            salida = servicio.consolidar(periodo=date(2026, 10, 1), rubro_ids=[rubro.pk])
            self.assertEqual(salida.errores, [])
            linea.refresh_from_db()
            self.assertEqual(linea.monto_real, D('1224.76'))
            cat.capa_objetivo = 'FABRICACION'
            cat.bucket = 'INDIRECTO_PROD'
            cat.save(update_fields=['capa_objetivo', 'bucket'])
            from reportes.services_rentabilidad_gastos import leer_gastos_mensuales
            mensual = leer_gastos_mensuales(date(2026, 10, 1))
            self.assertEqual([f['monto_mensual'] for f in mensual['filas'] if f['regla_id'] == regla.pk], [D('1224.76')])
            regla.filtros['cobertura_mensual_documentada'] = '2027-02'
            monto, valido = servicio._monto_regla(regla, date(2027, 2, 1), {})
            self.assertEqual((monto, valido), (D('1224.84'), True))

    def test_lista_vacia_no_barre_otros_rubros(self):
        area = AreaPresupuesto.objects.create(codigo='administracion', nombre='Administración')
        rubro = RubroPresupuesto.objects.create(area=area, concepto='No autorizado', tipo='EGRESO')
        LineaPresupuestoMensual.objects.create(rubro=rubro, periodo=date(2026, 10, 1), monto_presupuesto=D('1'))
        summary = PresupuestoRealConsolidacionService().consolidar(periodo=date(2026, 10, 1), rubro_ids=[])
        self.assertEqual(summary.sin_regla, 0)


class AplicacionAcotadaTests(TransactionTestCase):
    def setUp(self):
        from reportes.models import CategoriaGasto, CentroCosto, GastoOperativoMensual, EmpresaResultadoMensual, ReglaFuenteRubro as R
        from rrhh.models import Empleado, NominaLinea, NominaPeriodo, NominaConceptoLinea
        from sat_client.models import CfdiDescargado
        from django.utils import timezone
        self.area = AreaPresupuesto.objects.create(codigo='administracion', nombre='Administración')
        cat = CategoriaGasto.objects.create(codigo='INDIRECTO_PROD', nombre='Certificado', capa_objetivo='EMPRESA')
        centro = CentroCosto.objects.create(codigo='CORP', nombre='Corporativo', tipo='CORPORATIVO')
        for gid, cid in zip([2130, 2131, 2419, 2420, 2421, 2422], [794, 8884, 1993, 2002, 2047, 2207]):
            cfdi = CfdiDescargado.objects.create(pk=cid, uuid=f'00000000-0000-0000-0000-{gid:012}',
                rfc_emisor='AAA010101AAA', rfc_receptor='BBB010101BBB', subtotal=100, total=100,
                tipo_comprobante='I', tipo_cfdi='recibido', fecha_emision=timezone.now())
            GastoOperativoMensual.objects.create(pk=gid, periodo=date(2026, 3, 1),
                categoria_gasto=cat, centro_costo=centro, monto=100, cfdi_soporte=cfdi)
        for lid, rid, amount in [(20404, 1701, None), (20532, 1699, None),
                                 (20522, 1700, None), (8826, 738, D('81848.84')), (8874, 742, None), (8800, 99, D('9'))]:
            rubro = RubroPresupuesto.objects.create(pk=rid, area=self.area, concepto=f'Rubro {rid}', tipo='EGRESO')
            LineaPresupuestoMensual.objects.create(pk=lid, rubro=rubro, periodo=date(2026, 10, 1),
                monto_presupuesto=0, monto_real=amount, fuente_real='AUTO:NOMINA' if lid == 8826 else '')
        for pk, rid, tipo in [(16542, 1701, 'GASTO_OPERATIVO'), (16350, 1699, 'OBLIGACION_GASTO'),
                               (16351, 1700, 'OBLIGACION_GASTO'), (15904, 738, 'NOMINA')]:
            R.objects.create(pk=pk, rubro_id=rid, tipo_fuente=tipo,
                categoria_gasto=cat if pk == 16542 else None,
                filtros={'departamento': 'ADMINISTRACION'} if pk == 15904 else {})
        empleado = Empleado.objects.create(pk=33, codigo='TEST-RRHH', nombre='Sintético', departamento='RRHH')
        for pk, period, end, bonos, extra in [(1409, 22, 15, '1372.36', '272.36'), (1476, 24, 31, '1190.79', '90.79')]:
            periodo = NominaPeriodo.objects.create(pk=period, fecha_inicio=date(2026, 10, 1 if end == 15 else 16),
                fecha_fin=date(2026, 10, end), estatus='PAGADA')
            linea = NominaLinea.objects.create(pk=pk, periodo=periodo, empleado=empleado,
                salario_base=D('5519.88'), bonos=D(bonos), departamento_snapshot='RRHH')
            for codigo, importe in [('1', '5519.88'), ('14', '550'), ('15', '550'), ('4', extra), ('32', '710')]:
                NominaConceptoLinea.objects.create(linea=linea, tipo='PERCEPCION', codigo_concepto=codigo,
                    nombre=f'Concepto {codigo}', importe=D(importe))
        EmpresaResultadoMensual.objects.create(pk=10, periodo=date(2026, 10, 1), utilidad_operativa_total=D('1285478.39'))
        from reportes.models import PresupuestoResumenMensual
        PresupuestoResumenMensual.objects.create(pk=10, period=date(2026, 10, 1), tipo='FUENTE',
            fuente_nombre='PRESUPUESTO_VS_REAL', total_budget=D('2000000'), total_actual=D('1285478.39'),
            metadata={'rows': [dict(concept='utilidad_bruta', budget='3000000', actual='2031513.96', type='INGRESO'),
                               dict(concept='utilidad_operativa', budget='2000000', actual='1285478.39', type='INGRESO')]})
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.backup = str(Path(self.tmp.name).resolve() / 'antes.json')

    def monto(self, regla, periodo, indices):
        if regla.pk in {16542, 16350, 16351}:
            return {16542: D('586.64'), 16350: D('1224.76'), 16351: D('250.36')}[regla.pk], True
        if regla.tipo_fuente == 'NOMINA_CONCEPTO':
            return D('1420'), True
        return (D('13602.91') if regla.filtros.get('departamento') == 'RRHH' else D('81848.84')), True

    def totales(self):
        target = sum((r.monto_real or D(0) for r in LineaPresupuestoMensual.objects.filter(
            pk__in=[20404, 20532, 20522, 8826, 8874])), D(0))
        central = D('600079.66') + target - D('81848.84')
        totals = dict(ventas_netas=D('3648207.58'), costo_materia_prima=D('1566669.92'),
            costo_reventa=D('50191.39'), merma_registrada=D('11656.98'),
            costo_variable_total=D('1628518.29'), gasto_fijo_total=D('554984.76'),
            utilidad_operativa=D('1464704.53'), central=central, empresa=D('1464704.53')-central)
        return totals, dict(pendientes=[], resultado={'centrales': [], 'fuente': {'rows': 1}}), 11

    def ejecutar(self, apply=False, restore=False):
        with patch('reportes.management.commands.aplicar_ajustes_rentabilidad_octubre_2026.totales_actuales', self.totales), \
             patch.object(PresupuestoRealConsolidacionService, '_monto_regla', self.monto):
            output = StringIO()
            call_command('aplicar_ajustes_rentabilidad_octubre_2026', apply=apply,
                backup_file=self.backup, restore_file=self.backup if restore else None, stdout=output)
            return output.getvalue()

    def test_preview_no_escribe_y_aplicacion_es_idempotente_sin_tocar_otras_lineas(self):
        from reportes.models import EmpresaResultadoMensual, ReglaFuenteRubro as R
        from core.models import AuditLog
        before = list(LineaPresupuestoMensual.objects.order_by('pk').values())
        rules = R.objects.count()
        self.assertIn('PREVIEW', self.ejecutar())
        self.assertEqual(list(LineaPresupuestoMensual.objects.order_by('pk').values()), before)
        self.assertFalse(Path(self.backup).exists())
        self.assertIn('APLICADO', self.ejecutar(apply=True))
        self.assertEqual(R.objects.count(), rules+2)
        self.assertEqual(LineaPresupuestoMensual.objects.get(pk=8800).monto_real, D('9'))
        snap = EmpresaResultadoMensual.objects.get(pk=10)
        self.assertEqual(snap.utilidad_operativa_total, D('847540.20'))
        self.assertEqual(snap.margen_bruto_total, D('2019689.29'))
        self.assertEqual(snap.gasto_corporativo_total, D('617164.33'))
        self.assertEqual(snap.metadata['merma_registrada'], '11656.98')
        from reportes.models import PresupuestoResumenMensual
        resumen = PresupuestoResumenMensual.objects.get(pk=10)
        self.assertEqual(resumen.total_actual, D('847540.20'))
        self.assertEqual(resumen.total_budget, D('2000000'))
        self.assertEqual([r['budget'] for r in resumen.metadata['rows']], ['3000000', '2000000'])
        self.assertIn('YA_APLICADO', self.ejecutar(apply=True))
        self.assertEqual(R.objects.count(), rules+2)
        self.assertEqual(AuditLog.objects.filter(action='CONCILIAR').count(), 1)

    def test_concepto_ajeno_y_otro_periodo_conservan_importes_y_metadata(self):
        from reportes.models import PresupuestoResumenMensual
        resumen = PresupuestoResumenMensual.objects.get(pk=10)
        ajeno = dict(concept='capex_financiamiento', budget='12345.67', actual='9876.54',
            variance='-2469.13', variance_pct='-20', tone='warning', type='EGRESO', evidencia='original')
        resumen.metadata['rows'].append(ajeno)
        resumen.save(update_fields=['metadata'])
        otro = PresupuestoResumenMensual.objects.create(period=date(2026, 9, 1), tipo='FUENTE',
            fuente_nombre='PRESUPUESTO_VS_REAL', total_actual=D('123.45'), metadata={'rows': [ajeno]})
        before = PresupuestoResumenMensual.objects.filter(pk=otro.pk).values().get()
        self.ejecutar(apply=True)
        resumen.refresh_from_db()
        self.assertEqual(resumen.metadata['rows'][-1], ajeno)
        self.assertEqual(PresupuestoResumenMensual.objects.filter(pk=otro.pk).values().get(), before)

    def test_cambio_de_fuente_aborta_sin_crear_reglas(self):
        from reportes.models import ReglaFuenteRubro as R
        before = R.objects.count()
        LineaPresupuestoMensual.objects.filter(pk=8826).update(monto_real=D('81848.85'))
        with self.assertRaisesMessage(CommandError, 'Cambió una línea'):
            self.ejecutar(apply=True)
        self.assertEqual(R.objects.count(), before)
        self.assertFalse(Path(self.backup).exists())

    @patch('reportes.management.commands.aplicar_ajustes_rentabilidad_octubre_2026.bump_cache_scopes')
    def test_invalida_dashboard_solo_una_vez_al_confirmar_commit(self, invalidar):
        self.ejecutar()
        invalidar.assert_not_called()
        self.ejecutar(apply=True)
        invalidar.assert_called_once_with('dashboard')
        self.ejecutar(apply=True)
        invalidar.assert_called_once_with('dashboard')

    def test_fallo_despues_de_consolidar_revierte_lineas_reglas_y_snapshot(self):
        from reportes.models import EmpresaResultadoMensual, ReglaFuenteRubro as R
        from core.models import AuditLog
        antes = list(LineaPresupuestoMensual.objects.order_by('pk').values())
        reglas = list(R.objects.order_by('pk').values())
        with patch('reportes.management.commands.aplicar_ajustes_rentabilidad_octubre_2026.bump_cache_scopes') as invalidar, patch('reportes.management.commands.aplicar_ajustes_rentabilidad_octubre_2026.actualizar_snapshot',
                   side_effect=CommandError('Fallo posterior al DML')):
            with self.assertRaisesMessage(CommandError, 'posterior al DML'):
                self.ejecutar(apply=True)
        invalidar.assert_not_called()
        self.assertEqual(list(LineaPresupuestoMensual.objects.order_by('pk').values()), antes)
        self.assertEqual(list(R.objects.order_by('pk').values()), reglas)
        self.assertEqual(EmpresaResultadoMensual.objects.get(pk=10).utilidad_operativa_total, D('1285478.39'))
        self.assertFalse(AuditLog.objects.filter(action='CONCILIAR').exists())
        self.assertTrue(Path(self.backup).exists())

    def test_respaldo_restituye_contenido_completo_y_solo_retira_dos_reglas_creadas(self):
        from reportes.management.commands.aplicar_ajustes_rentabilidad_octubre_2026 import estado_destinos
        import json
        antes = json.loads(json.dumps(estado_destinos(), default=str))
        self.ejecutar(apply=True)
        self.assertIn('RESTAURACION_VERIFICADA', self.ejecutar(restore=True))
        self.assertIn('RESTAURADO_Y_VERIFICADO', self.ejecutar(apply=True, restore=True))
        self.assertEqual(json.loads(json.dumps(estado_destinos(), default=str)), antes)

    def test_restauracion_rechaza_modificacion_posterior_de_igual_importe(self):
        self.ejecutar(apply=True)
        linea = LineaPresupuestoMensual.objects.get(pk=8826)
        linea.metadata['nota_posterior'] = 'No sobrescribir'
        linea.save(update_fields=['metadata'])
        with self.assertRaisesMessage(CommandError, 'modificaciones posteriores'):
            self.ejecutar(apply=True, restore=True)
        linea.refresh_from_db()
        self.assertEqual(linea.metadata['nota_posterior'], 'No sobrescribir')

    def test_restauracion_rechaza_respaldo_y_recibo_reescritos(self):
        import json, hashlib
        self.ejecutar(apply=True)
        path = Path(self.backup)
        data = json.loads(path.read_text())
        data['lineas'][0]['monto_real'] = '999.00'
        path.write_text(json.dumps(data))
        receipt = path.with_suffix('.after.json')
        after = json.loads(receipt.read_text())
        after['backup_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        receipt.write_text(json.dumps(after))
        with self.assertRaisesMessage(CommandError, 'auditoría'):
            self.ejecutar(apply=True, restore=True)

    def test_ya_aplicado_detecta_concepto_modificado_aunque_no_cambie_monto(self):
        from rrhh.models import NominaConceptoLinea
        self.ejecutar(apply=True)
        NominaConceptoLinea.objects.filter(linea_id=1409, codigo_concepto='32').update(nombre='Concepto cambiado')
        with self.assertRaisesMessage(CommandError, 'fuentes, conceptos o reglas'):
            self.ejecutar(apply=True)

    def test_dos_sesiones_no_permiten_intercalar_escritura_de_concepto(self):
        from concurrent.futures import ThreadPoolExecutor
        from django.db import connections, transaction, OperationalError
        from reportes.management.commands.aplicar_ajustes_rentabilidad_octubre_2026 import bloquear_fuentes
        def competir(insertar=False):
            conn = connections['default']
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SET lock_timeout='200ms'")
                    if not insertar:
                        cursor.execute("UPDATE rrhh_nominaconceptolinea SET importe=importe+1 WHERE linea_id=1409 AND codigo_concepto='32'")
                    else:
                        from rrhh.models import NominaConceptoLinea
                        NominaConceptoLinea.objects.create(linea_id=1409, codigo_concepto='77',
                            tipo='PERCEPCION', nombre='Concurrente', importe=1)
                return 'ESCRITURA_INTERCALADA'
            except OperationalError as exc:
                return getattr(exc.__cause__, 'sqlstate', None) or getattr(exc.__cause__, 'pgcode', None)
            finally:
                conn.close()
        with transaction.atomic(), ThreadPoolExecutor(max_workers=1) as pool:
            bloquear_fuentes()
            self.assertEqual(pool.submit(competir).result(timeout=5), '55P03')
            self.assertEqual(pool.submit(competir, True).result(timeout=5), '55P03')

    def test_insercion_confirmada_antes_del_bloqueo_de_fuentes_aborta(self):
        from concurrent.futures import ThreadPoolExecutor
        from django.db import connections
        from rrhh.models import NominaConceptoLinea
        from reportes.management.commands.aplicar_ajustes_rentabilidad_octubre_2026 import bloquear_fuentes
        def insertar():
            try:
                NominaConceptoLinea.objects.create(linea_id=1409, codigo_concepto='77',
                    tipo='PERCEPCION', nombre='Confirmado antes del bloqueo', importe=1)
            finally:
                connections['default'].close()
        def intercalar():
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(insertar).result(timeout=5)
            return bloquear_fuentes()
        with patch('reportes.management.commands.aplicar_ajustes_rentabilidad_octubre_2026.bloquear_fuentes', side_effect=intercalar):
            with self.assertRaisesMessage(CommandError, 'composición'):
                self.ejecutar(apply=True)
        self.assertTrue(NominaConceptoLinea.objects.filter(linea_id=1409,codigo_concepto='77').exists())
        self.assertFalse(Path(self.backup).exists())
        self.assertEqual(LineaPresupuestoMensual.objects.get(pk=8826).monto_real,D('81848.84'))
