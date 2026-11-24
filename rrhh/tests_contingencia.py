from datetime import date, datetime, time, timedelta
from decimal import Decimal
from io import StringIO
import json

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.core.management import call_command
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from rrhh import models
from rrhh.models import Empleado, IncidenciaAsistencia as I, PermisoSalida, PrenominaCorte, PrenominaMovimiento
from rrhh.services_asistencia_reglas import evaluar_dia_empleado, _faltas_vigentes
from rrhh.services_prenomina import recalcular_corte_prenomina


class ContingenciaTests(TestCase):
    def setUp(self):
        self.dg = get_user_model().objects.create_superuser('dg-contingencia', '', 'x')
        self.empleado = Empleado.objects.create(codigo='CONT-1', nombre='Prueba contingencia', fecha_ingreso=date(2026, 1, 1), salario_diario=Decimal('400'))
        self.dia = date(2026, 11, 21)
        self.datos = dict(clave='CIERRE-20261121', fecha_inicio='2026-11-21', fecha_fin='2026-11-21', motivo='seguridad', descripcion='Suspensión total de actividad por contingencia', evidencia=['acta:20261121'], alcance='empresa', tratamiento_salarial='conservar')

    def servicio(self):
        from rrhh import services_contingencia
        return services_contingencia

    def aprobar(self, datos=None):
        s = self.servicio()
        p = s.previsualizar_contingencia(datos or self.datos, self.dg)
        return s.aplicar_contingencia(datos or self.datos, self.dg, digest=p['digest'])

    def corte(self):
        return PrenominaCorte.objects.create(fecha_inicio=self.dia, fecha_fin=self.dia, fecha_corte=self.dia)

    def test_permiso_personal_con_goce_no_crea_movimiento_falta(self):
        permiso = PermisoSalida.objects.create(empleado=self.empleado, tipo='permiso_dia', estado='aprobado', fecha_inicio=timezone.make_aware(datetime.combine(self.dia, time.min)), fecha_fin=timezone.make_aware(datetime.combine(self.dia, time.max)), goce_sueldo=True, motivo='Autorizado')
        I.objects.create(empleado=self.empleado, fecha=self.dia, tipo='falta', estado='conciliado', permiso=permiso, goce_sueldo=True)
        corte = recalcular_corte_prenomina(self.corte())
        self.assertEqual(corte.resumenes.get(empleado=self.empleado).faltas, 0)
        self.assertFalse(corte.movimientos.filter(tipo_movimiento_erp='FALTA').exists())

    def test_falta_real_conciliada_sigue_generando_movimiento(self):
        I.objects.create(empleado=self.empleado, fecha=self.dia, tipo='falta', estado='conciliado', editado_manual=True)
        corte = recalcular_corte_prenomina(self.corte())
        self.assertEqual(corte.resumenes.get(empleado=self.empleado).faltas, 1)
        self.assertTrue(corte.movimientos.filter(tipo_movimiento_erp='FALTA').exists())

    def test_aprobar_resuelve_falta_sin_crear_asistencia_ni_cambiar_salario(self):
        evaluar_dia_empleado(self.empleado, self.dia)
        permiso = self.aprobar()
        incidencia = I.objects.get(empleado=self.empleado, fecha=self.dia, tipo='falta')
        self.assertEqual(incidencia.estado, 'resuelto')
        self.assertTrue(incidencia.bitacora.exists())
        evaluar_dia_empleado(self.empleado, self.dia)
        self.assertEqual(_faltas_vigentes(self.empleado, self.dia), 0)
        self.assertFalse(self.empleado.asistencias.exists())
        self.empleado.refresh_from_db()
        self.assertEqual(self.empleado.salario_diario, Decimal('400'))
        self.assertEqual(permiso.beneficiarios.count(), 1)
        corte = recalcular_corte_prenomina(self.corte())
        self.assertFalse(corte.movimientos.filter(tipo_movimiento_erp='FALTA').exists())

    def test_dryrun_sin_escrituras_y_digest_obsoleto(self):
        s = self.servicio()
        with CaptureQueriesContext(connection) as queries:
            p = s.previsualizar_contingencia(self.datos, self.dg)
        self.assertFalse(any(q['sql'].lstrip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')) for q in queries))
        I.objects.create(empleado=self.empleado, fecha=self.dia, tipo='falta')
        with self.assertRaises(ValidationError):
            s.aplicar_contingencia(self.datos, self.dg, digest=p['digest'])
        self.assertFalse(models.PermisoContingencia.objects.exists())

    def test_idempotencia_snapshot_y_limites(self):
        p = self.aprobar()
        Empleado.objects.create(codigo='NUEVO', nombre='Nuevo', fecha_ingreso=date(2026, 11, 22))
        self.assertEqual(self.aprobar().pk, p.pk)
        self.assertEqual(p.beneficiarios.count(), 1)
        for datos in (dict(self.datos, fecha_fin='2026-12-22'), dict(self.datos, alcance='sucursal'), dict(self.datos, empleados=[99999], alcance='empleados')):
            with self.assertRaises(ValidationError):
                self.servicio().previsualizar_contingencia(datos, self.dg)

    def test_actor_inactivo_y_no_autorizado(self):
        user = get_user_model().objects.create_user('sin-permiso')
        for actor in (user, None):
            with self.assertRaises(PermissionDenied):
                self.servicio().previsualizar_contingencia(self.datos, actor)
        self.dg.is_active = False
        self.dg.save()
        with self.assertRaises(PermissionDenied):
            self.servicio().previsualizar_contingencia(self.datos, self.dg)

    def test_conflicto_manual_y_corte_cerrado_bloquean(self):
        i = I.objects.create(empleado=self.empleado, fecha=self.dia, tipo='falta', editado_manual=True)
        p = self.servicio().previsualizar_contingencia(self.datos, self.dg)
        self.assertTrue(p['conflictos'])
        with self.assertRaises(ValidationError):
            self.servicio().aplicar_contingencia(self.datos, self.dg, digest=p['digest'])
        i.delete()
        corte = self.corte()
        corte.estado = 'EXPORTADO'
        corte.save()
        p = self.servicio().previsualizar_contingencia(self.datos, self.dg)
        self.assertTrue(p['conflictos'])
        with self.assertRaises(ValidationError):
            self.servicio().aplicar_contingencia(self.datos, self.dg, digest=p['digest'])

    def test_contexto_fuera_borrador_revocado_y_fecha_ingreso(self):
        from rrhh.services_asistencia_contexto import cargar_contexto_asistencia
        s = self.servicio()
        p = s.previsualizar_contingencia(self.datos, self.dg, accion='borrador')
        permiso = s.aplicar_contingencia(self.datos, self.dg, accion='borrador', digest=p['digest'])
        def clasificar(dia):
            return cargar_contexto_asistencia(empleados=[self.empleado], fecha_inicio=dia, fecha_fin=dia).clasificar(self.empleado, dia)
        self.assertTrue(clasificar(self.dia).falta_penalizable)
        permiso = self.aprobar()
        self.assertEqual(clasificar(self.dia).codigo, 'permiso_contingencia')
        self.assertFalse(clasificar(self.dia).falta_penalizable)
        self.assertTrue(clasificar(self.dia-timedelta(days=1)).falta_penalizable)
        p = s.previsualizar_contingencia(self.datos, self.dg, accion='revocar')
        s.aplicar_contingencia(self.datos, self.dg, accion='revocar', digest=p['digest'])
        self.assertTrue(clasificar(self.dia).falta_penalizable)
        self.assertEqual(I.objects.get(empleado=self.empleado, fecha=self.dia, tipo='falta').estado, 'pendiente')

    def test_repara_alerta_posterior_conserva_faltas_reales(self):
        for dia in (date(2026, 11, 19), date(2026, 11, 20), self.dia):
            evaluar_dia_empleado(self.empleado, dia)
        evaluar_dia_empleado(self.empleado, date(2026, 11, 23))
        self.assertTrue(I.objects.filter(tipo='aviso_baja_faltas').exists())
        self.aprobar()
        self.assertEqual(_faltas_vigentes(self.empleado, date(2026, 11, 23)), 3)
        self.assertEqual(I.objects.get(tipo='baja_faltas').estado, 'resuelto')
        self.assertEqual(I.objects.filter(tipo='falta', fecha__lt=self.dia, estado='pendiente').count(), 2)

    def test_bono_ventas_neutral_no_inventa_asistencia_ni_borra_manuales(self):
        from core.models import Sucursal
        from bonos_ventas.models import ConfigBonoVentasPeriodo, BonoVentasEmpleado
        sucursal = Sucursal.objects.create(nombre='Contingencia ventas')
        cfg = ConfigBonoVentasPeriodo.objects.create(mes=11, anio=2026, fecha_inicio=self.dia, fecha_fin=self.dia, dias_laborables=1, cancela_por_asistencia=True, limite_asistencia_cancelacion=1)
        bono = BonoVentasEmpleado.objects.create(periodo=cfg,empleado=self.empleado,sucursal=sucursal,bono_extra=17,ajuste_positivo=9,ajuste_negativo=3)
        self.aprobar()
        bono.refresh_from_db()
        self.assertFalse(bono.cancela_bono)
        self.assertEqual((bono.dias_asistencia,bono.dias_trabajados,bono.dias_uniforme,bono.dias_puntualidad),(0,0,0,0))
        self.assertEqual((bono.bono_extra,bono.ajuste_positivo,bono.ajuste_negativo),(17,9,3))

    def test_fuente_snapshot_no_se_edita_ni_elimina(self):
        permiso = self.aprobar()
        permiso.descripcion = 'Historia alterada'
        with self.assertRaises(ValidationError):
            permiso.save()
        with self.assertRaises(ValidationError):
            permiso.eventos.first().delete()

    def test_comando_preview_por_defecto_y_apply_exige_digest(self):
        import tempfile
        from django.core.management.base import CommandError
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json') as f:
            json.dump(self.datos,f)
            f.flush()
            output = StringIO()
            call_command('permiso_contingencia', archivo=f.name, actor=self.dg.pk, stdout=output)
            self.assertFalse(models.PermisoContingencia.objects.exists())
            preview = json.loads(output.getvalue())
            with self.assertRaises(CommandError):
                call_command('permiso_contingencia', archivo=f.name, actor=self.dg.pk, aplicar=True)
            call_command('permiso_contingencia', archivo=f.name, actor=self.dg.pk, aplicar=True,digest=preview['digest'],stdout=StringIO())
            self.assertEqual(models.PermisoContingencia.objects.count(),1)

    def test_produccion_periodo_neutral_sin_asistencia_uniforme_ni_produccion(self):
        from bonos_produccion.models import ConfigBonoPeriodo, BonoProduccionEmpleado
        cfg = ConfigBonoPeriodo.objects.create(mes=11,anio=2026,fecha_inicio=self.dia,fecha_fin=self.dia,dias_laborables=1)
        regla = cfg.get_regla_area('HORNOS')
        regla.limite_uniforme = regla.limite_puntualidad = regla.limite_produccion = 0
        regla.save()
        bono = BonoProduccionEmpleado.objects.create(periodo=cfg,empleado=self.empleado,area='HORNOS',bono_extra=17,ajuste_positivo=9,ajuste_negativo=3)
        self.aprobar()
        bono.refresh_from_db()
        self.assertEqual(bono.faltas_rrhh,0)
        self.assertTrue(bono.pasa_uniforme)
        self.assertTrue(bono.pasa_puntualidad)
        self.assertEqual((bono.dias_asistencia,bono.dias_trabajados,bono.dias_uniforme,bono.dias_produccion),(0,0,0,0))
        self.assertEqual((bono.bono_extra,bono.ajuste_positivo,bono.ajuste_negativo),(17,9,3))

    def test_prenomina_abierta_actualiza_solo_resumen_y_falta_generada_afectados(self):
        i = I.objects.create(empleado=self.empleado,fecha=self.dia,tipo='falta',estado='conciliado')
        corte = recalcular_corte_prenomina(self.corte())
        self.assertEqual(corte.resumenes.get(empleado=self.empleado).faltas,1)
        self.assertTrue(corte.movimientos.filter(fuente_id=str(i.pk)).exists())
        otro = Empleado.objects.create(codigo='NO-AFECTADO',nombre='Fuera del alcance',fecha_ingreso=date(2026,1,1))
        datos = dict(self.datos,alcance='empleados',empleados=[self.empleado.pk])
        self.aprobar(datos)
        self.assertFalse(corte.movimientos.filter(fuente_id=str(i.pk)).exists())
        self.assertEqual(corte.resumenes.get(empleado=self.empleado).faltas,0)

    def test_nomina_cerrada_bloquea(self):
        from rrhh.models import NominaPeriodo, NominaLinea
        nomina = NominaPeriodo.objects.create(fecha_inicio=self.dia,fecha_fin=self.dia,estatus='CERRADA')
        NominaLinea.objects.create(periodo=nomina,empleado=self.empleado)
        p = self.servicio().previsualizar_contingencia(self.datos,self.dg)
        self.assertTrue(p['conflictos'])
        with self.assertRaises(ValidationError):
            self.servicio().aplicar_contingencia(self.datos,self.dg,digest=p['digest'])

    def test_permiso_personal_retroactivo_repara_alertas_posteriores(self):
        from rrhh.services_permisos import reevaluar_asistencia_permiso
        for dia in (date(2026,11,19),date(2026,11,20),self.dia,date(2026,11,23)):
            evaluar_dia_empleado(self.empleado,dia)
        permiso = PermisoSalida.objects.create(empleado=self.empleado,tipo='permiso_dia',estado='aprobado',goce_sueldo=True,fecha_inicio=timezone.make_aware(datetime.combine(self.dia,time.min)),fecha_fin=timezone.make_aware(datetime.combine(self.dia,time.max)),motivo='Autorizado')
        with self.captureOnCommitCallbacks(execute=True):
            reevaluar_asistencia_permiso(permiso)
        self.assertEqual(I.objects.get(tipo='baja_faltas',fecha=date(2026,11,23)).estado,'resuelto')

    def test_dia_con_trabajo_preserva_integridad_y_no_neutraliza_retardos_anteriores(self):
        from rrhh.models import AsistenciaEmpleado, Turno
        turno = Turno.objects.create(nombre='Turno test contingencia',hora_entrada=time(8),hora_salida=time(16))
        a = AsistenciaEmpleado.objects.create(empleado=self.empleado,fecha=self.dia,turno=turno,entrada=timezone.make_aware(datetime.combine(self.dia,time(9))))
        falta = I.objects.create(empleado=self.empleado,fecha=self.dia,tipo='falta_retardos',estado='conciliado')
        self.aprobar()
        corte = recalcular_corte_prenomina(self.corte())
        self.assertTrue(corte.movimientos.filter(tipo_movimiento_erp='FALTA',fuente_id=str(falta.pk)).exists())
        a.refresh_from_db()
        self.assertEqual(a.turno_id,turno.pk)
        self.assertEqual(timezone.localtime(a.entrada).hour,9)

    def test_superposicion_neutral_es_union_y_descanso_no_es_neutral(self):
        from rrhh.services_cobertura_permisos import dias_neutrales_bono
        self.aprobar()
        self.aprobar(dict(self.datos,clave='OTRA-CONTINGENCIA'))
        self.assertEqual(dias_neutrales_bono(self.empleado,self.dia,self.dia),1)
        domingo = dict(self.datos,clave='DOMINGO',fecha_inicio='2026-11-22',fecha_fin='2026-11-22')
        self.aprobar(domingo)
        self.assertEqual(dias_neutrales_bono(self.empleado,date(2026,11,22),date(2026,11,22)),0)

    def test_scope_explicito_guarda_ids_y_excluye_fuera_de_intervalo(self):
        nuevo = Empleado.objects.create(codigo='FUTURO',nombre='Posterior',fecha_ingreso=self.dia+timedelta(days=1))
        otro = Empleado.objects.create(codigo='OTRO',nombre='Fuera de alcance',fecha_ingreso=date(2026,1,1))
        permiso = self.aprobar(dict(self.datos,alcance='empleados',empleados=[self.empleado.pk]))
        self.assertEqual(list(permiso.beneficiarios.values_list('empleado_id',flat=True)),[self.empleado.pk])
        from rrhh.services_cobertura_permisos import contingencia_en_fecha
        self.assertIsNone(contingencia_en_fecha(otro,self.dia))
        self.assertIsNone(contingencia_en_fecha(nuevo,self.dia))

    def test_revocacion_idempotente_conserva_fuentes_e_historia(self):
        permiso = self.aprobar()
        s = self.servicio()
        p = s.previsualizar_contingencia(self.datos,self.dg,accion='revocar')
        s.aplicar_contingencia(self.datos,self.dg,digest=p['digest'],accion='revocar')
        n = permiso.eventos.count()
        p = s.previsualizar_contingencia(self.datos,self.dg,accion='revocar')
        s.aplicar_contingencia(self.datos,self.dg,digest=p['digest'],accion='revocar')
        self.assertEqual(permiso.eventos.count(),n)
        self.assertEqual(permiso.beneficiarios.count(),1)

    def test_digest_detecta_cambio_en_historia_previa_y_alertas_fuera_de_cota(self):
        s = self.servicio()
        p = s.previsualizar_contingencia(self.datos,self.dg)
        I.objects.create(empleado=self.empleado,fecha=self.dia-timedelta(days=20),tipo='falta')
        with self.assertRaises(ValidationError):
            s.aplicar_contingencia(self.datos,self.dg,digest=p['digest'])

    def test_preview_alertas_coincide_con_aplicacion(self):
        for dia in (date(2026,11,19),date(2026,11,20),self.dia,date(2026,11,23)):
            evaluar_dia_empleado(self.empleado,dia)
        p = self.servicio().previsualizar_contingencia(self.datos,self.dg)
        self.servicio().aplicar_contingencia(self.datos,self.dg,digest=p['digest'])
        actual = I.objects.filter(empleado=self.empleado,fecha__gte=self.dia,tipo__in=['aviso_baja_faltas','baja_faltas']).exclude(estado='resuelto').count()
        self.assertEqual(actual,p['alertas_despues'])

    def test_aprobado_no_puede_volver_a_borrador(self):
        self.aprobar()
        with self.assertRaises(ValidationError):
            self.servicio().previsualizar_contingencia(self.datos,self.dg,accion='borrador')

    def test_revocar_preview_refleja_falta_y_alertas_restituidas(self):
        for dia in (date(2026,11,19),date(2026,11,20),self.dia,date(2026,11,23)):
            evaluar_dia_empleado(self.empleado,dia)
        self.aprobar()
        s = self.servicio()
        p = s.previsualizar_contingencia(self.datos,self.dg,accion='revocar')
        self.assertTrue(p['incidencias_a_revalidar'])
        s.aplicar_contingencia(self.datos,self.dg,accion='revocar',digest=p['digest'])
        actual = I.objects.filter(empleado=self.empleado,fecha__gte=self.dia,tipo__in=['aviso_baja_faltas','baja_faltas']).exclude(estado='resuelto').count()
        self.assertEqual(actual,p['alertas_despues'])

    def test_preview_union_con_otro_permiso_y_falta_ingresada_desfasada(self):
        self.aprobar()
        I.objects.create(empleado=self.empleado,fecha=self.dia,tipo='falta',estado='pendiente')
        for dia in (date(2026,11,19),date(2026,11,20)):
            I.objects.create(empleado=self.empleado,fecha=dia,tipo='falta',estado='pendiente')
        datos = dict(self.datos,clave='OTRA-FECHA',fecha_inicio='2026-11-23',fecha_fin='2026-11-23')
        p = self.servicio().previsualizar_contingencia(datos,self.dg)
        self.servicio().aplicar_contingencia(datos,self.dg,digest=p['digest'])
        actual = I.objects.filter(empleado=self.empleado,fecha__gte=date(2026,11,23),tipo__in=['aviso_baja_faltas','baja_faltas']).exclude(estado='resuelto').count()
        self.assertEqual(actual,p['alertas_despues'])

    def test_simulacion_financiera_sin_escrituras_coincide_con_bono_aplicado(self):
        from core.models import Sucursal
        from bonos_ventas.models import ConfigBonoVentasPeriodo,BonoVentasEmpleado
        cfg = ConfigBonoVentasPeriodo.objects.create(mes=11,anio=2026,fecha_inicio=self.dia,fecha_fin=self.dia,dias_laborables=1,cancela_por_asistencia=True,limite_asistencia_cancelacion=1)
        bono = BonoVentasEmpleado.objects.create(periodo=cfg,empleado=self.empleado,sucursal=Sucursal.objects.create(nombre='Preview financiero'),bono_extra=13,ajuste_positivo=7,ajuste_negativo=2)
        with CaptureQueriesContext(connection) as queries:
            p = self.servicio().previsualizar_contingencia(self.datos,self.dg)
        self.assertFalse(any(q['sql'].lstrip().upper().startswith(('INSERT','UPDATE','DELETE')) for q in queries))
        plan = next(r for r in p['bonos_plan'] if r['id']==bono.pk and r['modelo']=='bonos_ventas.BonoVentasEmpleado')
        self.servicio().aplicar_contingencia(self.datos,self.dg,digest=p['digest'])
        bono.refresh_from_db()
        self.assertEqual(plan['despues']['total_a_pagar'],str(bono.total_a_pagar))
        self.assertEqual(plan['despues']['cancela_bono'],bono.cancela_bono)

    def test_revocar_no_recrea_falta_despues_de_baja_corregida(self):
        from rrhh.models import EmpleadoBaja
        self.aprobar()
        EmpleadoBaja.objects.create(empleado=self.empleado,nombre=self.empleado.nombre,fecha_ingreso=self.empleado.fecha_ingreso,fecha_baja=self.dia-timedelta(days=1))
        self.empleado.refresh_from_db()
        from core.models import Sucursal
        from bonos_ventas.models import ConfigBonoVentasPeriodo,BonoVentasEmpleado
        cfg = ConfigBonoVentasPeriodo.objects.create(mes=11,anio=2026,fecha_inicio=self.dia,fecha_fin=self.dia,dias_laborables=1)
        bono = BonoVentasEmpleado.objects.create(periodo=cfg,empleado=self.empleado,sucursal=Sucursal.objects.create(nombre='Baja corregida'),total_a_pagar=234,bono_extra=17)
        s = self.servicio()
        p = s.previsualizar_contingencia(self.datos,self.dg,accion='revocar')
        self.assertFalse(p['incidencias_a_revalidar'])
        s.aplicar_contingencia(self.datos,self.dg,digest=p['digest'],accion='revocar')
        self.assertFalse(I.objects.filter(empleado=self.empleado,fecha__gte=self.dia,estado='pendiente').exists())
        bono.refresh_from_db()
        self.assertEqual(bono.total_a_pagar,234)
        self.assertEqual(bono.bono_extra,17)
        self.assertFalse(bono.registros.exists())

    def test_permiso_personal_no_inventa_alerta_en_dia_no_evaluado(self):
        from rrhh.services_permisos import reevaluar_asistencia_permiso
        for dia in (date(2026,11,17),date(2026,11,18),date(2026,11,19)):
            I.objects.create(empleado=self.empleado,fecha=dia,tipo='falta')
        I.objects.create(empleado=self.empleado,fecha=date(2026,10,24),tipo='aviso_baja_faltas')
        permiso = PermisoSalida.objects.create(empleado=self.empleado,tipo='permiso_dia',estado='aprobado',goce_sueldo=True,fecha_inicio=timezone.make_aware(datetime.combine(self.dia,time.min)),fecha_fin=timezone.make_aware(datetime.combine(self.dia,time.max)),motivo='Autorizado')
        with self.captureOnCommitCallbacks(execute=True):
            reevaluar_asistencia_permiso(permiso)
        self.assertFalse(I.objects.filter(empleado=self.empleado,fecha__gt=self.dia,tipo='aviso_baja_faltas').exists())

    def test_bloquea_corte_antes_de_resumen_y_movimientos_como_recalculo_prenomina(self):
        from unittest.mock import patch
        s = self.servicio()
        recalcular_corte_prenomina(self.corte())
        orden = []
        original = s._rows
        def registrar(query,lock=False,fields=None):
            if lock:
                orden.append(query.model._meta.label)
            return original(query,lock,fields)
        with patch('rrhh.services_contingencia._rows',side_effect=registrar):
            s.previsualizar_contingencia(self.datos,self.dg,_lock=True)
        self.assertLess(orden.index('rrhh.PrenominaCorte'),orden.index('rrhh.PrenominaEmpleadoResumen'))
        self.assertLess(orden.index('rrhh.PrenominaCorte'),orden.index('rrhh.PrenominaMovimiento'))
