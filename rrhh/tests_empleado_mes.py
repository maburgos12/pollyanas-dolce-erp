from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import close_old_connections
from django.test import TestCase, TransactionTestCase
from django.utils import timezone
from core.models import AuditLog
from rrhh.models import Empleado, PeriodoEmpleadoMes
from rrhh.models_empleado_mes import CRITERIOS
from rrhh import services_empleado_mes as s


class EmpleadoMesTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.hoy = timezone.localdate()
        cls.rh = get_user_model().objects.create_user('rh_em_mes')
        cls.rh.groups.add(Group.objects.get_or_create(name='RRHH')[0])
        Empleado.objects.create(nombre='RH', codigo='EMRH', departamento='RRHH', usuario_erp=cls.rh)
        cls.dg = get_user_model().objects.create_user('dg_em_mes')
        cls.dg.groups.add(Group.objects.get_or_create(name='DG')[0])
        cls.jefes, cls.equipos = {}, {}
        for dep in s.DEPARTAMENTOS:
            user = get_user_model().objects.create_user('jefe_' + dep)
            jefe = Empleado.objects.create(nombre=dep, codigo=dep, departamento=dep, nivel_organizacional='JEFATURA', usuario_erp=user)
            cls.jefes[dep] = user
            cls.equipos[dep] = [Empleado.objects.create(nombre=f'{dep} {i}', codigo=f'{dep}{i}', departamento=dep, jefe_directo=jefe) for i in range(3)]

    def setUp(self):
        self.p = s.abrir_periodo(self.rh, self.hoy.replace(day=1), self.hoy)

    def postular(self, dep='PRODUCCION', index=0):
        return s.postular(self.jefes[dep], self.p.pk, self.equipos[dep][index].pk)

    def enviar(self, e, **kwargs):
        data = dict(calificaciones=dict.fromkeys(CRITERIOS, 5), comentarios={}, reconocimiento=False, reconocimiento_detalle='', recomienda=True, justificacion='Resultados comprobados', version=e.version, enviar=True)
        data.update(kwargs)
        return s.guardar_evaluacion(e.evaluador, e.pk, **data)

    def decidir(self):
        evaluaciones = [self.enviar(self.postular(dep, i)) for dep in s.DEPARTAMENTOS for i in range(2)]
        return s.elegir(self.rh, self.p.pk, evaluaciones[0].pk, 'Empate resuelto por contribución', self.p.version)

    def test_ciclo_y_auditoria(self):
        p = self.decidir()
        self.assertEqual(p.ganador.total, 70)
        p = s.aprobar_premio(self.rh, p.pk, 'vales', 'Vales de despensa', '500.00', p.version)
        p = s.entregar(self.rh, p.pk, self.hoy, 'Capital Humano', 'Acuse 123', p.version)
        self.assertEqual(p.estado_display, 'Entregado')
        log = AuditLog.objects.get(action='EM_MES_ENTREGAR', object_id=str(p.pk))
        self.assertIsNone(log.payload['antes']['entregado_en'])
        self.assertEqual(log.payload['despues']['entrega_comprobante'], 'Acuse 123')
        with self.assertRaises(ValidationError):
            s.entregar(self.rh, p.pk, self.hoy, 'RH', '', p.version)
        with self.assertRaises(ValidationError):
            s.aprobar_premio(self.rh, p.pk, 'efectivo', 'Otro', 200, p.version)

    def test_mes_pasado_sin_plazo_captura_hasta_eleccion(self):
        mes = (self.hoy.replace(day=1) - timedelta(days=1)).replace(day=1)
        self.p = s.abrir_periodo(self.rh, mes)
        self.assertIsNone(self.p.fecha_limite)
        log = AuditLog.objects.get(action='EM_MES_ABRIR', object_id=str(self.p.pk))
        self.assertIsNone(log.payload['despues']['fecha_limite'])
        with patch('django.utils.timezone.localdate', return_value=self.hoy + timedelta(days=365)):
            self.assertFalse(self.p.plazo_vencido)
            p = self.decidir()
            self.assertFalse(s.puede_editar_evaluacion(p.ganador.evaluador, p.ganador))
            with self.assertRaises(ValidationError):
                s.postular(self.jefes['PRODUCCION'], p.pk, self.equipos['PRODUCCION'][2].pk)
            with self.assertRaises(ValidationError):
                s.devolver(self.rh, p.ganador_id, 'Corrección', p.ganador.version)
            with self.assertRaises(ValidationError):
                s.ampliar_plazo(self.rh, p.pk, None, 'Retirar plazo', p.version)

    def test_retirar_plazo_conserva_evaluacion_y_auditoria(self):
        e = self.enviar(self.postular())
        PeriodoEmpleadoMes.objects.filter(pk=self.p.pk).update(fecha_limite=self.hoy - timedelta(days=1))
        with self.assertRaises(PermissionDenied):
            s.ampliar_plazo(self.dg, self.p.pk, None, 'Retirar plazo', 0)
        with self.assertRaises(ValidationError):
            s.ampliar_plazo(self.rh, self.p.pk, None, '', 0)
        with self.assertRaises(ValidationError):
            s.ampliar_plazo(self.rh, self.p.pk, None, 'Retirar plazo', 1)
        p = s.ampliar_plazo(self.rh, self.p.pk, None, 'Captura hasta elección de RRHH', 0)
        self.assertIsNone(p.fecha_limite)
        self.assertEqual(p.version, 1)
        log = AuditLog.objects.get(action='EM_MES_AMPLIAR', object_id=str(p.pk))
        self.assertEqual(log.payload['antes']['fecha_limite'], (self.hoy - timedelta(days=1)).isoformat())
        self.assertIsNone(log.payload['despues']['fecha_limite'])
        e = s.devolver(self.rh, e.pk, 'Completar evidencia', e.version)
        self.assertTrue(s.puede_editar_evaluacion(e.evaluador, e))
        self.assertEqual(e.total, 70)
        with self.assertRaises(ValidationError):
            s.ampliar_plazo(self.rh, p.pk, self.hoy, 'No acortar captura abierta', p.version)

    def test_apertura_sin_plazo_conserva_validaciones(self):
        mes = (self.hoy.replace(day=1) - timedelta(days=1)).replace(day=1)
        with self.assertRaises(PermissionDenied):
            s.abrir_periodo(self.dg, mes)
        for fecha in (self.hoy - timedelta(days=1), '2026-11-30'):
            with self.assertRaises(ValidationError):
                s.abrir_periodo(self.rh, mes, fecha)
        with self.assertRaises(ValidationError):
            s.abrir_periodo(self.rh, mes.replace(day=2))
        s.abrir_periodo(self.rh, mes)
        with self.assertRaises(ValidationError):
            s.abrir_periodo(self.rh, mes)

    def test_permisos_equipo_ventas_dg(self):
        self.assertTrue(s.puede_ver_todo(self.dg))
        self.assertFalse(s.puede_gestionar(self.dg))
        with self.assertRaises(PermissionDenied):
            s.abrir_periodo(self.dg, self.hoy.replace(day=1), self.hoy)
        with self.assertRaises(PermissionDenied):
            s.postular(self.jefes['PRODUCCION'], self.p.pk, self.equipos['ADMINISTRACION'][0].pk)
        venta = Empleado.objects.create(nombre='Ventas', codigo='EMVENT', departamento='VENTAS', jefe_directo=self.equipos['PRODUCCION'][0].jefe_directo)
        with self.assertRaises(PermissionDenied):
            s.postular(self.jefes['PRODUCCION'], self.p.pk, venta.pk)
        e = self.postular()
        self.assertFalse(s.evaluaciones_visibles(self.jefes['ADMINISTRACION'], self.p).exists())
        with self.assertRaises(PermissionDenied):
            s.guardar_evaluacion(self.dg, e.pk, calificaciones={}, comentarios={}, reconocimiento=False, reconocimiento_detalle='', recomienda=None, justificacion='', version=0)

    def test_duplicados_y_cupo(self):
        self.postular()
        with self.assertRaises(ValidationError):
            self.postular()
        self.postular(index=1)
        with self.assertRaises(ValidationError):
            self.postular(index=2)

    def test_validacion_y_version(self):
        e = self.postular()
        for cal in ({}, {'extra': 5}, dict.fromkeys(CRITERIOS, True), dict.fromkeys(CRITERIOS, 6)):
            with self.assertRaises(ValidationError):
                self.enviar(e, calificaciones=cal)
        with self.assertRaises(ValidationError):
            self.enviar(e, reconocimiento=True)
        with self.assertRaises(ValidationError):
            self.enviar(e, recomienda=None)
        e = self.enviar(e, enviar=False, calificaciones={'metas': 3}, recomienda=None, justificacion='')
        with self.assertRaises(ValidationError):
            self.enviar(e, version=0)
        e = self.enviar(e)
        with self.assertRaises(ValidationError):
            self.enviar(e)
        e = s.devolver(self.rh, e.pk, 'Corregir evidencia', e.version)
        self.assertEqual(e.estado, 'borrador')
        self.assertEqual(AuditLog.objects.filter(model='rrhh.EvaluacionEmpleadoMes', object_id=str(e.pk)).count(), 4)

    def test_plazo_y_cambio_jefatura(self):
        e = self.postular()
        jefe = self.equipos['PRODUCCION'][0].jefe_directo
        jefe.nivel_organizacional = 'COLABORADOR'
        jefe.save()
        otro = get_user_model().objects.create_user('nueva_jefa')
        Empleado.objects.create(nombre='Nueva', codigo='EMNEW', departamento='PRODUCCION', nivel_organizacional='JEFATURA', usuario_erp=otro)
        self.assertFalse(s.candidatos(otro, self.p).exists())
        self.assertTrue(s.puede_acceder(e.evaluador))
        e = self.enviar(e)
        PeriodoEmpleadoMes.objects.filter(pk=self.p.pk).update(fecha_limite=self.hoy-timedelta(days=1))
        with self.assertRaises(ValidationError):
            s.devolver(self.rh, e.pk, 'Corrección', e.version)
        p = s.ampliar_plazo(self.rh, self.p.pk, self.hoy, 'Reabrir plazo', 0)
        self.assertEqual(p.version, 1)
        s.devolver(self.rh, e.pk, 'Corrección', e.version)

    def test_decision_y_premios_invalidos(self):
        e = self.enviar(self.postular())
        with self.assertRaises(ValidationError):
            s.elegir(self.rh, self.p.pk, e.pk, 'Motivo', 0)
        self.enviar(self.postular(index=1))
        for i in range(2):
            self.enviar(self.postular('ADMINISTRACION', i))
        with self.assertRaises(ValidationError):
            s.elegir(self.rh, self.p.pk, e.pk, '', 0)
        p = s.elegir(self.rh, self.p.pk, e.pk, 'Desempate', 0)
        for importe in (None, '-1', 'NaN', 'Infinity', '1.234'):
            with self.assertRaises(ValidationError):
                s.aprobar_premio(self.rh, p.pk, 'efectivo', 'Premio', importe, p.version)
        p = s.aprobar_premio(self.rh, p.pk, 'descanso', 'Día acordado sin movimiento de vacaciones', None, p.version)
        for fecha in (self.hoy-timedelta(days=1), self.hoy+timedelta(days=1)):
            with self.assertRaises(ValidationError):
                s.entregar(self.rh, p.pk, fecha, 'RH', '', p.version)

    def test_cambio_departamento_retiro_edicion_conserva_historia(self):
        e = self.postular()
        self.assertTrue(s.puede_editar_evaluacion(e.evaluador, e))
        jefe = self.equipos['PRODUCCION'][0].jefe_directo
        jefe.departamento = 'VENTAS'
        jefe.save()
        self.assertFalse(s.puede_editar_evaluacion(e.evaluador, e))
        self.assertTrue(s.evaluaciones_visibles(e.evaluador, self.p).filter(pk=e.pk).exists())
        with self.assertRaises(PermissionDenied):
            self.enviar(e)

    def test_entrega_no_puede_preceder_aprobacion(self):
        p = self.decidir()
        p = s.aprobar_premio(self.rh, p.pk, 'vales', 'Premio', '500', p.version)
        PeriodoEmpleadoMes.objects.filter(pk=p.pk).update(elegido_en=timezone.now()-timedelta(days=3))
        with self.assertRaises(ValidationError):
            s.entregar(self.rh, p.pk, self.hoy-timedelta(days=1), 'RH', '', p.version)

    def test_usuario_desvinculado_no_accede_a_evaluaciones(self):
        e = self.postular()
        Empleado.objects.filter(usuario_erp=e.evaluador).update(usuario_erp=None)
        usuario = get_user_model().objects.get(pk=e.evaluador_id)
        self.assertFalse(s.puede_acceder(usuario))
        self.assertTrue(s.evaluaciones_visibles(self.rh, self.p).filter(pk=e.pk).exists())

    def test_jefatura_ambigua(self):
        Empleado.objects.create(nombre='Duplicada', codigo='EMDUP', departamento='PRODUCCION', nivel_organizacional='JEFATURA')
        mes = (self.hoy.replace(day=1)+timedelta(days=32)).replace(day=1)
        with self.assertRaises(ValidationError):
            s.abrir_periodo(self.rh, mes, mes)


class EmpleadoMesConcurrenciaTests(TransactionTestCase):
    def test_dos_postulaciones_concurrentes_no_superan_cupo(self):
        EmpleadoMesTests.setUpTestData.__func__(type(self))
        periodo = s.abrir_periodo(self.rh, self.hoy.replace(day=1), self.hoy)
        jefe = self.jefes['PRODUCCION']
        s.postular(jefe, periodo.pk, self.equipos['PRODUCCION'][0].pk)
        barrera = Barrier(2)

        def postular(empleado_id):
            close_old_connections()
            try:
                usuario = get_user_model().objects.get(pk=jefe.pk)
                barrera.wait(timeout=10)
                try:
                    s.postular(usuario, periodo.pk, empleado_id)
                    return 'creada'
                except ValidationError:
                    return 'cupo'
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as executor:
            resultados = list(executor.map(postular, [e.pk for e in self.equipos['PRODUCCION'][1:]]))
        self.assertCountEqual(resultados, ['creada', 'cupo'])
        self.assertEqual(periodo.evaluaciones.count(), 2)
