from datetime import date, datetime, time, timedelta
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.test import TestCase
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from rrhh.models import (
    AsignacionTurnoEmpleado, AsistenciaEmpleado, Empleado,
    IncidenciaAsistencia, SolicitudVacaciones, Turno,
)
from rrhh.services_contingencia import aplicar_contingencia, previsualizar_contingencia
from rrhh.services_asistencia_reglas import _faltas_vigentes


class LimitesContingenciaTests(TestCase):
    def setUp(self):
        self.actor = get_user_model().objects.create_superuser('cont-limites', '', 'test')
        self.employee = Empleado.objects.create(
            codigo='CONT-LIMITES', nombre='Colaborador sintético', fecha_ingreso=date(2026, 1, 1),
        )
        self.day = date(2026, 11, 21)
        self.data = dict(
            clave='CONT-LIMITES-20261121', fecha_inicio=str(self.day), fecha_fin=str(self.day),
            motivo='seguridad', descripcion='Cierre empresarial', evidencia=['acta:limites'],
            alcance='empresa', tratamiento_salarial='conservar',
        )

    def apply(self):
        preview = previsualizar_contingencia(self.data, self.actor)
        return aplicar_contingencia(self.data, self.actor, digest=preview['digest'])

    def test_cruce_mes_detecta_bono_legacy_cerrado_del_segundo_mes(self):
        from core.models import Sucursal
        from bonos_ventas.models import ConfigBonoVentasPeriodo, BonoVentasEmpleado
        branch = Sucursal.objects.create(nombre='Sucursal sintética límite')
        period = ConfigBonoVentasPeriodo.objects.create(mes=11, anio=2026)
        BonoVentasEmpleado.objects.create(
            periodo=period, empleado=self.employee, sucursal=branch, estatus='PAGADO',
        )
        data = dict(self.data, fecha_inicio='2026-10-30', fecha_fin='2026-11-02')
        preview = previsualizar_contingencia(data, self.actor)
        self.assertTrue(any(c['tipo'] == 'bono_cerrado' for c in preview['conflictos']))
        with self.assertRaises(ValidationError):
            aplicar_contingencia(data, self.actor, digest=preview['digest'])

    def test_vacaciones_nuevas_invalidan_digest_revisado(self):
        preview = previsualizar_contingencia(self.data, self.actor)
        SolicitudVacaciones.objects.create(
            empleado=self.employee, fecha_inicio=self.day, fecha_fin=self.day, estado='aprobada',
        )
        with self.assertRaises(ValidationError):
            aplicar_contingencia(self.data, self.actor, digest=preview['digest'])

    def test_jornada_nueva_invalida_digest_revisado(self):
        preview = previsualizar_contingencia(self.data, self.actor)
        shift = Turno.objects.create(nombre='Turno sintético', hora_entrada=time(8), hora_salida=time(16))
        AsignacionTurnoEmpleado.objects.create(
            empleado=self.employee, turno=shift, fecha_inicio=self.day, motivo='Cambio después de simulación',
        )
        with self.assertRaises(ValidationError):
            aplicar_contingencia(self.data, self.actor, digest=preview['digest'])

    def test_no_inventa_fecha_evaluada_para_emitir_alertas(self):
        # Los únicos días con evidencia del motor terminan el sábado.
        for offset in (4, 3, 2, 0):
            IncidenciaAsistencia.objects.create(
                empleado=self.employee, fecha=self.day-timedelta(days=offset), tipo='falta',
            )
        self.apply()
        self.assertFalse(IncidenciaAsistencia.objects.filter(
            empleado=self.employee, fecha__gt=self.day, tipo__in=['aviso_baja_faltas', 'baja_faltas'],
        ).exists())

    def test_falta_con_trabajo_real_se_preserva_en_aprobacion_y_revocacion(self):
        mark = timezone.make_aware(datetime.combine(self.day, time(11)))
        AsistenciaEmpleado.objects.create(empleado=self.employee, fecha=self.day, entrada=mark)
        incidence = IncidenciaAsistencia.objects.create(
            empleado=self.employee, fecha=self.day, tipo='falta', detalle='Falta derivada de entrada tardía real',
        )
        self.apply()
        incidence.refresh_from_db()
        self.assertEqual(incidence.estado, 'pendiente')
        self.assertEqual(_faltas_vigentes(self.employee, self.day), 1)
        preview = previsualizar_contingencia(self.data, self.actor, accion='revocar')
        aplicar_contingencia(self.data, self.actor, digest=preview['digest'], accion='revocar')
        incidence.refresh_from_db()
        self.assertEqual(incidence.estado, 'pendiente')

    def test_fila_sin_marcas_no_equivale_a_trabajo_real(self):
        AsistenciaEmpleado.objects.create(empleado=self.employee, fecha=self.day)
        incidence = IncidenciaAsistencia.objects.create(
            empleado=self.employee, fecha=self.day, tipo='falta',
        )
        self.apply()
        incidence.refresh_from_db()
        self.assertEqual(incidence.estado, 'resuelto')
        self.assertEqual(_faltas_vigentes(self.employee, self.day), 0)
        self.assertIsNone(AsistenciaEmpleado.objects.get(empleado=self.employee, fecha=self.day).entrada)

    def test_simula_bono_sin_escrituras_ni_cantidades_ficticias(self):
        from core.models import Sucursal
        from bonos_ventas.models import ConfigBonoVentasPeriodo, BonoVentasEmpleado
        branch = Sucursal.objects.create(nombre='Sucursal sintética simulación')
        period = ConfigBonoVentasPeriodo.objects.create(
            mes=11, anio=2026, fecha_inicio=self.day, fecha_fin=self.day, dias_laborables=1,
            cancela_por_asistencia=True, limite_asistencia_cancelacion=1,
        )
        bonus = BonoVentasEmpleado.objects.create(
            periodo=period, empleado=self.employee, sucursal=branch,
            bono_extra=17, ajuste_positivo=9, ajuste_negativo=3,
        )
        bonus.recalcular()
        bonus.save()
        self.assertTrue(bonus.cancela_bono)
        original_total = bonus.total_a_pagar
        with CaptureQueriesContext(connection) as queries:
            preview = previsualizar_contingencia(self.data, self.actor)
        self.assertFalse(any(q['sql'].lstrip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')) for q in queries))
        plan = next(row for row in preview['bonos_plan'] if row['modelo']=='bonos_ventas.BonoVentasEmpleado' and row['id']==bonus.pk)
        self.assertFalse(plan['despues']['cancela_bono'])
        self.assertEqual(plan['despues']['dias_trabajados'], 0)
        self.assertEqual(plan['despues']['dias_asistencia'], 0)
        self.assertEqual(Decimal(str(plan['delta_total'])), Decimal(str(plan['despues']['total_a_pagar']))-original_total)
        bonus.refresh_from_db()
        self.assertTrue(bonus.cancela_bono)
        self.assertEqual(bonus.total_a_pagar, original_total)
        self.assertEqual((bonus.bono_extra,bonus.ajuste_positivo,bonus.ajuste_negativo),(17,9,3))

    def test_simulacion_coincide_con_aplicacion_si_encabezado_bono_estaba_desactualizado(self):
        from core.models import Sucursal
        from bonos_ventas.models import ConfigBonoVentasPeriodo, BonoVentasEmpleado, RegistroDiarioVentas
        branch = Sucursal.objects.create(nombre='Sucursal sintética encabezado')
        previous_day = self.day-timedelta(days=1)
        period = ConfigBonoVentasPeriodo.objects.create(
            mes=11, anio=2026, fecha_inicio=previous_day, fecha_fin=self.day, dias_laborables=2,
        )
        bonus = BonoVentasEmpleado.objects.create(
            periodo=period, empleado=self.employee, sucursal=branch,
            dias_trabajados=2, dias_asistencia=2, bono_extra=17,
        )
        RegistroDiarioVentas.objects.create(
            bono=bonus, fecha=previous_day, dia=previous_day.day,
            tiene_asistencia=True, tiene_puntualidad=True, tiene_uniforme=True,
        )
        preview = previsualizar_contingencia(self.data, self.actor)
        plan = next(row for row in preview['bonos_plan'] if row['modelo']=='bonos_ventas.BonoVentasEmpleado' and row['id']==bonus.pk)
        self.assertEqual(plan['despues']['dias_trabajados'], 1)
        aplicar_contingencia(self.data,self.actor,digest=preview['digest'])
        bonus.refresh_from_db()
        self.assertEqual(bonus.dias_trabajados, plan['despues']['dias_trabajados'])
        self.assertEqual(bonus.total_a_pagar, Decimal(str(plan['despues']['total_a_pagar'])))
        self.assertEqual(bonus.bono_extra, 17)

    def test_periodo_explicito_cerrado_del_mismo_mes_sin_interseccion_no_bloquea(self):
        from core.models import Sucursal
        from bonos_ventas.models import ConfigBonoVentasPeriodo, BonoVentasEmpleado
        branch = Sucursal.objects.create(nombre='Sucursal sintética otro periodo')
        period = ConfigBonoVentasPeriodo.objects.create(
            mes=11, anio=2026, fecha_inicio=date(2026,11,1), fecha_fin=date(2026,11,2),
        )
        BonoVentasEmpleado.objects.create(
            periodo=period, empleado=self.employee, sucursal=branch, estatus='PAGADO',
        )
        preview = previsualizar_contingencia(self.data, self.actor)
        self.assertFalse(any(c['tipo']=='bono_cerrado' for c in preview['conflictos']))

    def test_ambiguo_produccion_bloquea_aplicacion_sin_modificar_bono(self):
        from bonos_produccion.models import ConfigBonoPeriodo, BonoProduccionEmpleado, RegistroDiarioProduccion
        period = ConfigBonoPeriodo.objects.create(
            mes=11, anio=2026, fecha_inicio=self.day, fecha_fin=self.day, dias_laborables=1,
        )
        bonus = BonoProduccionEmpleado.objects.create(periodo=period, empleado=self.employee, area='HORNOS')
        RegistroDiarioProduccion.objects.create(bono=bonus,dia=self.day.day,fecha=None,tiene_asistencia=True)
        RegistroDiarioProduccion.objects.create(bono=bonus,dia=self.day.day,fecha=self.day,tiene_asistencia=False)
        preview = previsualizar_contingencia(self.data,self.actor)
        self.assertTrue(preview['conflictos'])
        with self.assertRaises(ValidationError):
            aplicar_contingencia(self.data,self.actor,digest=preview['digest'])
        self.assertEqual(RegistroDiarioProduccion.objects.filter(bono=bonus).count(), 2)
        bonus.refresh_from_db()
        self.assertEqual(bonus.dias_trabajados, 0)
