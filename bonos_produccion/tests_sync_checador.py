from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from unittest.mock import patch
from zoneinfo import ZoneInfo

from django.contrib.auth.models import Group, User
from django.test import TestCase
from django.urls import reverse

from core.access import ROLE_BONOS_PRODUCCION_CAPTURA
from rrhh.models import AsistenciaEmpleado, Empleado, IncapacidadEmpleado, IncidenciaAsistencia
from rrhh.services_bonos_checador import programar_sincronizacion_bonos_desde_checador

from .models import (
    AREA_HORNOS,
    BonoProduccionEmpleado,
    ConfigBonoArea,
    ConfigBonoPeriodo,
    RegistroDiarioProduccion,
)
from .serializers import BonoProduccionCapturaSerializer, RegistroDiarioCapturaSerializer
from .services_checador import sincronizar_asistencia_desde_checador, sincronizar_empleado_dia_desde_checador


TZ = ZoneInfo("America/Mazatlan")


def dt_local(fecha: date, hora: time = time(8, 0)) -> datetime:
    return datetime(fecha.year, fecha.month, fecha.day, hora.hour, hora.minute, tzinfo=TZ)


class SyncChecadorProduccionTests(TestCase):
    def crear_bono(self, fecha: date, **bono_kwargs):
        periodo = ConfigBonoPeriodo.objects.create(
            mes=fecha.month,
            anio=fecha.year,
            dias_laborables=1,
            fecha_inicio=fecha,
            fecha_fin=fecha,
        )
        empleado = Empleado.objects.create(
            nombre=f"Empleado Produccion {fecha.day}",
            area="HORNOS",
            fecha_ingreso=date(2026, 1, 1),
        )
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area=AREA_HORNOS,
            **bono_kwargs,
        )
        return periodo, empleado, bono

    def crear_asistencia(self, empleado: Empleado, fecha: date) -> None:
        AsistenciaEmpleado.objects.create(
            empleado=empleado,
            fecha=fecha,
            entrada=dt_local(fecha),
            fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
        )

    def crear_incidencia(self, empleado: Empleado, fecha: date, tipo: str, estado: str | None = None) -> None:
        IncidenciaAsistencia.objects.create(
            empleado=empleado,
            fecha=fecha,
            tipo=tipo,
            estado=estado or IncidenciaAsistencia.ESTADO_PENDIENTE,
        )

    def test_registro_admite_contexto_rrhh_y_serializer_lo_expone_solo_lectura(self):
        fecha = date(2026, 9, 1)
        _periodo, _empleado, bono = self.crear_bono(fecha)
        registro = RegistroDiarioProduccion.objects.create(
            bono=bono,
            dia=1,
            fecha=fecha,
            estado_rrhh="incapacidad",
            motivo_rrhh="Incapacidad vigente.",
            falta_penalizable=False,
        )

        data = RegistroDiarioCapturaSerializer(registro).data

        self.assertEqual(data["fecha"], "2026-09-01")
        self.assertEqual(data["estado_rrhh"], "incapacidad")
        self.assertFalse(data["falta_penalizable"])
        self.assertTrue(RegistroDiarioCapturaSerializer().fields["estado_rrhh"].read_only)
        self.assertTrue(RegistroDiarioCapturaSerializer().fields["falta_penalizable"].read_only)

    def test_filas_historicas_conservan_contexto_nullable(self):
        fecha = date(2026, 9, 2)
        _periodo, _empleado, bono = self.crear_bono(fecha)

        registro = RegistroDiarioProduccion.objects.create(bono=bono, dia=2)

        self.assertIsNone(registro.fecha)
        self.assertIsNone(registro.falta_penalizable)
        self.assertIsNone(bono.faltas_rrhh)

    def test_argelia_incapacidad_y_festivo_no_se_convierten_en_faltas(self):
        periodo = ConfigBonoPeriodo.objects.create(
            mes=9,
            anio=2026,
            dias_laborables=23,
            fecha_inicio=date(2026, 8, 28),
            fecha_fin=date(2026, 9, 26),
        )
        empleado = Empleado.objects.create(
            codigo="ARGELIA-CTX",
            nombre="Melendrez Verdugo Argelia",
            area="HORNOS",
            fecha_ingreso=date(2007, 7, 30),
        )
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area=AREA_HORNOS,
        )
        ConfigBonoArea.objects.create(
            periodo=periodo,
            area=AREA_HORNOS,
            cancela_por_asistencia=True,
            limite_asistencia_cancelacion=1,
        )
        IncapacidadEmpleado.objects.create(
            empleado=empleado,
            fecha_inicio=date(2026, 8, 28),
            fecha_fin=date(2026, 9, 1),
            estado=IncapacidadEmpleado.ESTADO_CERRADA,
        )
        fecha = date(2026, 9, 2)
        while fecha <= date(2026, 9, 26):
            if fecha.weekday() != 6 and fecha != date(2026, 9, 16):
                self.crear_asistencia(empleado, fecha)
            fecha = date.fromordinal(fecha.toordinal() + 1)

        sincronizar_asistencia_desde_checador(periodo)
        bono.refresh_from_db()

        self.assertEqual(bono.dias_asistencia, 21)
        self.assertEqual(bono.faltas_rrhh, 0)
        self.assertFalse(bono.cancela_bono)
        incapacidad = RegistroDiarioProduccion.objects.get(bono=bono, fecha=date(2026, 9, 1))
        festivo = RegistroDiarioProduccion.objects.get(bono=bono, fecha=date(2026, 9, 16))
        self.assertEqual(incapacidad.estado_rrhh, "incapacidad")
        self.assertFalse(incapacidad.falta_penalizable)
        self.assertEqual(festivo.estado_rrhh, "festivo")
        self.assertFalse(festivo.falta_penalizable)

    def test_sync_preserva_captura_manual_dinero_y_es_idempotente(self):
        fecha = date(2026, 9, 7)
        periodo, empleado, bono = self.crear_bono(
            fecha,
            ajuste_positivo=Decimal("125.00"),
            ajuste_negativo=Decimal("25.00"),
            bono_extra=Decimal("75.00"),
            desc_ajuste_positivo="Captura Carolina",
            desc_ajuste_negativo="Ajuste revisado",
            desc_bono_extra="Extra autorizado",
        )
        capturista = User.objects.create_user(username="carolina.contexto", password="x")
        self.crear_asistencia(empleado, fecha)
        RegistroDiarioProduccion.objects.create(
            bono=bono,
            dia=fecha.day,
            tiene_asistencia=False,
            tiene_puntualidad=False,
            tiene_uniforme=False,
            tiene_produccion=False,
            cantidad_embetunados=4,
            observacion="Corrección manual",
            capturado_por=capturista,
        )
        dinero_antes = (
            bono.ajuste_positivo,
            bono.ajuste_negativo,
            bono.bono_extra,
            bono.desc_ajuste_positivo,
            bono.desc_ajuste_negativo,
            bono.desc_bono_extra,
        )

        primera = sincronizar_asistencia_desde_checador(periodo)
        registro = RegistroDiarioProduccion.objects.get(bono=bono)
        segunda = sincronizar_asistencia_desde_checador(periodo)
        bono.refresh_from_db()
        registro.refresh_from_db()

        self.assertEqual(primera["registros_actualizados"], 1)
        self.assertEqual(segunda["registros_creados"], 0)
        self.assertEqual(segunda["registros_actualizados"], 0)
        self.assertFalse(registro.tiene_asistencia)
        self.assertFalse(registro.tiene_puntualidad)
        self.assertFalse(registro.tiene_uniforme)
        self.assertFalse(registro.tiene_produccion)
        self.assertEqual(registro.cantidad_embetunados, 4)
        self.assertEqual(registro.observacion, "Corrección manual")
        self.assertTrue(registro.falta_penalizable)
        self.assertEqual(
            (
                bono.ajuste_positivo,
                bono.ajuste_negativo,
                bono.bono_extra,
                bono.desc_ajuste_positivo,
                bono.desc_ajuste_negativo,
                bono.desc_bono_extra,
            ),
            dinero_antes,
        )

    def test_checada_normal_sin_incidencias_crea_asistencia_y_puntualidad_true(self):
        fecha = date(2026, 6, 1)
        periodo, empleado, bono = self.crear_bono(fecha)
        self.crear_asistencia(empleado, fecha)

        resultado = sincronizar_asistencia_desde_checador(periodo)

        registro = RegistroDiarioProduccion.objects.get(bono=bono, dia=1)
        self.assertEqual(resultado["registros_creados"], 1)
        self.assertTrue(registro.tiene_asistencia)
        self.assertTrue(registro.tiene_puntualidad)

    def test_checada_con_uso_tolerancia_mantiene_asistencia_y_quita_puntualidad(self):
        fecha = date(2026, 6, 2)
        periodo, empleado, bono = self.crear_bono(fecha)
        self.crear_asistencia(empleado, fecha)
        self.crear_incidencia(empleado, fecha, IncidenciaAsistencia.TIPO_USO_TOLERANCIA)

        sincronizar_asistencia_desde_checador(periodo)

        registro = RegistroDiarioProduccion.objects.get(bono=bono, dia=2)
        self.assertTrue(registro.tiene_asistencia)
        self.assertFalse(registro.tiene_puntualidad)

    def test_dia_sin_checada_quita_asistencia_y_puntualidad(self):
        fecha = date(2026, 6, 3)
        periodo, _empleado, bono = self.crear_bono(fecha)

        sincronizar_asistencia_desde_checador(periodo)

        registro = RegistroDiarioProduccion.objects.get(bono=bono, dia=3)
        self.assertFalse(registro.tiene_asistencia)
        self.assertFalse(registro.tiene_puntualidad)

    def test_sync_no_genera_ni_conserva_registros_antes_de_fecha_ingreso(self):
        periodo = ConfigBonoPeriodo.objects.create(
            mes=6,
            anio=2026,
            dias_laborables=11,
            fecha_inicio=date(2026, 6, 1),
            fecha_fin=date(2026, 6, 11),
        )
        empleado = Empleado.objects.create(
            nombre="Empleado Ingreso Produccion",
            area="HORNOS",
            fecha_ingreso=date(2026, 6, 10),
        )
        bono = BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_HORNOS)
        RegistroDiarioProduccion.objects.create(bono=bono, dia=1, tiene_asistencia=False, tiene_puntualidad=False)
        self.crear_asistencia(empleado, date(2026, 6, 10))
        self.crear_asistencia(empleado, date(2026, 6, 11))

        resultado = sincronizar_asistencia_desde_checador(periodo)

        self.assertEqual(resultado["registros_eliminados"], 1)
        self.assertEqual(resultado["registros_creados"], 2)
        self.assertFalse(RegistroDiarioProduccion.objects.filter(bono=bono, dia__lt=10).exists())
        self.assertEqual(
            list(RegistroDiarioProduccion.objects.filter(bono=bono).values_list("dia", flat=True).order_by("dia")),
            [10, 11],
        )

    def test_sync_no_genera_ni_conserva_registros_futuros_en_periodo_en_curso(self):
        periodo = ConfigBonoPeriodo.objects.create(
            mes=6,
            anio=2026,
            dias_laborables=27,
            fecha_inicio=date(2026, 6, 1),
            fecha_fin=date(2026, 6, 27),
        )
        empleado = Empleado.objects.create(
            nombre="Empleado Futuro Produccion",
            area="HORNOS",
            fecha_ingreso=date(2026, 1, 1),
        )
        bono = BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_HORNOS)
        RegistroDiarioProduccion.objects.create(bono=bono, dia=12, tiene_asistencia=False, tiene_puntualidad=False)

        with patch("bonos_produccion.services_checador.timezone.localdate", return_value=date(2026, 6, 11)):
            resultado = sincronizar_asistencia_desde_checador(periodo)

        self.assertEqual(resultado["registros_eliminados"], 1)
        self.assertFalse(RegistroDiarioProduccion.objects.filter(bono=bono, dia__gt=11).exists())

    def test_sync_dia_futuro_elimina_registro_existente_en_periodo_en_curso(self):
        periodo = ConfigBonoPeriodo.objects.create(
            mes=6,
            anio=2026,
            dias_laborables=27,
            fecha_inicio=date(2026, 6, 1),
            fecha_fin=date(2026, 6, 27),
        )
        empleado = Empleado.objects.create(
            nombre="Empleado Futuro Dia Produccion",
            area="HORNOS",
            fecha_ingreso=date(2026, 1, 1),
        )
        bono = BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_HORNOS)
        RegistroDiarioProduccion.objects.create(bono=bono, dia=12, tiene_asistencia=False, tiene_puntualidad=False)

        with patch("bonos_produccion.services_checador.timezone.localdate", return_value=date(2026, 6, 11)):
            resultado = sincronizar_empleado_dia_desde_checador(empleado.id, date(2026, 6, 12))

        self.assertEqual(resultado["registros_eliminados"], 1)
        self.assertFalse(RegistroDiarioProduccion.objects.filter(bono=bono, dia=12).exists())

    def test_suspension_quita_asistencia_y_puntualidad(self):
        fecha = date(2026, 6, 4)
        periodo, empleado, bono = self.crear_bono(fecha)
        self.crear_asistencia(empleado, fecha)
        self.crear_incidencia(
            empleado,
            fecha,
            IncidenciaAsistencia.TIPO_SUSPENSION,
            estado=IncidenciaAsistencia.ESTADO_CONCILIADO,
        )

        sincronizar_asistencia_desde_checador(periodo)

        registro = RegistroDiarioProduccion.objects.get(bono=bono, dia=4)
        self.assertFalse(registro.tiene_asistencia)
        self.assertFalse(registro.tiene_puntualidad)

    def test_suspension_resuelta_no_quita_asistencia_ni_puntualidad(self):
        fecha = date(2026, 6, 8)
        periodo, empleado, bono = self.crear_bono(fecha)
        self.crear_asistencia(empleado, fecha)
        self.crear_incidencia(
            empleado,
            fecha,
            IncidenciaAsistencia.TIPO_SUSPENSION,
            estado=IncidenciaAsistencia.ESTADO_RESUELTO,
        )

        sincronizar_asistencia_desde_checador(periodo)

        registro = RegistroDiarioProduccion.objects.get(bono=bono, dia=8)
        self.assertTrue(registro.tiene_asistencia)
        self.assertTrue(registro.tiene_puntualidad)

    def test_incidencia_conciliada_no_quita_puntualidad(self):
        fecha = date(2026, 6, 9)
        periodo, empleado, bono = self.crear_bono(fecha)
        self.crear_asistencia(empleado, fecha)
        self.crear_incidencia(
            empleado,
            fecha,
            IncidenciaAsistencia.TIPO_RETARDO,
            estado=IncidenciaAsistencia.ESTADO_CONCILIADO,
        )

        sincronizar_asistencia_desde_checador(periodo)

        registro = RegistroDiarioProduccion.objects.get(bono=bono, dia=9)
        self.assertTrue(registro.tiene_asistencia)
        self.assertTrue(registro.tiene_puntualidad)

    def test_registro_existente_conserva_campos_manuales(self):
        fecha = date(2026, 6, 5)
        periodo, empleado, bono = self.crear_bono(fecha)
        self.crear_asistencia(empleado, fecha)
        RegistroDiarioProduccion.objects.create(
            bono=bono,
            dia=5,
            tiene_uniforme=False,
            tiene_produccion=False,
            cantidad_embetunados=7,
            observacion="x",
            tiene_asistencia=False,
            tiene_puntualidad=False,
        )

        resultado = sincronizar_asistencia_desde_checador(periodo)

        registro = RegistroDiarioProduccion.objects.get(bono=bono, dia=5)
        self.assertEqual(resultado["registros_actualizados"], 1)
        self.assertTrue(registro.tiene_asistencia)
        self.assertTrue(registro.tiene_puntualidad)
        self.assertFalse(registro.tiene_uniforme)
        self.assertFalse(registro.tiene_produccion)
        self.assertEqual(registro.cantidad_embetunados, 7)
        self.assertEqual(registro.observacion, "x")

    def test_bono_extra_y_ajuste_positivo_se_conservan_tras_recalcular(self):
        fecha = date(2026, 6, 6)
        periodo, empleado, bono = self.crear_bono(
            fecha,
            bono_extra=Decimal("100.00"),
            ajuste_positivo=Decimal("50.00"),
        )
        self.crear_asistencia(empleado, fecha)

        sincronizar_asistencia_desde_checador(periodo)

        bono.refresh_from_db()
        self.assertEqual(bono.bono_extra, Decimal("100.00"))
        self.assertEqual(bono.ajuste_positivo, Decimal("50.00"))

    def test_bono_fuera_de_borrador_no_se_modifica(self):
        fecha = date(2026, 6, 7)
        periodo, _empleado, bono = self.crear_bono(fecha, estatus=BonoProduccionEmpleado.ESTATUS_CERRADO)
        RegistroDiarioProduccion.objects.create(
            bono=bono,
            dia=7,
            tiene_asistencia=True,
            tiene_puntualidad=True,
        )

        resultado = sincronizar_asistencia_desde_checador(periodo)

        registro = RegistroDiarioProduccion.objects.get(bono=bono, dia=7)
        self.assertEqual(resultado["bonos_sincronizados"], 0)
        self.assertEqual(resultado["bonos_omitidos"], 1)
        self.assertTrue(registro.tiene_asistencia)
        self.assertTrue(registro.tiene_puntualidad)

    def test_usuario_solo_captura_no_puede_sync_checador_por_api(self):
        fecha = date(2026, 6, 10)
        periodo, _empleado, _bono = self.crear_bono(fecha)
        user = User.objects.create_user(username="captura", password="test")
        user.groups.add(Group.objects.get_or_create(name=ROLE_BONOS_PRODUCCION_CAPTURA)[0])
        self.client.force_login(user)

        response = self.client.post(reverse("bonoproduccion-periodo-sync-checador", args=[periodo.id]), {})

        self.assertEqual(response.status_code, 403)

    def test_sync_empleado_dia_crea_solo_el_registro_del_dia_afectado(self):
        fecha = date(2026, 6, 11)
        _periodo, empleado, bono = self.crear_bono(fecha)
        self.crear_asistencia(empleado, fecha)

        resultado = sincronizar_empleado_dia_desde_checador(empleado.id, fecha)

        self.assertEqual(resultado["bonos_sincronizados"], 1)
        self.assertEqual(resultado["registros_creados"], 1)
        self.assertEqual(RegistroDiarioProduccion.objects.filter(bono=bono).count(), 1)
        registro = RegistroDiarioProduccion.objects.get(bono=bono, dia=11)
        self.assertTrue(registro.tiene_asistencia)
        self.assertTrue(registro.tiene_puntualidad)

    def test_sync_empleado_dia_no_modifica_bono_cerrado(self):
        fecha = date(2026, 6, 12)
        _periodo, empleado, bono = self.crear_bono(fecha, estatus=BonoProduccionEmpleado.ESTATUS_CERRADO)
        self.crear_asistencia(empleado, fecha)

        resultado = sincronizar_empleado_dia_desde_checador(empleado.id, fecha)

        self.assertEqual(resultado["bonos_sincronizados"], 0)
        self.assertEqual(resultado["bonos_omitidos"], 1)
        self.assertFalse(RegistroDiarioProduccion.objects.filter(bono=bono, dia=12).exists())

    def test_puente_rrhh_programa_sync_bonos_al_confirmar_checada(self):
        fecha = date(2026, 6, 13)
        _periodo, empleado, bono = self.crear_bono(fecha)
        self.crear_asistencia(empleado, fecha)

        with self.captureOnCommitCallbacks(execute=True):
            programar_sincronizacion_bonos_desde_checador(empleado.id, fecha)

        registro = RegistroDiarioProduccion.objects.get(bono=bono, dia=13)
        self.assertTrue(registro.tiene_asistencia)
        self.assertTrue(registro.tiene_puntualidad)

    def test_sync_empleado_dia_respeta_rango_personalizado_del_periodo(self):
        fecha_periodo = date(2026, 6, 14)
        fecha_fuera = date(2026, 6, 15)
        _periodo, empleado, bono = self.crear_bono(fecha_periodo)
        self.crear_asistencia(empleado, fecha_fuera)

        resultado = sincronizar_empleado_dia_desde_checador(empleado.id, fecha_fuera)

        self.assertEqual(resultado["bonos_sincronizados"], 0)
        self.assertFalse(RegistroDiarioProduccion.objects.filter(bono=bono, dia=15).exists())

    def test_sync_incluye_dias_del_corte_que_caen_en_el_mes_anterior(self):
        periodo = ConfigBonoPeriodo.objects.create(
            mes=9,
            anio=2026,
            dias_laborables=5,
            fecha_inicio=date(2026, 8, 28),
            fecha_fin=date(2026, 9, 2),
        )
        empleado = Empleado.objects.create(
            nombre="Empleado corte cruzado",
            area="HORNOS",
            fecha_ingreso=date(2026, 1, 1),
        )
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area=AREA_HORNOS,
        )
        for fecha in (
            date(2026, 8, 28),
            date(2026, 8, 29),
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
        ):
            self.crear_asistencia(empleado, fecha)

        sincronizar_asistencia_desde_checador(periodo)

        bono.refresh_from_db()
        self.assertEqual(bono.dias_trabajados, 5)
        self.assertFalse(bono.cancela_bono)
        self.assertEqual(
            list(bono.registros.values_list("dia", flat=True).order_by("dia")),
            [1, 2, 28, 29, 30, 31],
        )

    def test_sync_dia_encuentra_periodo_cuyo_corte_inicia_en_mes_anterior(self):
        periodo = ConfigBonoPeriodo.objects.create(
            mes=9,
            anio=2026,
            dias_laborables=23,
            fecha_inicio=date(2026, 8, 28),
            fecha_fin=date(2026, 9, 26),
        )
        empleado = Empleado.objects.create(
            nombre="Empleado puente corte",
            area="HORNOS",
            fecha_ingreso=date(2026, 1, 1),
        )
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area=AREA_HORNOS,
        )
        fecha = date(2026, 8, 28)
        self.crear_asistencia(empleado, fecha)

        resultado = sincronizar_empleado_dia_desde_checador(empleado.id, fecha)

        self.assertEqual(resultado["bonos_sincronizados"], 1)
        self.assertTrue(
            RegistroDiarioProduccion.objects.get(bono=bono, dia=28).tiene_asistencia
        )

    def test_sync_corte_cruzado_no_crea_faltas_para_fechas_futuras(self):
        periodo = ConfigBonoPeriodo.objects.create(
            mes=9,
            anio=2026,
            dias_laborables=23,
            fecha_inicio=date(2026, 8, 28),
            fecha_fin=date(2026, 9, 26),
        )
        empleado = Empleado.objects.create(
            nombre="Empleado corte en curso",
            area="HORNOS",
            fecha_ingreso=date(2026, 1, 1),
        )
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area=AREA_HORNOS,
        )

        with patch("bonos_produccion.services_checador.timezone.localdate", return_value=date(2026, 8, 29)):
            sincronizar_asistencia_desde_checador(periodo)

        self.assertEqual(
            list(bono.registros.values_list("dia", flat=True).order_by("dia")),
            [28, 29],
        )

    def test_captura_expone_rango_y_acepta_dia_del_mes_anterior(self):
        periodo = ConfigBonoPeriodo.objects.create(
            mes=9,
            anio=2026,
            dias_laborables=23,
            fecha_inicio=date(2026, 8, 28),
            fecha_fin=date(2026, 9, 26),
        )
        empleado = Empleado.objects.create(
            nombre="Empleado captura corte",
            area="HORNOS",
            fecha_ingreso=date(2026, 1, 1),
        )
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area=AREA_HORNOS,
        )

        payload = BonoProduccionCapturaSerializer(bono).data
        self.assertEqual(payload["periodo_fecha_inicio"], "2026-08-28")
        self.assertEqual(payload["periodo_fecha_fin"], "2026-09-26")

        serializer = RegistroDiarioCapturaSerializer(
            data={"bono": bono.id, "dia": 31},
            context={"bonos_permitidos": BonoProduccionEmpleado.objects.filter(pk=bono.pk)},
        )
        self.assertTrue(serializer.is_valid(), serializer.errors)
