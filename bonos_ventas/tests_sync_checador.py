from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from zoneinfo import ZoneInfo

from django.test import TestCase

from core.models import Sucursal
from rrhh.models import AsistenciaEmpleado, Empleado, IncidenciaAsistencia

from .models import BonoVentasEmpleado, ConfigBonoVentasPeriodo, RegistroDiarioVentas
from .services_checador import sincronizar_asistencia_desde_checador, sincronizar_empleado_dia_desde_checador


TZ = ZoneInfo("America/Mazatlan")


def dt_local(fecha: date, hora: time = time(8, 0)) -> datetime:
    return datetime(fecha.year, fecha.month, fecha.day, hora.hour, hora.minute, tzinfo=TZ)


class SyncChecadorVentasTests(TestCase):
    def crear_bono(self, fecha: date, **bono_kwargs):
        sucursal = Sucursal.objects.create(codigo=f"S{fecha.day:02d}", nombre=f"Sucursal {fecha.day}", activa=True)
        periodo = ConfigBonoVentasPeriodo.objects.create(
            mes=fecha.month,
            anio=fecha.year,
            dias_laborables=1,
            fecha_inicio=fecha,
            fecha_fin=fecha,
        )
        empleado = Empleado.objects.create(
            nombre=f"Empleado Ventas {fecha.day}",
            area="VENTAS",
            sucursal=sucursal.nombre,
        )
        bono = BonoVentasEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            sucursal=sucursal,
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

    def test_checada_normal_sin_incidencias_crea_asistencia_y_puntualidad_true(self):
        fecha = date(2026, 6, 1)
        periodo, empleado, bono = self.crear_bono(fecha)
        self.crear_asistencia(empleado, fecha)

        resultado = sincronizar_asistencia_desde_checador(periodo)

        registro = RegistroDiarioVentas.objects.get(bono=bono, dia=1)
        self.assertEqual(resultado["registros_creados"], 1)
        self.assertTrue(registro.tiene_asistencia)
        self.assertTrue(registro.tiene_puntualidad)

    def test_checada_con_uso_tolerancia_mantiene_asistencia_y_quita_puntualidad(self):
        fecha = date(2026, 6, 2)
        periodo, empleado, bono = self.crear_bono(fecha)
        self.crear_asistencia(empleado, fecha)
        self.crear_incidencia(empleado, fecha, IncidenciaAsistencia.TIPO_USO_TOLERANCIA)

        sincronizar_asistencia_desde_checador(periodo)

        registro = RegistroDiarioVentas.objects.get(bono=bono, dia=2)
        self.assertTrue(registro.tiene_asistencia)
        self.assertFalse(registro.tiene_puntualidad)

    def test_dia_sin_checada_quita_asistencia_y_puntualidad(self):
        fecha = date(2026, 6, 3)
        periodo, _empleado, bono = self.crear_bono(fecha)

        sincronizar_asistencia_desde_checador(periodo)

        registro = RegistroDiarioVentas.objects.get(bono=bono, dia=3)
        self.assertFalse(registro.tiene_asistencia)
        self.assertFalse(registro.tiene_puntualidad)

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

        registro = RegistroDiarioVentas.objects.get(bono=bono, dia=4)
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

        registro = RegistroDiarioVentas.objects.get(bono=bono, dia=8)
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

        registro = RegistroDiarioVentas.objects.get(bono=bono, dia=9)
        self.assertTrue(registro.tiene_asistencia)
        self.assertTrue(registro.tiene_puntualidad)

    def test_registro_existente_conserva_campos_manuales(self):
        fecha = date(2026, 6, 5)
        periodo, empleado, bono = self.crear_bono(fecha)
        self.crear_asistencia(empleado, fecha)
        RegistroDiarioVentas.objects.create(
            bono=bono,
            dia=5,
            tiene_uniforme=False,
            puntos_de_vista="x",
            tiene_asistencia=False,
            tiene_puntualidad=False,
        )

        resultado = sincronizar_asistencia_desde_checador(periodo)

        registro = RegistroDiarioVentas.objects.get(bono=bono, dia=5)
        self.assertEqual(resultado["registros_actualizados"], 1)
        self.assertTrue(registro.tiene_asistencia)
        self.assertTrue(registro.tiene_puntualidad)
        self.assertFalse(registro.tiene_uniforme)
        self.assertEqual(registro.puntos_de_vista, "x")

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
        periodo, _empleado, bono = self.crear_bono(fecha, estatus="CERRADO")
        RegistroDiarioVentas.objects.create(
            bono=bono,
            dia=7,
            tiene_asistencia=True,
            tiene_puntualidad=True,
        )

        resultado = sincronizar_asistencia_desde_checador(periodo)

        registro = RegistroDiarioVentas.objects.get(bono=bono, dia=7)
        self.assertEqual(resultado["bonos_sincronizados"], 0)
        self.assertEqual(resultado["bonos_omitidos"], 1)
        self.assertTrue(registro.tiene_asistencia)
        self.assertTrue(registro.tiene_puntualidad)

    def test_sync_empleado_dia_crea_solo_el_registro_del_dia_afectado(self):
        fecha = date(2026, 6, 11)
        _periodo, empleado, bono = self.crear_bono(fecha)
        self.crear_asistencia(empleado, fecha)

        resultado = sincronizar_empleado_dia_desde_checador(empleado.id, fecha)

        self.assertEqual(resultado["bonos_sincronizados"], 1)
        self.assertEqual(resultado["registros_creados"], 1)
        self.assertEqual(RegistroDiarioVentas.objects.filter(bono=bono).count(), 1)
        registro = RegistroDiarioVentas.objects.get(bono=bono, dia=11)
        self.assertTrue(registro.tiene_asistencia)
        self.assertTrue(registro.tiene_puntualidad)

    def test_sync_empleado_dia_no_modifica_bono_cerrado(self):
        fecha = date(2026, 6, 12)
        _periodo, empleado, bono = self.crear_bono(fecha, estatus="CERRADO")
        self.crear_asistencia(empleado, fecha)

        resultado = sincronizar_empleado_dia_desde_checador(empleado.id, fecha)

        self.assertEqual(resultado["bonos_sincronizados"], 0)
        self.assertEqual(resultado["bonos_omitidos"], 1)
        self.assertFalse(RegistroDiarioVentas.objects.filter(bono=bono, dia=12).exists())

    def test_periodo_cruza_mes_no_convierte_mayo_en_dias_futuros_de_junio(self):
        sucursal = Sucursal.objects.create(codigo="CRUCE", nombre="Sucursal Cruce", activa=True)
        periodo = ConfigBonoVentasPeriodo.objects.create(
            mes=6,
            anio=2026,
            dias_laborables=31,
            fecha_inicio=date(2026, 5, 28),
            fecha_fin=date(2026, 6, 27),
        )
        empleado = Empleado.objects.create(nombre="Empleado Cruce Mes", area="VENTAS", sucursal=sucursal.nombre)
        bono = BonoVentasEmpleado.objects.create(periodo=periodo, empleado=empleado, sucursal=sucursal)
        self.crear_asistencia(empleado, date(2026, 5, 28))
        self.crear_asistencia(empleado, date(2026, 6, 1))

        resultado = sincronizar_asistencia_desde_checador(periodo)

        self.assertEqual(resultado["registros_creados"], 27)
        self.assertTrue(RegistroDiarioVentas.objects.filter(bono=bono, dia=1).exists())
        self.assertFalse(RegistroDiarioVentas.objects.filter(bono=bono, dia=28).exists())

    def test_sync_empleado_dia_no_impacta_periodo_junio_con_fecha_de_mayo(self):
        sucursal = Sucursal.objects.create(codigo="MAY", nombre="Sucursal Mayo", activa=True)
        periodo = ConfigBonoVentasPeriodo.objects.create(
            mes=6,
            anio=2026,
            dias_laborables=31,
            fecha_inicio=date(2026, 5, 28),
            fecha_fin=date(2026, 6, 27),
        )
        empleado = Empleado.objects.create(nombre="Empleado Mayo", area="VENTAS", sucursal=sucursal.nombre)
        bono = BonoVentasEmpleado.objects.create(periodo=periodo, empleado=empleado, sucursal=sucursal)
        self.crear_asistencia(empleado, date(2026, 5, 28))

        resultado = sincronizar_empleado_dia_desde_checador(empleado.id, date(2026, 5, 28))

        self.assertEqual(resultado["bonos_sincronizados"], 0)
        self.assertFalse(RegistroDiarioVentas.objects.filter(bono=bono).exists())
