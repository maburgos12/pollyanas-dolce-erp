from __future__ import annotations

from datetime import date, timedelta
from importlib import import_module

from django.apps import apps as app_registry
from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from rrhh.models import AsistenciaEmpleado, Empleado, EmpleadoBaja
from rrhh.services_hikvision import _baja_bloquea_marca, _resolver_sucursal, procesar_eventos_hik


class SucursalDelChecadorTests(TestCase):
    """Hay UN solo checador y está en Matriz/CEDIS: la marca manda sobre la asignación."""

    def setUp(self):
        self.matriz, _ = Sucursal.objects.get_or_create(codigo="MATRIZ", defaults={"nombre": "Sucursal Matriz", "activa": True})
        self.cedis, _ = Sucursal.objects.get_or_create(codigo="CEDIS", defaults={"nombre": "CEDIS", "activa": True})
        self.crucero, _ = Sucursal.objects.get_or_create(codigo="CRUCERO", defaults={"nombre": "Sucursal Bamoa", "activa": True})

    def test_asignacion_vencida_cede_ante_el_aparato(self):
        # Caso real: Laiza y Sara seguían asignadas a Crucero (ubicación que dejó
        # de existir) mientras checaban en el checador de Matriz.
        emp = Empleado.objects.create(
            nombre="Laiza", area="PRODUCCION", sucursal="Sucursal Crucero", sucursal_ref=self.crucero,
        )

        self.assertEqual(_resolver_sucursal(emp), self.matriz)

    def test_rotativo_sin_sucursal_asignada_cae_en_matriz(self):
        # Personal rotativo (Perla): sin sucursal fija, antes quedaba en NULL.
        emp = Empleado.objects.create(nombre="Perla", area="VENTAS", sucursal="")

        self.assertEqual(_resolver_sucursal(emp), self.matriz)

    def test_cedis_se_conserva_por_compartir_sitio_fisico(self):
        emp = Empleado.objects.create(nombre="Cedis", area="LOGISTICA", sucursal="", sucursal_ref=self.cedis)

        self.assertEqual(_resolver_sucursal(emp), self.cedis)

    def test_migracion_normaliza_filas_del_checador_y_respeta_las_demas(self):
        normalizar = import_module(
            "rrhh.migrations.0042_normalizar_sucursal_asistencia_checador"
        ).normalizar_sucursal_checador
        emp = Empleado.objects.create(nombre="Laiza", area="PRODUCCION", sucursal="", sucursal_ref=self.crucero)

        vencida = AsistenciaEmpleado.objects.create(
            empleado=emp, fecha=date(2026, 7, 21), sucursal=self.crucero,
            fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
        )
        hueca = AsistenciaEmpleado.objects.create(
            empleado=emp, fecha=date(2026, 7, 22), fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
        )
        ya_cedis = AsistenciaEmpleado.objects.create(
            empleado=emp, fecha=date(2026, 7, 23), sucursal=self.cedis,
            fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
        )
        de_point = AsistenciaEmpleado.objects.create(
            empleado=emp, fecha=date(2026, 7, 24), sucursal=self.crucero,
            fuente=AsistenciaEmpleado.FUENTE_POINT,
        )

        normalizar(app_registry, None)

        for fila in (vencida, hueca, ya_cedis, de_point):
            fila.refresh_from_db()
        self.assertEqual(vencida.sucursal_id, self.matriz.id)
        self.assertEqual(hueca.sucursal_id, self.matriz.id)
        self.assertEqual(ya_cedis.sucursal_id, self.cedis.id)   # mismo sitio, se respeta
        self.assertEqual(de_point.sucursal_id, self.crucero.id)  # Point manda en lo suyo


class BajaBloqueaChecadorTests(TestCase):
    """Un dado de baja ya no genera asistencia, pero conserva su último día."""

    def setUp(self):
        Sucursal.objects.get_or_create(codigo="MATRIZ", defaults={"nombre": "Sucursal Matriz", "activa": True})
        self.emp = Empleado.objects.create(
            nombre="Laura", area="VENTAS", sucursal="", codigo="307",
            fecha_ingreso=date(2025, 1, 1),
        )

    def _dar_de_baja(self, fecha_baja):
        return EmpleadoBaja.objects.create(
            empleado=self.emp, nombre=self.emp.nombre,
            fecha_ingreso=date(2025, 1, 1), fecha_baja=fecha_baja,
        )

    def test_activo_nunca_se_bloquea(self):
        self.assertFalse(_baja_bloquea_marca(self.emp, date(2026, 7, 25)))

    def test_ultimo_dia_trabajado_se_conserva(self):
        self._dar_de_baja(date(2026, 7, 24))
        self.emp.refresh_from_db()

        self.assertFalse(_baja_bloquea_marca(self.emp, date(2026, 7, 24)))

    def test_marca_posterior_a_la_baja_se_bloquea(self):
        self._dar_de_baja(date(2026, 7, 24))
        self.emp.refresh_from_db()

        self.assertTrue(_baja_bloquea_marca(self.emp, date(2026, 7, 25)))

    def test_inactivo_sin_registro_de_baja_se_bloquea(self):
        self.emp.activo = False
        self.emp.save()

        self.assertTrue(_baja_bloquea_marca(self.emp, date(2026, 7, 25)))

    def test_procesar_eventos_no_crea_asistencia_de_dado_de_baja(self):
        self._dar_de_baja(timezone.localdate() - timedelta(days=1))
        marca = timezone.localtime().replace(hour=8, minute=0, second=0, microsecond=0)

        resultado = procesar_eventos_hik(
            [{"employee_no": "307", "attendance_status": "checkIn", "time": marca.isoformat()}]
        )

        self.assertEqual(resultado["descartadas_baja"], 1)
        self.assertEqual(resultado["procesados"], 0)
        self.assertFalse(AsistenciaEmpleado.objects.filter(empleado=self.emp).exists())
