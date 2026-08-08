import json

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase, override_settings

from core.access import ROLE_BONOS_PRODUCCION_CAPTURA
from rrhh.models import Empleado, PermisoSalida

from .solicitudes import empleados_operables_solicitudes_produccion


@override_settings(SECURE_SSL_REDIRECT=False)
class OperadoraCatalogoPermisosTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        users = get_user_model().objects
        cls.carolina_user = users.create_user(username="carolina.cayetano", password="test12345")
        cls.carolina = Empleado.objects.create(
            codigo="CAR-001",
            nombre="CAYETANO VALENZUELA CAROLINA",
            departamento=Empleado.DEP_PRODUCCION,
            departamento_origen=Empleado.DEP_PRODUCCION,
            activo=True,
            usuario_erp=cls.carolina_user,
        )
        cls.operadora = users.create_user(username="rosa.cervantes", password="test12345")
        grupo = Group.objects.create(name=ROLE_BONOS_PRODUCCION_CAPTURA)
        cls.operadora.groups.add(grupo)
        cls.operadora_empleado = Empleado.objects.create(
            codigo="348",
            nombre="CERVANTES GUTIERREZ ROSA ICELA",
            departamento=Empleado.DEP_PRODUCCION,
            departamento_origen=Empleado.DEP_PRODUCCION,
            activo=True,
            usuario_erp=cls.operadora,
            jefe_directo=cls.carolina,
        )
        cls.produccion_empleado = Empleado.objects.create(
            codigo="PROD-001",
            nombre="COLABORADORA PRODUCCION",
            departamento=Empleado.DEP_PRODUCCION,
            activo=True,
            jefe_directo=cls.carolina,
        )
        cls.inactivo = Empleado.objects.create(
            codigo="PROD-002",
            nombre="COLABORADORA INACTIVA",
            departamento=Empleado.DEP_PRODUCCION,
            activo=False,
            jefe_directo=cls.carolina,
        )
        cls.ventas = Empleado.objects.create(
            codigo="VEN-001",
            nombre="COLABORADORA VENTAS",
            departamento=Empleado.DEP_VENTAS,
            activo=True,
            jefe_directo=cls.carolina,
        )

    def setUp(self):
        self.client.force_login(self.operadora)

    def _payload_permiso(self, empleado):
        return {
            "empleado": empleado.id,
            "area": "PRODUCCION",
            "tipo": PermisoSalida.TIPO_PERMISO_HORA,
            "fecha_inicio": "2026-08-11T12:00:00",
            "fecha_fin": "2026-08-11T13:00:00",
            "goce_sueldo": True,
            "motivo": "Cita medica",
        }

    def test_catalogo_solo_incluye_produccion_activa_con_jefa_erp(self):
        ids = set(empleados_operables_solicitudes_produccion().values_list("id", flat=True))

        self.assertEqual(ids, {self.operadora_empleado.id, self.produccion_empleado.id})

    def test_operadora_puede_solicitar_pero_nunca_gestionar(self):
        response = self.client.get("/api/bonos-produccion/permisos/?area=PRODUCCION")

        self.assertEqual(response.status_code, 200)
        empleados = response.json()["empleados"]
        self.assertEqual({item["id"] for item in empleados}, {self.operadora_empleado.id, self.produccion_empleado.id})
        self.assertTrue(all(item["puede_solicitar"] for item in empleados))
        self.assertTrue(all(not item["puede_gestionar"] for item in empleados))

    def test_operadora_captura_permiso_propio_y_ajeno(self):
        for empleado in (self.operadora_empleado, self.produccion_empleado):
            response = self.client.post(
                "/api/bonos-produccion/permisos/",
                json.dumps(self._payload_permiso(empleado)),
                content_type="application/json",
            )
            self.assertEqual(response.status_code, 201)
            permiso = PermisoSalida.objects.get(pk=response.json()["id"])
            self.assertEqual(permiso.empleado, empleado)
            self.assertEqual(permiso.estado, PermisoSalida.ESTADO_SOLICITADO)
            self.assertEqual(permiso.origen_solicitud, PermisoSalida.ORIGEN_BONOS_PRODUCCION)

    def test_operadora_no_captura_para_inactivo_ni_ventas(self):
        for empleado in (self.inactivo, self.ventas):
            response = self.client.post(
                "/api/bonos-produccion/permisos/",
                json.dumps(self._payload_permiso(empleado)),
                content_type="application/json",
            )
            self.assertIn(response.status_code, {400, 403})
        self.assertFalse(PermisoSalida.objects.exists())

    def test_operadora_no_edita_elimina_preautoriza_ni_rechaza(self):
        create_response = self.client.post(
            "/api/bonos-produccion/permisos/",
            json.dumps(self._payload_permiso(self.operadora_empleado)),
            content_type="application/json",
        )
        self.assertEqual(create_response.status_code, 201)
        permiso = PermisoSalida.objects.get(pk=create_response.json()["id"])
        estado_inicial = (permiso.estado, permiso.estado_jefe)

        for accion in ("editar", "eliminar", "preautorizar", "rechazar"):
            response = self.client.post(
                f"/api/bonos-produccion/permisos/{permiso.id}/{accion}/",
                json.dumps({"motivo_cambio": "Intento no autorizado"}),
                content_type="application/json",
            )
            self.assertEqual(response.status_code, 403, accion)

        permiso.refresh_from_db()
        self.assertEqual((permiso.estado, permiso.estado_jefe), estado_inicial)

    def test_lista_operadora_no_expone_permisos_ajenos(self):
        PermisoSalida.objects.create(
            empleado=self.produccion_empleado,
            tipo=PermisoSalida.TIPO_PERMISO_HORA,
            fecha_inicio="2026-08-11T12:00:00Z",
            motivo="Registro ajeno",
            origen_solicitud=PermisoSalida.ORIGEN_BONOS_PRODUCCION,
        )

        response = self.client.get("/api/bonos-produccion/permisos/?area=PRODUCCION")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["permisos"], [])
