import json
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase, override_settings

from core.access import ROLE_BONOS_PRODUCCION_CAPTURA
from rrhh.models import Empleado, PermisoSalida, Prestamo

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

    def test_operadora_no_accede_a_bonos_configuracion_registros_ni_horas_extra(self):
        rutas = (
            "/api/bonos-produccion/periodos/",
            "/api/bonos-produccion/bonos/",
            "/api/bonos-produccion/registros-diarios/",
            "/api/bonos-produccion/horas-extra/",
        )

        for ruta in rutas:
            with self.subTest(ruta=ruta):
                self.assertEqual(self.client.get(ruta).status_code, 403)


@override_settings(SECURE_SSL_REDIRECT=False)
class OperadoraPrestamosApiTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        users = get_user_model().objects
        cls.carolina_user = users.create_user(username="carolina.prestamos", password="test12345")
        cls.carolina = Empleado.objects.create(
            codigo="CAR-API",
            nombre="CAROLINA PRESTAMOS",
            departamento=Empleado.DEP_PRODUCCION,
            activo=True,
            usuario_erp=cls.carolina_user,
        )
        cls.operadora = users.create_user(username="julissa.angulo", password="test12345")
        grupo = Group.objects.create(name=ROLE_BONOS_PRODUCCION_CAPTURA)
        cls.operadora.groups.add(grupo)
        cls.operadora_empleado = Empleado.objects.create(
            codigo="306",
            nombre="ANGULO PARRA JULISSA",
            departamento=Empleado.DEP_PRODUCCION,
            activo=True,
            usuario_erp=cls.operadora,
            jefe_directo=cls.carolina,
        )
        cls.produccion_empleado = Empleado.objects.create(
            codigo="PROD-API",
            nombre="COLABORADORA PRODUCCION API",
            departamento_origen=Empleado.DEP_PRODUCCION,
            activo=True,
            jefe_directo=cls.carolina,
        )
        cls.ventas = Empleado.objects.create(
            codigo="VEN-API",
            nombre="COLABORADORA VENTAS API",
            departamento=Empleado.DEP_VENTAS,
            activo=True,
            jefe_directo=cls.carolina,
        )
        cls.otro_usuario = users.create_user(username="otra.operadora", password="test12345")

    def setUp(self):
        self.client.force_login(self.operadora)

    def _payload(self, empleado, **overrides):
        payload = {
            "empleado": empleado.id,
            "concepto": "Apoyo personal",
            "metodo_pago": Prestamo.METODO_TRANSFERENCIA,
            "fecha_deposito": "2026-08-20",
            "importe": "1200.00",
            "num_quincenas": 4,
        }
        payload.update(overrides)
        return payload

    def _post(self, empleado, **overrides):
        return self.client.post(
            "/api/bonos-produccion/prestamos/",
            json.dumps(self._payload(empleado, **overrides)),
            content_type="application/json",
        )

    @patch("rrhh.services_prestamos.notificar_prestamo_solicitado")
    def test_operadora_crea_prestamo_propio_y_ajeno_en_solicitado(self, notificar):
        for empleado in (self.operadora_empleado, self.produccion_empleado):
            response = self._post(empleado)

            self.assertEqual(response.status_code, 201)
            prestamo = Prestamo.objects.get(pk=response.json()["id"])
            self.assertEqual(prestamo.empleado, empleado)
            self.assertEqual(prestamo.estado, Prestamo.ESTADO_SOLICITADO)
            self.assertEqual(prestamo.jefe_directo, self.carolina_user)
            self.assertEqual(prestamo.creado_por, self.operadora)
            prestamo.saldo_actual = Decimal("0.00")
            prestamo.estado = Prestamo.ESTADO_LIQUIDADO
            prestamo.save(update_fields=["saldo_actual", "estado"])

        self.assertEqual(notificar.call_count, 2)

    def test_operadora_no_crea_para_otro_departamento(self):
        response = self._post(self.ventas)

        self.assertIn(response.status_code, {400, 403})
        self.assertFalse(Prestamo.objects.exists())

    def test_operadora_no_crea_segundo_prestamo_con_saldo(self):
        primer_response = self._post(self.produccion_empleado)
        self.assertEqual(primer_response.status_code, 201)

        segundo_response = self._post(self.produccion_empleado, concepto="Segundo intento")

        self.assertEqual(segundo_response.status_code, 400)
        self.assertEqual(Prestamo.objects.filter(empleado=self.produccion_empleado).count(), 1)

    def test_servidor_ignora_estado_firmas_y_autorizadores_enviados(self):
        response = self._post(
            self.produccion_empleado,
            estado=Prestamo.ESTADO_ACTIVO,
            saldo_actual="0.00",
            jefe_directo=self.otro_usuario.id,
            firma_jefe=True,
            autorizado_dg=self.otro_usuario.id,
        )

        self.assertEqual(response.status_code, 201)
        prestamo = Prestamo.objects.get(pk=response.json()["id"])
        self.assertEqual(prestamo.estado, Prestamo.ESTADO_SOLICITADO)
        self.assertEqual(prestamo.saldo_actual, Decimal("1200.00"))
        self.assertEqual(prestamo.jefe_directo, self.carolina_user)
        self.assertFalse(prestamo.firma_jefe)
        self.assertIsNone(prestamo.autorizado_dg)

    def test_lista_solo_muestra_creados_por_operadora_o_beneficio_propio(self):
        visible_creado = Prestamo.objects.create(
            empleado=self.produccion_empleado,
            concepto="Capturado por operadora",
            fecha_solicitud=date(2026, 8, 8),
            importe=Decimal("100.00"),
            num_quincenas=1,
            descuento_quincenal=Decimal("100.00"),
            saldo_actual=Decimal("100.00"),
            creado_por=self.operadora,
            jefe_directo=self.carolina_user,
        )
        visible_propio = Prestamo.objects.create(
            empleado=self.operadora_empleado,
            concepto="Beneficio propio",
            fecha_solicitud=date(2026, 8, 8),
            importe=Decimal("200.00"),
            num_quincenas=2,
            descuento_quincenal=Decimal("100.00"),
            saldo_actual=Decimal("200.00"),
            creado_por=self.otro_usuario,
            jefe_directo=self.carolina_user,
        )
        Prestamo.objects.create(
            empleado=self.produccion_empleado,
            concepto="Historial financiero ajeno",
            fecha_solicitud=date(2026, 8, 8),
            importe=Decimal("300.00"),
            num_quincenas=3,
            descuento_quincenal=Decimal("100.00"),
            saldo_actual=Decimal("300.00"),
            creado_por=self.otro_usuario,
            jefe_directo=self.carolina_user,
        )

        response = self.client.get("/api/bonos-produccion/prestamos/")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        rows = body.get("results", body) if isinstance(body, dict) else body
        self.assertEqual({row["id"] for row in rows}, {visible_creado.id, visible_propio.id})

    def test_endpoint_no_permite_editar_eliminar_ni_autorizar(self):
        create_response = self._post(self.produccion_empleado)
        self.assertEqual(create_response.status_code, 201)
        prestamo_id = create_response.json()["id"]

        self.assertEqual(
            self.client.patch(
                f"/api/bonos-produccion/prestamos/{prestamo_id}/",
                json.dumps({"concepto": "Alterado"}),
                content_type="application/json",
            ).status_code,
            405,
        )
        self.assertEqual(self.client.delete(f"/api/bonos-produccion/prestamos/{prestamo_id}/").status_code, 405)
        self.assertEqual(
            self.client.post(f"/api/bonos-produccion/prestamos/{prestamo_id}/autorizar-jefe/").status_code,
            404,
        )


@override_settings(SECURE_SSL_REDIRECT=False)
class OperadoraPwaTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        users = get_user_model().objects
        cls.operadora = users.create_user(username="operadora.pwa", password="test12345")
        grupo = Group.objects.create(name=ROLE_BONOS_PRODUCCION_CAPTURA)
        cls.operadora.groups.add(grupo)
        cls.admin = users.create_superuser(username="admin.pwa", password="test12345", email="admin@example.com")

    def test_pwa_marca_operadora_y_contiene_formulario_de_prestamos(self):
        self.client.force_login(self.operadora)

        response = self.client.get("/bonos-produccion/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'data-operadora-solicitudes="true"')
        self.assertContains(response, "const OPERADOR_SOLICITUDES=true")
        self.assertContains(response, "function PrestamosTab")
        self.assertContains(response, "/api/bonos-produccion/prestamos/")

    def test_pwa_administrativa_no_se_marca_como_operadora_acotada(self):
        self.client.force_login(self.admin)

        response = self.client.get("/bonos-produccion/app/?captura=1")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'data-operadora-solicitudes="false"')
        self.assertContains(response, "const OPERADOR_SOLICITUDES=false")
