from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import Sucursal
from rrhh.models import Empleado


class RRHHAsignacionSucursalTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(
            username="rrhh-admin",
            email="rrhh@example.com",
            password="test",
        )
        self.client.force_login(self.user)

    def test_patch_asignar_sucursal_por_id_escribe_fk_canonico(self):
        # Fuente canónica (FASE 2): asignar escribe el FK sucursal_ref, no solo el texto.
        sucursal, _ = Sucursal.objects.update_or_create(codigo="PAY-TEST", defaults={"nombre": "Sucursal Payan", "activa": True})
        empleado = Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS", sucursal="")

        response = self.client.patch(
            f"/api/rrhh/empleados/{empleado.id}/asignar-sucursal/",
            data={"sucursal_id": sucursal.id},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        empleado.refresh_from_db()
        self.assertEqual(empleado.sucursal_ref_id, sucursal.id)
        self.assertEqual(empleado.sucursal, "Sucursal Payan")
        self.assertEqual(response.json()["sucursal_ref"], sucursal.id)

    def test_patch_asignar_sucursal_por_nombre_pese_a_rename(self):
        # Compat: aceptar nombre viejo/acentuado y resolverlo al FK (sin match exacto).
        sucursal, _ = Sucursal.objects.update_or_create(codigo="GUAMUCHIL", defaults={"nombre": "Sucursal Guamuchil", "activa": True})
        empleado = Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS", sucursal="")

        response = self.client.patch(
            f"/api/rrhh/empleados/{empleado.id}/asignar-sucursal/",
            data={"sucursal": "Guamúchil"},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        empleado.refresh_from_db()
        self.assertEqual(empleado.sucursal_ref_id, sucursal.id)

    def test_patch_quitar_asignacion_limpia_fk_y_texto(self):
        sucursal, _ = Sucursal.objects.update_or_create(codigo="PAY-TEST", defaults={"nombre": "Sucursal Payan", "activa": True})
        empleado = Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS", sucursal="Sucursal Payan", sucursal_ref=sucursal)

        response = self.client.patch(
            f"/api/rrhh/empleados/{empleado.id}/asignar-sucursal/",
            data={"sucursal_id": None},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        empleado.refresh_from_db()
        self.assertIsNone(empleado.sucursal_ref_id)
        self.assertEqual(empleado.sucursal, "")

    def test_patch_asignar_sucursal_invalida_devuelve_400(self):
        empleado = Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS", sucursal="")

        response = self.client.patch(
            f"/api/rrhh/empleados/{empleado.id}/asignar-sucursal/",
            data={"sucursal": "No existe"},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        empleado.refresh_from_db()
        self.assertEqual(empleado.sucursal, "")

    def test_patch_asignar_sucursal_rechaza_empleado_inactivo(self):
        Sucursal.objects.update_or_create(codigo="PAY-TEST", defaults={"nombre": "Payán Test", "activa": True})
        empleado = Empleado.objects.create(nombre="Empleado Baja", area="VENTAS", sucursal="", activo=False)

        response = self.client.patch(
            f"/api/rrhh/empleados/{empleado.id}/asignar-sucursal/",
            data={"sucursal": "Payán Test"},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 404)
        empleado.refresh_from_db()
        self.assertEqual(empleado.sucursal, "")

    def test_patch_asignar_area_produccion_y_regresar_a_pool(self):
        empleado = Empleado.objects.create(nombre="Empleado Produccion", area="PRODUCCION", sucursal="")

        response = self.client.patch(
            f"/api/rrhh/empleados/{empleado.id}/asignar-sucursal/",
            data={"area_detalle": "HORNOS"},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        empleado.refresh_from_db()
        self.assertEqual(empleado.area, "HORNOS")

        response = self.client.patch(
            f"/api/rrhh/empleados/{empleado.id}/asignar-sucursal/",
            data={"area_detalle": "PRODUCCION"},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        empleado.refresh_from_db()
        self.assertEqual(empleado.area, "PRODUCCION")

    def test_get_sin_asignar_se_basa_en_fk_no_en_texto(self):
        # Pendiente = sin FK canónico. Un empleado con texto pero SIN FK sigue pendiente
        # (el texto no es fuente de verdad); solo el que tiene sucursal_ref sale de la lista.
        sucursal, _ = Sucursal.objects.update_or_create(codigo="PAY-TEST", defaults={"nombre": "Sucursal Payan", "activa": True})
        Empleado.objects.create(nombre="Ventas sin sucursal", area="VENTAS", sucursal="")
        Empleado.objects.create(nombre="Produccion sin sucursal", area="PRODUCCION", sucursal="")
        Empleado.objects.create(nombre="Ventas solo texto sin fk", area="VENTAS", sucursal="Payán")
        Empleado.objects.create(nombre="Ventas asignado con fk", area="VENTAS", sucursal="Sucursal Payan", sucursal_ref=sucursal)
        Empleado.objects.create(nombre="Administracion sin sucursal", area="ADMINISTRACION", sucursal="")
        Empleado.objects.create(nombre="Ventas baja", area="VENTAS", sucursal="", activo=False)

        response = self.client.get("/api/rrhh/empleados/sin-asignar/")

        self.assertEqual(response.status_code, 200)
        names = {row["nombre"] for row in response.json()["results"]}
        self.assertEqual(names, {"Ventas sin sucursal", "Produccion sin sucursal", "Ventas solo texto sin fk"})

    def test_get_empleados_filtra_por_area(self):
        Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS")
        Empleado.objects.create(nombre="Empleado Produccion", area="PRODUCCION")

        response = self.client.get("/api/rrhh/empleados/?area=VENTAS")

        self.assertEqual(response.status_code, 200)
        names = {row["nombre"] for row in response.json()["results"]}
        self.assertIn("Empleado Ventas", names)
        self.assertNotIn("Empleado Produccion", names)

    def test_get_empleados_filtra_asignados_por_sucursal_id(self):
        sucursal, _ = Sucursal.objects.update_or_create(codigo="LEY-TEST", defaults={"nombre": "Sucursal Leyva", "activa": True})
        Empleado.objects.create(nombre="Asignado a Leyva", area="VENTAS", sucursal="Sucursal Leyva", sucursal_ref=sucursal)
        Empleado.objects.create(nombre="Con texto pero otra", area="VENTAS", sucursal="Sucursal Leyva")

        response = self.client.get(f"/api/rrhh/empleados/?area=VENTAS&sucursal_id={sucursal.id}")

        self.assertEqual(response.status_code, 200)
        names = {row["nombre"] for row in response.json()["results"]}
        self.assertEqual(names, {"Asignado a Leyva"})

    def test_ruta_html_asignacion_sucursal_responde_autenticado(self):
        response = self.client.get("/rrhh/asignacion-sucursal/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Asignación de personal")
        self.assertContains(response, "Buscar empleado")
        self.assertContains(response, "data-employee-search")

    def test_api_asignacion_sucursales_uniforma_nombres_operativos(self):
        Sucursal.objects.get_or_create(codigo="TUN", defaults={"nombre": "El Túnel", "activa": True})
        Sucursal.objects.get_or_create(codigo="GLO", defaults={"nombre": "Las Glorias", "activa": True})
        Sucursal.objects.get_or_create(codigo="COL", defaults={"nombre": "Sucursal Colosio", "activa": True})
        Sucursal.objects.get_or_create(codigo="CEDIS", defaults={"nombre": "CEDIS", "activa": True})

        response = self.client.get("/rrhh/api/asignacion-sucursales/")

        self.assertEqual(response.status_code, 200)
        rows = response.json()["results"]
        names = [item["nombre"] for item in rows]
        values = {item["valor"] for item in rows}
        self.assertIn("Sucursal El Túnel", names)
        self.assertIn("Sucursal Las Glorias", names)
        self.assertIn("Sucursal Colosio", names)
        self.assertIn("El Túnel", values)
        self.assertIn("Las Glorias", values)
        self.assertNotIn("CEDIS", names)
