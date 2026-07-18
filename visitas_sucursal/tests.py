from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse

from core.models import Sucursal, UserModuleAccess, UserProfile
from fallas.models import ReporteFalla
from logistica.models import PuntoLogistico
from rrhh.models import Empleado

from .models import ChecklistVisita, FotoVisita, HallazgoVisita, VisitaSucursal


class VisitasSucursalTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser("admin", "admin@example.com", "pass")
        self.sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        self.client.force_login(self.user)

    def test_nueva_visita_carga_checklist_base(self):
        response = self.client.post(
            reverse("visitas_sucursal:nueva"),
            {
                "sucursal": self.sucursal.id,
                "fecha_programada": "2026-06-24",
                "tipo": VisitaSucursal.TIPO_QUINCENAL,
            },
        )
        self.assertEqual(response.status_code, 302)
        visita = VisitaSucursal.objects.get()
        self.assertGreater(visita.checklist.count(), 10)

    def test_lista_muestra_cronograma_mensual(self):
        VisitaSucursal.objects.create(
            sucursal=self.sucursal,
            fecha_programada="2026-06-24",
            creado_por=self.user,
        )

        response = self.client.get(reverse("visitas_sucursal:lista"), {"anio": 2026, "mes": 6})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Cronograma")
        self.assertContains(response, "Junio 2026")
        self.assertContains(response, "Payán")
        self.assertContains(response, "Plan")

    def test_visitas_sucursal_excluye_cedis_del_cronograma_y_app(self):
        cedis, _created = Sucursal.objects.update_or_create(
            codigo="CEDIS",
            defaults={"nombre": "CEDIS", "activa": True},
        )
        VisitaSucursal.objects.create(
            sucursal=cedis,
            fecha_programada="2026-06-24",
            creado_por=self.user,
        )
        Empleado.objects.create(nombre="Encargado de control de producción", sucursal="CEDIS")

        response = self.client.get(reverse("visitas_sucursal:lista"), {"anio": 2026, "mes": 6})

        self.assertEqual(response.status_code, 200)
        self.assertNotIn("CEDIS", [row["sucursal"].codigo for row in response.context["rows"]])

        response = self.client.get(reverse("visitas_sucursal:app"))

        self.assertEqual(response.status_code, 200)
        self.assertNotIn("CEDIS", [visita.sucursal.codigo for visita in response.context["visitas"]])
        self.assertNotIn("CEDIS", [sucursal.codigo for sucursal in response.context["sucursales_preview"]])
        self.assertNotContains(response, "Encargado de control de producción")

    def test_no_permite_programar_visita_a_cedis(self):
        cedis, _created = Sucursal.objects.update_or_create(
            codigo="CEDIS",
            defaults={"nombre": "CEDIS", "activa": True},
        )

        response = self.client.post(
            reverse("visitas_sucursal:lista"),
            {
                "anio": 2026,
                "mes": 6,
                "sucursal": cedis.id,
                "fecha_programada": "2026-06-25",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertFalse(VisitaSucursal.objects.filter(sucursal=cedis).exists())

    def test_lista_programa_visita_desde_cronograma(self):
        response = self.client.post(
            reverse("visitas_sucursal:lista"),
            {
                "anio": 2026,
                "mes": 6,
                "sucursal": self.sucursal.id,
                "fecha_programada": "2026-06-25",
            },
        )

        self.assertEqual(response.status_code, 302)
        visita = VisitaSucursal.objects.get(fecha_programada="2026-06-25")
        self.assertEqual(visita.sucursal, self.sucursal)
        self.assertGreater(visita.checklist.count(), 10)

    def test_convertir_hallazgo_crea_reporte_falla_sin_duplicar(self):
        visita = VisitaSucursal.objects.create(sucursal=self.sucursal, creado_por=self.user)
        ChecklistVisita.objects.create(
            visita=visita,
            categoria="Funcionamiento de equipos",
            titulo="Terminal funciona",
            respuesta=ChecklistVisita.RESPUESTA_NO,
        )
        hallazgo = HallazgoVisita.objects.create(
            visita=visita,
            categoria="Funcionamiento de equipos",
            descripcion="Terminal no prende",
            accion_correctiva="Revisar conexión o reemplazar terminal",
            prioridad=HallazgoVisita.PRIORIDAD_ALTA,
            requiere_falla=True,
            creado_por=self.user,
        )

        url = reverse("visitas_sucursal:convertir_falla", args=[hallazgo.id])
        self.assertEqual(self.client.post(url).status_code, 302)
        self.assertEqual(ReporteFalla.objects.count(), 1)
        hallazgo.refresh_from_db()
        self.assertIsNotNone(hallazgo.reporte_falla_id)

        self.assertEqual(self.client.post(url).status_code, 302)
        self.assertEqual(ReporteFalla.objects.count(), 1)

    def test_app_sucursal_no_captura_checklist(self):
        user = get_user_model().objects.create_user("encargada", password="pass")
        UserProfile.objects.create(user=user, sucursal=self.sucursal)
        UserModuleAccess.objects.create(
            user=user,
            module="ventas.visitas_sucursal",
            access=UserModuleAccess.ACCESS_VIEW,
        )
        visita = VisitaSucursal.objects.create(sucursal=self.sucursal, creado_por=self.user)
        ChecklistVisita.objects.bulk_create(
            ChecklistVisita(visita=visita, categoria="Orden y limpieza", titulo=f"Punto {index}", orden=index)
            for index in range(1, 4)
        )
        self.client.force_login(user)

        response = self.client.post(
            reverse("visitas_sucursal:app"),
            {
                "visita_id": visita.id,
                **{f"respuesta_{item.id}": ChecklistVisita.RESPUESTA_SI for item in visita.checklist.all()},
            },
        )

        self.assertEqual(response.status_code, 302)
        item = visita.checklist.first()
        item.refresh_from_db()
        self.assertEqual(item.respuesta, ChecklistVisita.RESPUESTA_PENDIENTE)
        visita.refresh_from_db()
        self.assertEqual(visita.estatus, VisitaSucursal.ESTATUS_PROGRAMADA)
        self.assertIsNone(visita.fecha_real)
        self.assertIsNone(visita.realizada_por)
        self.assertIsNone(visita.realizada_en)

    def test_detalle_erp_no_marca_visita_real(self):
        visita = VisitaSucursal.objects.create(sucursal=self.sucursal, creado_por=self.user)
        ChecklistVisita.objects.create(visita=visita, categoria="Orden y limpieza", titulo="Pisos", orden=1)

        response = self.client.post(
            reverse("visitas_sucursal:detalle", args=[visita.id]),
            {
                "estatus": VisitaSucursal.ESTATUS_REALIZADA,
                "fecha_real": "2026-06-25",
                "observaciones": "Visita confirmada por supervisión",
            },
        )

        self.assertEqual(response.status_code, 302)
        visita.refresh_from_db()
        self.assertEqual(visita.estatus, VisitaSucursal.ESTATUS_PROGRAMADA)
        self.assertIsNone(visita.fecha_real)
        self.assertIsNone(visita.realizada_por)
        self.assertIsNone(visita.realizada_en)

    def test_app_auditor_marca_visita_real_con_usuario_y_hora(self):
        visita = VisitaSucursal.objects.create(sucursal=self.sucursal, creado_por=self.user)
        item = ChecklistVisita.objects.create(visita=visita, categoria="Orden y limpieza", titulo="Pisos", orden=1)
        empleado = Empleado.objects.create(nombre="Cajera presente", sucursal=self.sucursal.nombre)
        PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Payán",
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=80,
        )

        response = self.client.post(
            reverse("visitas_sucursal:app"),
            {
                "visita_id": visita.id,
                f"respuesta_{item.id}": ChecklistVisita.RESPUESTA_SI,
                "gps_latitud": "25.570000",
                "gps_longitud": "-108.470000",
                "gps_precision_m": "12.50",
                "personal_presente": [empleado.id],
                "fotos": SimpleUploadedFile("auditoria.jpg", b"fake-image", content_type="image/jpeg"),
            },
        )

        self.assertEqual(response.status_code, 302)
        visita.refresh_from_db()
        self.assertEqual(visita.estatus, VisitaSucursal.ESTATUS_REALIZADA)
        self.assertIsNotNone(visita.fecha_real)
        self.assertEqual(visita.realizada_por, self.user)
        self.assertIsNotNone(visita.realizada_en)
        self.assertEqual(str(visita.gps_latitud), "25.570000")
        self.assertEqual(visita.gps_dentro_geocerca, True)
        self.assertEqual(visita.gps_distancia_sucursal_m, 0)
        self.assertEqual(list(visita.personal_presente.all()), [empleado])
        self.assertEqual(FotoVisita.objects.filter(visita=visita).count(), 1)

    def test_app_auditor_bloquea_gps_fuera_de_geocerca_logistica(self):
        visita = VisitaSucursal.objects.create(sucursal=self.sucursal, creado_por=self.user)
        item = ChecklistVisita.objects.create(visita=visita, categoria="Orden y limpieza", titulo="Pisos", orden=1)
        PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Payán",
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=80,
        )

        response = self.client.post(
            reverse("visitas_sucursal:app"),
            {
                "visita_id": visita.id,
                f"respuesta_{item.id}": ChecklistVisita.RESPUESTA_SI,
                "gps_latitud": "25.600000",
                "gps_longitud": "-108.500000",
            },
        )

        self.assertEqual(response.status_code, 302)
        visita.refresh_from_db()
        self.assertEqual(visita.estatus, VisitaSucursal.ESTATUS_PROGRAMADA)
        self.assertIsNone(visita.realizada_en)

    def test_app_sucursal_no_edita_visitas_de_otra_sucursal(self):
        user = get_user_model().objects.create_user("encargada.colosio", password="pass")
        otra_sucursal = Sucursal.objects.create(codigo="CENT", nombre="Centro", activa=True)
        UserProfile.objects.create(user=user, sucursal=self.sucursal)
        UserModuleAccess.objects.create(
            user=user,
            module="ventas.visitas_sucursal",
            access=UserModuleAccess.ACCESS_VIEW,
        )
        visita_ajena = VisitaSucursal.objects.create(sucursal=otra_sucursal, creado_por=self.user)
        self.client.force_login(user)

        response = self.client.post(reverse("visitas_sucursal:app"), {"visita_id": visita_ajena.id})

        self.assertEqual(response.status_code, 302)
        self.assertFalse(visita_ajena.checklist.exists())

    def test_app_superusuario_puede_ver_como_sucursal_sin_capturar(self):
        visita = VisitaSucursal.objects.create(sucursal=self.sucursal, creado_por=self.user)
        item = ChecklistVisita.objects.create(visita=visita, categoria="Orden y limpieza", titulo="Pisos", orden=1)

        response = self.client.get(
            reverse("visitas_sucursal:app"),
            {"modo": "sucursal", "sucursal": self.sucursal.id},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Viendo la app como empleado de sucursal")
        self.assertContains(response, "Vista bloqueada")

        response = self.client.post(
            f"{reverse('visitas_sucursal:app')}?modo=sucursal&sucursal={self.sucursal.id}",
            {
                "visita_id": visita.id,
                f"respuesta_{item.id}": ChecklistVisita.RESPUESTA_SI,
            },
        )

        self.assertEqual(response.status_code, 302)
        item.refresh_from_db()
        self.assertEqual(item.respuesta, ChecklistVisita.RESPUESTA_PENDIENTE)

    def test_app_superusuario_sucursal_no_crea_bitacora(self):
        response = self.client.get(
            reverse("visitas_sucursal:app"),
            {"modo": "sucursal", "sucursal": self.sucursal.id},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.context["visita"])
        self.assertFalse(VisitaSucursal.objects.filter(sucursal=self.sucursal).exists())

    def test_app_superusuario_muestra_etiqueta_global_de_sucursal(self):
        visita = VisitaSucursal.objects.create(sucursal=self.sucursal, creado_por=self.user)

        response = self.client.get(
            reverse("visitas_sucursal:app"),
            {"modo": "sucursal", "sucursal": self.sucursal.id, "visita": visita.id},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Sucursal Payán")

    def test_detalle_reprogramar_visita(self):
        visita = VisitaSucursal.objects.create(
            sucursal=self.sucursal, fecha_programada="2026-06-24", creado_por=self.user
        )

        response = self.client.post(
            reverse("visitas_sucursal:detalle", args=[visita.id]),
            {"action": "reprogramar", "nueva_fecha": "2026-06-26"},
        )

        self.assertEqual(response.status_code, 302)
        visita.refresh_from_db()
        self.assertEqual(str(visita.fecha_programada), "2026-06-26")
        self.assertEqual(visita.estatus, VisitaSucursal.ESTATUS_PROGRAMADA)

    def test_detalle_reprogramar_rechaza_dia_ocupado(self):
        visita = VisitaSucursal.objects.create(
            sucursal=self.sucursal, fecha_programada="2026-06-24", creado_por=self.user
        )
        VisitaSucursal.objects.create(
            sucursal=self.sucursal, fecha_programada="2026-06-26", creado_por=self.user
        )

        self.client.post(
            reverse("visitas_sucursal:detalle", args=[visita.id]),
            {"action": "reprogramar", "nueva_fecha": "2026-06-26"},
        )

        visita.refresh_from_db()
        self.assertEqual(str(visita.fecha_programada), "2026-06-24")

    def test_detalle_cancelar_visita(self):
        visita = VisitaSucursal.objects.create(
            sucursal=self.sucursal, fecha_programada="2026-06-24", creado_por=self.user
        )

        self.client.post(
            reverse("visitas_sucursal:detalle", args=[visita.id]),
            {"action": "cancelar"},
        )

        visita.refresh_from_db()
        self.assertEqual(visita.estatus, VisitaSucursal.ESTATUS_CANCELADA)

    def test_detalle_eliminar_visita_programada(self):
        visita = VisitaSucursal.objects.create(
            sucursal=self.sucursal, fecha_programada="2026-06-24", creado_por=self.user
        )

        response = self.client.post(
            reverse("visitas_sucursal:detalle", args=[visita.id]),
            {"action": "eliminar"},
        )

        self.assertEqual(response.status_code, 302)
        self.assertIn("anio=2026", response.url)
        self.assertFalse(VisitaSucursal.objects.filter(pk=visita.pk).exists())

    def test_detalle_no_elimina_visita_realizada(self):
        visita = VisitaSucursal.objects.create(
            sucursal=self.sucursal,
            fecha_programada="2026-06-24",
            estatus=VisitaSucursal.ESTATUS_REALIZADA,
            creado_por=self.user,
        )

        self.client.post(
            reverse("visitas_sucursal:detalle", args=[visita.id]),
            {"action": "eliminar"},
        )

        self.assertTrue(VisitaSucursal.objects.filter(pk=visita.pk).exists())

    def test_app_auditor_filtra_por_sucursal(self):
        otra = Sucursal.objects.create(codigo="OTR", nombre="Otra", activa=True)
        VisitaSucursal.objects.create(sucursal=self.sucursal, creado_por=self.user)
        visita_otra = VisitaSucursal.objects.create(sucursal=otra, creado_por=self.user)

        response = self.client.get(
            reverse("visitas_sucursal:app"),
            {"modo": "auditor", "sucursal": otra.id},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["visita"].id, visita_otra.id)
        self.assertEqual(
            [item.sucursal_id for item in response.context["visitas"]],
            [otra.id],
        )

    def test_lista_agenda_por_dia_es_default(self):
        VisitaSucursal.objects.create(
            sucursal=self.sucursal, fecha_programada="2026-06-24", creado_por=self.user
        )

        response = self.client.get(reverse("visitas_sucursal:lista"), {"anio": 2026, "mes": 6})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["vista_movil"], "dia")
        agenda = response.context["agenda_dias"]
        self.assertEqual(len(agenda), 1)
        self.assertEqual(str(agenda[0]["fecha"]), "2026-06-24")
        self.assertEqual(len(agenda[0]["visitas"]), 1)
        self.assertContains(response, "Por sucursal")

    def test_lista_vista_sucursal_por_parametro(self):
        response = self.client.get(
            reverse("visitas_sucursal:lista"),
            {"anio": 2026, "mes": 6, "vista": "sucursal"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["vista_movil"], "sucursal")

    def test_lista_vista_invalida_regresa_a_dia(self):
        response = self.client.get(
            reverse("visitas_sucursal:lista"),
            {"anio": 2026, "mes": 6, "vista": "xxx"},
        )

        self.assertEqual(response.context["vista_movil"], "dia")

    def test_nueva_precarga_sucursal_y_fecha_desde_get(self):
        response = self.client.get(
            reverse("visitas_sucursal:nueva"),
            {"sucursal": self.sucursal.id, "fecha": "2026-06-28"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, f'value="{self.sucursal.id}" selected')
        self.assertContains(response, 'value="2026-06-28"')
