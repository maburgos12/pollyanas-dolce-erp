import json
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import Client, TestCase, override_settings

from core.access import ROLE_PRODUCCION, ROLE_RRHH
from core.navigation import NAV_GROUPS
from rrhh.models import Empleado, HoraExtra, NominaLinea, NominaPeriodo, PermisoSalida, PermisoSalidaCambio

from .models import (
    AREA_EMBETUNADO,
    AREA_HORNOS,
    AREA_LOGISTICA,
    AREA_PRODUCCION,
    AREAS_PRODUCCION,
    BonoProduccionEmpleado,
    ConfigBonoPeriodo,
    RegistroDiarioProduccion,
    area_bono_produccion_empleado,
    normalizar_area_produccion,
)
from .views import _recalcular_desde_registros


@override_settings(SECURE_SSL_REDIRECT=False)
class BonosProduccionTests(TestCase):
    def crear_contexto_autosolicitud(self):
        grupo, _ = Group.objects.get_or_create(name=ROLE_PRODUCCION)
        user = get_user_model().objects.create_user(username="julissa.angulo", password="test12345")
        user.groups.add(grupo)
        carolina_user = get_user_model().objects.create_user(
            username="carolina.cayetano",
            password="test12345",
        )
        carolina = Empleado.objects.create(
            nombre="CAYETANO VALENZUELA CAROLINA",
            activo=True,
            area="PRODUCCION",
            departamento="PRODUCCION",
            puesto="Jefa de Producción",
            nivel_organizacional=Empleado.NIVEL_JEFATURA,
            usuario_erp=carolina_user,
        )
        julissa = Empleado.objects.create(
            nombre="ANGULO PARRA JULISSA",
            activo=True,
            area="PRODUCCION",
            departamento="PRODUCCION",
            departamento_origen="PRODUCCION",
            puesto="Encargada de Producción",
            nivel_organizacional=Empleado.NIVEL_SUPERVISION,
            participa_bonos_produccion=False,
            usuario_erp=user,
            jefe_directo=carolina,
        )
        ajeno = Empleado.objects.create(
            nombre="EMPLEADO FUERA DE ALCANCE",
            activo=True,
            area="ADMINISTRACION",
            departamento="ADMINISTRACION",
        )
        return user, carolina_user, julissa, ajeno

    def test_autosolicitud_incluye_usuario_actual_sin_participar_en_bonos(self):
        user, _, julissa, _ = self.crear_contexto_autosolicitud()
        self.client.force_login(user)

        permisos = self.client.get("/api/bonos-produccion/permisos/?area=PRODUCCION")
        horas = self.client.get("/api/bonos-produccion/horas-extra/?area=PRODUCCION")

        self.assertEqual(permisos.status_code, 200)
        self.assertEqual(horas.status_code, 200)
        for response in (permisos, horas):
            propios = [row for row in response.json()["empleados"] if row["id"] == julissa.id]
            self.assertEqual(len(propios), 1)
            self.assertTrue(propios[0]["es_usuario_actual"])
            self.assertTrue(propios[0]["puede_solicitar"])
            self.assertFalse(propios[0]["puede_gestionar"])
            self.assertEqual(response.json()["empleados"][0]["id"], julissa.id)

    @patch("rrhh.bonos_permisos.notificar_permiso_solicitado")
    def test_usuario_crea_permiso_propio_y_notifica_a_su_jefa(self, notificar_permiso):
        user, _, julissa, _ = self.crear_contexto_autosolicitud()
        self.client.force_login(user)

        response = self.client.post(
            "/api/bonos-produccion/permisos/",
            json.dumps(
                {
                    "empleado": julissa.id,
                    "area": "PRODUCCION",
                    "tipo": PermisoSalida.TIPO_PERMISO_HORA,
                    "fecha_inicio": "2026-07-14T12:00:00",
                    "fecha_fin": "2026-07-14T13:00:00",
                    "goce_sueldo": True,
                    "motivo": "Cita médica",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        permiso = PermisoSalida.objects.get(pk=response.json()["id"])
        self.assertEqual(permiso.empleado, julissa)
        self.assertEqual(permiso.estado_jefe, PermisoSalida.ESTADO_JEFE_PENDIENTE)
        self.assertFalse(permiso.requiere_direccion)
        notificar_permiso.assert_called_once_with(permiso, actor=user)

    @patch("bonos_produccion.views.notificar_hora_extra_solicitada", create=True)
    def test_usuario_crea_hora_extra_propia_asignada_y_notificada_a_su_jefa(self, notificar_hora):
        user, carolina_user, julissa, _ = self.crear_contexto_autosolicitud()
        self.client.force_login(user)

        response = self.client.post(
            "/api/bonos-produccion/horas-extra/",
            json.dumps(
                {
                    "empleado": julissa.id,
                    "area": "PRODUCCION",
                    "fecha": "2026-07-14",
                    "horas": "1.50",
                    "notas": "Pedido especial",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        hora_extra = HoraExtra.objects.get(pk=response.json()["id"])
        self.assertEqual(hora_extra.empleado, julissa)
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_PENDIENTE)
        self.assertEqual(hora_extra.jefe_directo, carolina_user)
        notificar_hora.assert_called_once_with(hora_extra, actor=user)

    def test_usuario_no_gestiona_ni_autoriza_su_hora_extra(self):
        user, carolina_user, julissa, _ = self.crear_contexto_autosolicitud()
        hora_extra = HoraExtra.objects.create(
            empleado=julissa,
            fecha="2026-07-14",
            horas="1.50",
            notas="Pedido especial",
            jefe_directo=carolina_user,
        )
        self.client.force_login(user)

        listado = self.client.get("/api/bonos-produccion/horas-extra/?area=PRODUCCION")
        propios = [row for row in listado.json()["horas_extra"] if row["id"] == hora_extra.id]
        self.assertEqual(len(propios), 1)
        payload = propios[0]
        self.assertFalse(payload["puede_editar"])
        self.assertFalse(payload["puede_eliminar"])
        self.assertFalse(payload["puede_autorizar"])

        editar = self.client.post(
            f"/api/bonos-produccion/horas-extra/{hora_extra.id}/editar/",
            json.dumps(
                {
                    "fecha": "2026-07-15",
                    "horas": "2.00",
                    "notas": "Intento propio",
                    "motivo_cambio": "Intento propio",
                }
            ),
            content_type="application/json",
        )
        eliminar = self.client.post(
            f"/api/bonos-produccion/horas-extra/{hora_extra.id}/eliminar/",
            json.dumps({"motivo_cambio": "Intento propio"}),
            content_type="application/json",
        )
        autorizar = self.client.post(f"/api/bonos-produccion/horas-extra/{hora_extra.id}/autorizar/")

        self.assertEqual(editar.status_code, 403)
        self.assertEqual(eliminar.status_code, 403)
        self.assertEqual(autorizar.status_code, 403)
        hora_extra.refresh_from_db()
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_PENDIENTE)

    def test_autosolicitud_rechaza_empleado_fuera_del_selector(self):
        user, _, _, ajeno = self.crear_contexto_autosolicitud()
        self.client.force_login(user)

        response = self.client.post(
            "/api/bonos-produccion/horas-extra/",
            json.dumps(
                {
                    "empleado": ajeno.id,
                    "area": "PRODUCCION",
                    "fecha": "2026-07-14",
                    "horas": "1.00",
                    "notas": "Manipulación",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(HoraExtra.objects.filter(empleado=ajeno).exists())

    def test_autosolicitud_no_se_habilita_sin_jefa_con_usuario_erp(self):
        user, _, julissa, _ = self.crear_contexto_autosolicitud()
        julissa.jefe_directo.usuario_erp = None
        julissa.jefe_directo.save(update_fields=["usuario_erp"])
        self.client.force_login(user)

        response = self.client.get("/api/bonos-produccion/permisos/?area=PRODUCCION")

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(julissa.id, [row["id"] for row in response.json()["empleados"]])

    def test_app_etiqueta_al_usuario_actual_como_yo(self):
        user, _, _, _ = self.crear_contexto_autosolicitud()
        self.client.force_login(user)

        response = self.client.get("/bonos-produccion/app/?captura=1&tab=permisos")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Yo — ${b.empleado_nombre}", response.content.decode())

    def test_sidebar_de_produccion_apunta_al_dashboard_erp(self):
        produccion = next(group for group in NAV_GROUPS if group["key"] == "produccion")
        bonos_item = next(item for item in produccion["items"] if item[0] == "produccion" and item[1] == "bonos")

        self.assertEqual(bonos_item[3], "/bonos-produccion/dashboard/")

    def test_dashboard_erp_muestra_configuracion_y_app_de_captura(self):
        user = get_user_model().objects.create_superuser(username="admin-bonos", password="x")
        self.client.force_login(user)
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        empleado = Empleado.objects.create(
            nombre="Empleado Dashboard",
            area="PRODUCCION",
            participa_bonos_produccion=True,
        )
        BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_PRODUCCION)

        response = self.client.get("/bonos-produccion/dashboard/?mes=5&anio=2026")

        self.assertEqual(response.status_code, 200)
        content = response.content.decode()
        self.assertIn("Control de configuración y ajustes", content)
        self.assertIn("Abrir app de captura", content)
        self.assertIn("Permisos de equipo", content)
        self.assertIn("/bonos-produccion/app/?captura=1&tab=permisos", content)
        self.assertIn("Monto logística", content)
        self.assertIn("Buscar empleado por nombre", content)
        self.assertIn("Usa concepto producción", content)
        self.assertIn("Empleado Dashboard", content)
        self.assertEqual(response["Cache-Control"], "max-age=0, no-cache, no-store, must-revalidate, private")

    def test_embetunado_es_etiqueta_del_bucket_historico_produccion(self):
        empleado = Empleado.objects.create(nombre="Empleado Embetunado", puesto_operativo="EMBETUNADO")

        self.assertNotEqual(AREA_EMBETUNADO, AREA_PRODUCCION)
        self.assertEqual(dict(AREAS_PRODUCCION)[AREA_PRODUCCION], "Embetunado")
        self.assertEqual(normalizar_area_produccion("EMBETUNADO"), AREA_PRODUCCION)
        self.assertEqual(area_bono_produccion_empleado(empleado), AREA_PRODUCCION)

    def test_raiz_web_de_bonos_produccion_redirige_al_dashboard(self):
        response = self.client.get("/bonos-produccion/")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/bonos-produccion/dashboard/")

    def test_dashboard_erp_actualiza_configuracion_del_periodo(self):
        user = get_user_model().objects.create_superuser(username="admin-config-bonos", password="x")
        self.client.force_login(user)
        ConfigBonoPeriodo.objects.create(mes=5, anio=2026)

        response = self.client.post(
            "/bonos-produccion/dashboard/",
            {
                "action": "config",
                "mes": "5",
                "anio": "2026",
                "dias_laborables": "24",
                "monto_hornos": "1100.00",
                "monto_area_produccion": "900.00",
                "monto_armado": "875.00",
                "monto_logistica": "800.00",
                "monto_crucero": "950.00",
                "premio_embetunado": "400.00",
                "regla_hornos_usa_produccion": "on",
                "regla_hornos_pct_produccion": "60.00",
                "regla_hornos_pct_asistencia": "20.00",
                "regla_hornos_pct_puntualidad": "15.00",
                "regla_hornos_pct_uniforme": "5.00",
                "regla_hornos_limite_produccion": "2",
                "regla_hornos_limite_asistencia": "2",
                "regla_hornos_limite_puntualidad": "2",
                "regla_hornos_limite_uniforme": "1",
                "regla_produccion_usa_produccion": "on",
                "regla_produccion_pct_produccion": "65.00",
                "regla_produccion_pct_asistencia": "15.00",
                "regla_produccion_pct_puntualidad": "15.00",
                "regla_produccion_pct_uniforme": "5.00",
                "regla_produccion_limite_produccion": "2",
                "regla_produccion_limite_asistencia": "2",
                "regla_produccion_limite_puntualidad": "2",
                "regla_produccion_limite_uniforme": "1",
                "regla_armado_usa_produccion": "on",
                "regla_armado_pct_produccion": "65.00",
                "regla_armado_pct_asistencia": "15.00",
                "regla_armado_pct_puntualidad": "15.00",
                "regla_armado_pct_uniforme": "5.00",
                "regla_armado_limite_produccion": "2",
                "regla_armado_limite_asistencia": "2",
                "regla_armado_limite_puntualidad": "2",
                "regla_armado_limite_uniforme": "1",
                "regla_logistica_pct_produccion": "0.00",
                "regla_logistica_pct_asistencia": "50.00",
                "regla_logistica_pct_puntualidad": "30.00",
                "regla_logistica_pct_uniforme": "20.00",
                "regla_logistica_limite_produccion": "0",
                "regla_logistica_limite_asistencia": "3",
                "regla_logistica_limite_puntualidad": "2",
                "regla_logistica_limite_uniforme": "1",
                "regla_crucero_usa_produccion": "on",
                "regla_crucero_pct_produccion": "65.00",
                "regla_crucero_pct_asistencia": "15.00",
                "regla_crucero_pct_puntualidad": "15.00",
                "regla_crucero_pct_uniforme": "5.00",
                "regla_crucero_limite_produccion": "2",
                "regla_crucero_limite_asistencia": "2",
                "regla_crucero_limite_puntualidad": "2",
                "regla_crucero_limite_uniforme": "1",
            },
        )

        self.assertEqual(response.status_code, 302)
        periodo = ConfigBonoPeriodo.objects.get(mes=5, anio=2026)
        self.assertEqual(periodo.dias_laborables, 24)
        self.assertEqual(periodo.monto_logistica, Decimal("800.00"))
        regla_hornos = periodo.reglas_area.get(area=AREA_HORNOS)
        regla_logistica = periodo.reglas_area.get(area=AREA_LOGISTICA)
        self.assertEqual(regla_hornos.pct_produccion, Decimal("60.00"))
        self.assertFalse(regla_logistica.usa_produccion)
        self.assertEqual(regla_logistica.pct_asistencia, Decimal("50.00"))

    def test_dashboard_erp_actualiza_ajuste_de_empleado(self):
        user = get_user_model().objects.create_superuser(username="admin-ajuste-bonos", password="x")
        self.client.force_login(user)
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        empleado = Empleado.objects.create(nombre="Empleado Ajuste", area="PRODUCCION")
        bono = BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_PRODUCCION)

        response = self.client.post(
            "/bonos-produccion/dashboard/",
            {
                "action": "ajuste_bono",
                "mes": "5",
                "anio": "2026",
                "bono_id": str(bono.id),
                "dias_trabajados": "20",
                "dias_uniforme": "20",
                "dias_asistencia": "20",
                "dias_puntualidad": "20",
                "dias_produccion": "20",
                "total_embetunados": "14",
                "ajuste_positivo": "50.00",
                "ajuste_negativo": "10.00",
                "bono_extra": "25.00",
                "observaciones": "Ajuste operativo",
            },
        )

        self.assertEqual(response.status_code, 302)
        bono.refresh_from_db()
        self.assertEqual(bono.dias_trabajados, 20)
        self.assertEqual(bono.total_embetunados, 14)
        self.assertEqual(bono.ajuste_positivo, Decimal("50.00"))
        self.assertEqual(bono.observaciones, "Ajuste operativo")

    def test_reglas_por_area_permiten_logistica_sin_concepto_produccion(self):
        periodo = ConfigBonoPeriodo.objects.create(
            mes=5,
            anio=2026,
            dias_laborables=20,
            monto_logistica=Decimal("1000.00"),
        )
        periodo.asegurar_reglas_area()
        regla = periodo.reglas_area.get(area=AREA_LOGISTICA)
        self.assertFalse(regla.usa_produccion)

        empleado = Empleado.objects.create(nombre="Empleado Logistica", area="LOGISTICA")
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area=AREA_LOGISTICA,
            dias_trabajados=20,
            dias_uniforme=20,
            dias_asistencia=20,
            dias_puntualidad=20,
            dias_produccion=0,
        )

        bono.recalcular()

        self.assertTrue(bono.pasa_produccion)
        self.assertEqual(bono.monto_produccion, Decimal("0.00"))
        self.assertEqual(bono.total_a_pagar, Decimal("1000.00"))

    def test_premio_embetunado_se_asigna_en_area_produccion(self):
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026, premio_embetunado=Decimal("400.00"))
        empleado_1 = Empleado.objects.create(
            nombre="Empleado Produccion 1",
            area="PRODUCCION",
            participa_bonos_produccion=True,
        )
        empleado_2 = Empleado.objects.create(
            nombre="Empleado Produccion 2",
            area="PRODUCCION",
            participa_bonos_produccion=True,
        )
        bono_1 = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado_1,
            area=AREA_PRODUCCION,
            dias_trabajados=20,
            dias_uniforme=20,
            dias_asistencia=20,
            dias_puntualidad=20,
            dias_produccion=20,
            total_embetunados=12,
        )
        bono_2 = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado_2,
            area=AREA_PRODUCCION,
            dias_trabajados=20,
            dias_uniforme=20,
            dias_asistencia=20,
            dias_puntualidad=20,
            dias_produccion=20,
            total_embetunados=18,
        )

        periodo.recalcular_todos()

        bono_1.refresh_from_db()
        bono_2.refresh_from_db()
        self.assertFalse(bono_1.gano_premio_embetunado)
        self.assertTrue(bono_2.gano_premio_embetunado)
        self.assertEqual(bono_2.monto_premio_embetunado, Decimal("400.00"))

    def test_pwa_usa_sesion_django_y_expone_csrf(self):
        user = get_user_model().objects.create_superuser(username="pwa-produccion")
        self.client.force_login(user)

        response = self.client.get("/bonos-produccion/app/?captura=1")

        self.assertEqual(response.status_code, 200)
        self.assertIn("csrftoken", response.cookies)
        content = response.content.decode()
        self.assertIn("credentials:'same-origin'", content)
        self.assertIn("/bonos-produccion/manifest.json", content)
        self.assertIn("/static/bonos_produccion/icons/apple-touch-icon-180.png?v=20260521", content)
        self.assertIn("/bonos-produccion/sw.js", content)
        self.assertIn("href:'/logout/'", content)
        self.assertIn("Cerrar sesión", content)
        self.assertIn("employee-search", content)
        self.assertIn("Teclea nombre o apellido", content)
        self.assertIn("Vista previa tamaño carta", content)
        self.assertIn("Imprimir / PDF", content)
        self.assertIn("label:'Area',value:contextLabel", content)
        self.assertNotIn("Monto calculado", content)
        self.assertIn("grid-template-columns:repeat(2,minmax(0,1fr))", content)
        self.assertIn("margin:28px auto 0", content)
        self.assertIn("Permiso ${lastPermiso.folio} registrado", content)
        self.assertIn("Imprimir / guardar PDF", content)
        self.assertIn("ReactDOM.createPortal", content)
        self.assertIn("body > :not(.print-modal)", content)
        self.assertIn("transform:none!important", content)
        self.assertIn("Firma empleado", content)
        self.assertIn("const FORCE_CAPTURE=", content)
        self.assertIn("const initialTab=", content)
        self.assertIn("React.useState(initialTab)", content)
        self.assertIn("React.useState(FORCE_CAPTURE?AREA_TODAS:'HORNOS')", content)
        self.assertIn("includeAll:true", content)
        self.assertIn("r.redirected", content)
        self.assertNotIn("pointer-events:none", content)
        self.assertNotIn("if(!dom)", content)
        self.assertIn(".day-cell.dom.worked", content)
        self.assertIn(".day-cell.saving", content)
        self.assertIn("savingDia", content)
        self.assertIn("onClick:()=>togDia(d)", content)
        self.assertNotIn("togDia(d);setSelDia(d);", content)
        self.assertNotIn("pd_logistica_access", content)
        self.assertIn("no-store", response["Cache-Control"])

    def test_app_de_produccion_redirige_a_dashboard_en_escritorio(self):
        user = get_user_model().objects.create_superuser(username="desktop-produccion")
        self.client.force_login(user)

        response = self.client.get(
            "/bonos-produccion/app/",
            HTTP_USER_AGENT="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/bonos-produccion/dashboard/")

    def test_app_de_produccion_conserva_pwa_en_movil_o_captura_forzada(self):
        user = get_user_model().objects.create_superuser(username="mobile-produccion")
        self.client.force_login(user)

        mobile = self.client.get(
            "/bonos-produccion/app/",
            HTTP_USER_AGENT="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Mobile/15E148",
        )
        forced = self.client.get(
            "/bonos-produccion/app/?captura=1",
            HTTP_USER_AGENT="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        )

        self.assertEqual(mobile.status_code, 200)
        self.assertEqual(forced.status_code, 200)
        self.assertIn("Bonos de Produccion", mobile.content.decode())
        self.assertIn("Bonos de Produccion", forced.content.decode())

    def test_manifest_y_service_worker_de_produccion_sirven_con_content_type_correcto(self):
        manifest = self.client.get("/bonos-produccion/manifest.json")
        sw = self.client.get("/bonos-produccion/sw.js")

        self.assertEqual(manifest.status_code, 200)
        self.assertEqual(manifest["Content-Type"], "application/manifest+json")
        self.assertEqual(manifest.json()["start_url"], "/bonos-produccion/app/?captura=1")
        self.assertIn(
            "/static/bonos_produccion/icons/apple-touch-icon-180.png",
            [icon["src"] for icon in manifest.json()["icons"]],
        )
        self.assertEqual(sw.status_code, 200)
        self.assertIn("application/javascript", sw["Content-Type"])
        sw_content = sw.content.decode()
        self.assertIn("pollyanas-bonos-produccion-pwa-v20-autosolicitudes", sw_content)
        self.assertIn('cache: "no-store"', sw_content)
        self.assertIn('url.pathname.startsWith("/bonos-produccion/dashboard/")', sw_content)

    def test_api_produccion_acepta_post_con_sesion_y_csrf(self):
        client = Client(enforce_csrf_checks=True)
        user = get_user_model().objects.create_superuser(username="csrf-produccion")
        client.force_login(user)
        client.get("/bonos-produccion/app/?captura=1")
        csrf_token = client.cookies["csrftoken"].value

        response = client.post(
            "/api/bonos-produccion/periodos/",
            {"mes": 1, "anio": 2098, "dias_laborables": 23},
            content_type="application/json",
            HTTP_X_CSRFTOKEN=csrf_token,
        )

        self.assertEqual(response.status_code, 201)

    def test_recalcular_usa_dias_laborables_como_base_de_asistencia(self):
        empleado = Empleado.objects.create(nombre="Empleado Produccion", area="PRODUCCION")
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026, dias_laborables=22, monto_hornos=Decimal("800.00"))
        periodo.asegurar_reglas_area()
        regla = periodo.reglas_area.get(area=AREA_HORNOS)
        regla.limite_asistencia = 0
        regla.cancela_por_asistencia = True
        regla.limite_asistencia_cancelacion = 1
        regla.save()
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area=AREA_HORNOS,
            dias_trabajados=21,
            dias_uniforme=21,
            dias_asistencia=21,
            dias_puntualidad=21,
            dias_produccion=21,
        )

        bono.recalcular()

        self.assertFalse(bono.pasa_asistencia)
        self.assertEqual(bono.monto_asistencia, Decimal("0.00"))
        self.assertEqual(bono.monto_uniforme, Decimal("0.00"))
        self.assertEqual(bono.monto_puntualidad, Decimal("0.00"))
        self.assertEqual(bono.monto_produccion, Decimal("0.00"))
        self.assertEqual(bono.total_a_pagar, Decimal("0.00"))

    def test_tres_retardos_cancelan_bono_produccion_completo(self):
        empleado = Empleado.objects.create(nombre="Empleado Produccion", area="PRODUCCION")
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026, dias_laborables=22, monto_hornos=Decimal("800.00"))
        periodo.asegurar_reglas_area()
        regla = periodo.reglas_area.get(area=AREA_HORNOS)
        regla.limite_puntualidad = 2
        regla.cancela_por_puntualidad = True
        regla.limite_retardos_cancelacion = 3
        regla.save()
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area=AREA_HORNOS,
            dias_trabajados=22,
            dias_uniforme=22,
            dias_asistencia=22,
            dias_puntualidad=19,
            dias_produccion=22,
            gano_premio_embetunado=True,
        )

        bono.recalcular()

        self.assertFalse(bono.pasa_puntualidad)
        self.assertEqual(bono.monto_uniforme, Decimal("0.00"))
        self.assertEqual(bono.monto_asistencia, Decimal("0.00"))
        self.assertEqual(bono.monto_puntualidad, Decimal("0.00"))
        self.assertEqual(bono.monto_produccion, Decimal("0.00"))
        self.assertEqual(bono.monto_premio_embetunado, Decimal("0.00"))
        self.assertEqual(bono.total_a_pagar, Decimal("0.00"))

    def test_regla_cancelacion_personalizada_cancela_bono_con_una_falta(self):
        empleado = Empleado.objects.create(
            nombre="Empleado Produccion",
            area="PRODUCCION",
            participa_bonos_produccion=True,
        )
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026, dias_laborables=23, monto_hornos=Decimal("800.00"))
        periodo.asegurar_reglas_area()
        regla = periodo.reglas_area.get(area=AREA_HORNOS)
        regla.limite_asistencia = 2
        regla.limite_puntualidad = 2
        regla.cancela_por_asistencia = True
        regla.limite_asistencia_cancelacion = 0
        regla.save()
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area=AREA_HORNOS,
            dias_trabajados=22,
            dias_uniforme=23,
            dias_asistencia=22,
            dias_puntualidad=23,
            dias_produccion=23,
        )

        bono.recalcular()

        self.assertTrue(bono.pasa_asistencia)
        self.assertTrue(bono.pasa_puntualidad)
        self.assertEqual(bono.total_a_pagar, Decimal("0.00"))

    def test_recalcular_no_deja_total_negativo_por_ajuste(self):
        empleado = Empleado.objects.create(nombre="Empleado Produccion", area="PRODUCCION")
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026, dias_laborables=10, monto_hornos=Decimal("800.00"))
        periodo.asegurar_reglas_area()
        regla = periodo.reglas_area.get(area=AREA_HORNOS)
        regla.limite_puntualidad = 3
        regla.save()
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area=AREA_HORNOS,
            dias_trabajados=10,
            dias_uniforme=9,
            dias_asistencia=9,
            dias_puntualidad=7,
            dias_produccion=9,
            ajuste_negativo=Decimal("1000.00"),
        )

        bono.recalcular()

        self.assertEqual(bono.monto_uniforme, Decimal("40.00"))
        self.assertEqual(bono.monto_asistencia, Decimal("120.00"))
        self.assertEqual(bono.monto_puntualidad, Decimal("120.00"))
        self.assertEqual(bono.monto_produccion, Decimal("520.00"))
        self.assertEqual(bono.total_a_pagar, Decimal("0.00"))

    def test_recalcular_desde_registros_cuenta_solo_asistencias_reales(self):
        user = get_user_model().objects.create_superuser(username="captura-produccion")
        self.client.force_login(user)
        empleado = Empleado.objects.create(nombre="Empleado Produccion", area="PRODUCCION")
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026, dias_laborables=23)
        bono = BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_HORNOS)
        for dia in range(1, 24):
            RegistroDiarioProduccion.objects.create(
                bono=bono,
                dia=dia,
                tiene_asistencia=True,
                tiene_uniforme=True,
                tiene_puntualidad=True,
                tiene_produccion=True,
            )
        for dia in range(24, 32):
            RegistroDiarioProduccion.objects.create(
                bono=bono,
                dia=dia,
                tiene_asistencia=False,
                tiene_uniforme=True,
                tiene_puntualidad=True,
                tiene_produccion=True,
            )

        _recalcular_desde_registros(bono)

        bono.refresh_from_db()
        self.assertEqual(bono.dias_trabajados, 23)
        self.assertEqual(bono.dias_uniforme, 23)
        self.assertEqual(bono.dias_puntualidad, 23)
        self.assertEqual(bono.dias_produccion, 23)
        self.assertTrue(bono.pasa_asistencia)
        self.assertEqual(bono.total_a_pagar, Decimal("1000.00"))

    def test_recalcular_desde_registros_permite_dias_extra_trabajados(self):
        user = get_user_model().objects.create_superuser(username="captura-produccion-extra")
        self.client.force_login(user)
        empleado = Empleado.objects.create(nombre="Empleado Produccion Extra", area="PRODUCCION")
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026, dias_laborables=23)
        bono = BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_HORNOS)
        for dia in range(1, 25):
            RegistroDiarioProduccion.objects.create(bono=bono, dia=dia, tiene_asistencia=True)

        _recalcular_desde_registros(bono)

        bono.refresh_from_db()
        self.assertEqual(bono.dias_trabajados, 24)

    def test_aplicar_a_nomina_escribe_total_en_linea_bonos(self):
        empleado = Empleado.objects.create(
            nombre="Empleado Produccion",
            area="PRODUCCION",
            participa_bonos_produccion=True,
        )
        periodo_bono = ConfigBonoPeriodo.objects.create(mes=5, anio=2026, dias_laborables=15)
        nomina = NominaPeriodo.objects.create(fecha_inicio=date(2026, 5, 1), fecha_fin=date(2026, 5, 31))
        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo_bono,
            empleado=empleado,
            area="HORNOS",
            dias_trabajados=15,
            dias_uniforme=15,
            dias_asistencia=15,
            dias_puntualidad=15,
            dias_produccion=15,
        )
        bono.recalcular()
        bono.save()

        updated = periodo_bono.aplicar_a_nomina(nomina)

        self.assertEqual(updated, 1)
        linea = NominaLinea.objects.get(periodo=nomina, empleado=empleado)
        self.assertEqual(linea.dias_trabajados, Decimal("15"))
        self.assertEqual(linea.bonos, Decimal("1000.00"))

    def test_inicializar_bonos_respeta_elegibilidad_rrhh_no_solo_area(self):
        user = get_user_model().objects.create_user(username="bonos")
        user.groups.add(Group.objects.get_or_create(name=ROLE_PRODUCCION)[0])
        user.groups.add(Group.objects.get_or_create(name=ROLE_RRHH)[0])
        self.client.force_login(user)
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        elegible = Empleado.objects.create(
            nombre="Empleado Produccion Elegible",
            area="PRODUCCION",
            sucursal="",
            participa_bonos_produccion=True,
        )
        no_elegible = Empleado.objects.create(
            nombre="ANGULO PARRA JULISSA",
            area="PRODUCCION",
            departamento="PRODUCCION",
            puesto="Encargada de Producción",
            puesto_operativo="ENCARGADA_PRODUCCION",
            participa_bonos_produccion=False,
        )

        response = self.client.post(f"/api/bonos-produccion/periodos/{periodo.id}/inicializar-bonos/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["creados"], 1)
        self.assertEqual(response.json()["total"], 1)
        bono = BonoProduccionEmpleado.objects.get(periodo=periodo, empleado=elegible)
        self.assertEqual(bono.area, AREA_PRODUCCION)
        self.assertFalse(BonoProduccionEmpleado.objects.filter(periodo=periodo, empleado=no_elegible).exists())

    def test_api_bonos_no_expone_borrador_no_elegible_por_rrhh(self):
        user = get_user_model().objects.create_user(username="bonos-api")
        user.groups.add(Group.objects.get_or_create(name=ROLE_PRODUCCION)[0])
        user.groups.add(Group.objects.get_or_create(name=ROLE_RRHH)[0])
        self.client.force_login(user)
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        elegible = Empleado.objects.create(
            nombre="Empleado Produccion Visible",
            area="PRODUCCION",
            participa_bonos_produccion=True,
        )
        no_elegible = Empleado.objects.create(
            nombre="ANGULO PARRA JULISSA",
            area="PRODUCCION",
            puesto_operativo="ENCARGADA_PRODUCCION",
            participa_bonos_produccion=False,
        )
        BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=elegible, area=AREA_PRODUCCION)
        BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=no_elegible, area=AREA_PRODUCCION)

        response = self.client.get("/api/bonos-produccion/bonos/?mes=5&anio=2026")

        self.assertEqual(response.status_code, 200)
        contenido = response.content.decode()
        self.assertIn(elegible.nombre, contenido)
        self.assertNotIn(no_elegible.nombre, contenido)

    def test_logistica_es_area_valida_de_bonos_produccion(self):
        periodo = ConfigBonoPeriodo.objects.create(mes=6, anio=2026, dias_laborables=10)
        empleado = Empleado.objects.create(nombre="Empleado Logistica", area="LOGISTICA")

        bono = BonoProduccionEmpleado.objects.create(
            periodo=periodo,
            empleado=empleado,
            area=AREA_LOGISTICA,
            dias_trabajados=10,
            dias_uniforme=10,
            dias_asistencia=10,
            dias_puntualidad=10,
            dias_produccion=10,
        )
        bono.recalcular()

        self.assertEqual(bono.area, AREA_LOGISTICA)
        self.assertEqual(bono.total_a_pagar, Decimal("850.00"))

    def test_permisos_equipo_produccion_crea_y_rechaza(self):
        user = get_user_model().objects.create_user(username="jefe-produccion")
        user.groups.add(Group.objects.get_or_create(name=ROLE_PRODUCCION)[0])
        user.groups.add(Group.objects.get_or_create(name=ROLE_RRHH)[0])
        self.client.force_login(user)
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        jefe = Empleado.objects.create(nombre="Jefe Produccion", departamento=Empleado.DEP_PRODUCCION, usuario_erp=user)
        empleado = Empleado.objects.create(
            nombre="Empleado Hornos A",
            area="PRODUCCION",
            jefe_directo=jefe,
            participa_bonos_produccion=True,
        )
        empleado_2 = Empleado.objects.create(
            nombre="Empleado Hornos B",
            area="PRODUCCION",
            jefe_directo=jefe,
            participa_bonos_produccion=True,
        )
        BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_HORNOS)
        BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado_2, area=AREA_HORNOS)
        Empleado.objects.create(nombre="Empleado Ventas", area="VENTAS")

        listado = self.client.get("/api/bonos-produccion/permisos/?mes=5&anio=2026&area=HORNOS")

        self.assertEqual(listado.status_code, 200)
        empleados_payload = listado.json()["empleados"]
        self.assertEqual([row["id"] for row in empleados_payload], [empleado.id, empleado_2.id])
        self.assertEqual({row["area"] for row in empleados_payload}, {AREA_HORNOS})

        creado = self.client.post(
            "/api/bonos-produccion/permisos/",
            json.dumps(
                {
                    "empleado": empleado.id,
                    "tipo": PermisoSalida.TIPO_PERMISO_DIA,
                    "fecha_inicio": "2026-05-21T08:00:00",
                    "fecha_fin": "2026-05-21T16:00:00",
                    "goce_sueldo": True,
                    "motivo": "Tramite familiar",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(creado.status_code, 201)
        permiso = PermisoSalida.objects.get(pk=creado.json()["id"])
        self.assertEqual(permiso.origen_solicitud, PermisoSalida.ORIGEN_BONOS_PRODUCCION)
        self.assertEqual(permiso.estado_jefe, PermisoSalida.ESTADO_JEFE_PENDIENTE)
        self.assertTrue(creado.json()["puede_preautorizar"])

        rechazado = self.client.post(f"/api/bonos-produccion/permisos/{permiso.id}/rechazar/")

        self.assertEqual(rechazado.status_code, 200)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado_jefe, PermisoSalida.ESTADO_JEFE_RECHAZADO)
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_RECHAZADO)
        self.assertEqual(permiso.autorizado_jefe_por, user)

    def test_horas_extra_produccion_crea_y_autoriza_en_rrhh(self):
        user = get_user_model().objects.create_user(username="jefe-produccion-he")
        user.groups.add(Group.objects.get_or_create(name=ROLE_PRODUCCION)[0])
        user.groups.add(Group.objects.get_or_create(name=ROLE_RRHH)[0])
        self.client.force_login(user)
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        jefe = Empleado.objects.create(nombre="Jefe Produccion HE", departamento=Empleado.DEP_PRODUCCION, usuario_erp=user)
        empleado = Empleado.objects.create(
            nombre="Empleado Hornos HE",
            area="PRODUCCION",
            jefe_directo=jefe,
            participa_bonos_produccion=True,
            salario_diario=Decimal("400.00"),
        )
        BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_HORNOS)

        creado = self.client.post(
            "/api/bonos-produccion/horas-extra/",
            json.dumps(
                {
                    "empleado": empleado.id,
                    "mes": 5,
                    "anio": 2026,
                    "area": AREA_HORNOS,
                    "fecha": "2026-05-21",
                    "horas": "2.50",
                    "notas": "Cierre de produccion",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(creado.status_code, 201)
        hora_extra = HoraExtra.objects.get(pk=creado.json()["id"])
        self.assertEqual(hora_extra.empleado, empleado)
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_PENDIENTE)
        self.assertEqual(hora_extra.jefe_directo, user)
        self.assertTrue(creado.json()["puede_autorizar"])
        self.assertTrue(creado.json()["puede_editar"])
        self.assertTrue(creado.json()["puede_eliminar"])
        self.assertIn("folio", creado.json())

        duplicado = self.client.post(
            "/api/bonos-produccion/horas-extra/",
            json.dumps(
                {
                    "empleado": empleado.id,
                    "mes": 5,
                    "anio": 2026,
                    "area": AREA_HORNOS,
                    "fecha": "2026-05-21",
                    "horas": "1.00",
                    "notas": "Duplicado",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(duplicado.status_code, 400)

        sin_motivo = self.client.post(
            f"/api/bonos-produccion/horas-extra/{hora_extra.id}/editar/",
            json.dumps({"fecha": "2026-05-22", "horas": "3.00", "notas": "Cierre corregido"}),
            content_type="application/json",
        )

        self.assertEqual(sin_motivo.status_code, 400)

        corregida = self.client.post(
            f"/api/bonos-produccion/horas-extra/{hora_extra.id}/editar/",
            json.dumps(
                {
                    "fecha": "2026-05-22",
                    "horas": "3.00",
                    "notas": "Cierre corregido",
                    "motivo_cambio": "Se capturo fecha incorrecta",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(corregida.status_code, 200)
        hora_extra.refresh_from_db()
        self.assertEqual(hora_extra.fecha.isoformat(), "2026-05-22")
        self.assertEqual(hora_extra.horas, Decimal("3.00"))
        self.assertIn("Se capturo fecha incorrecta", hora_extra.notas)

        hora_cancelada = HoraExtra.objects.create(
            empleado=empleado,
            fecha="2026-05-23",
            horas=Decimal("1.00"),
            jefe_directo=user,
            notas="Solicitud duplicada",
        )
        eliminado = self.client.post(
            f"/api/bonos-produccion/horas-extra/{hora_cancelada.id}/eliminar/",
            json.dumps({"motivo_cambio": "Solicitud duplicada"}),
            content_type="application/json",
        )

        self.assertEqual(eliminado.status_code, 200)
        hora_cancelada.refresh_from_db()
        self.assertEqual(hora_cancelada.estado, HoraExtra.ESTADO_CANCELADO)
        self.assertIn("Solicitud duplicada", hora_cancelada.notas)

        autorizado = self.client.post(f"/api/bonos-produccion/horas-extra/{hora_extra.id}/autorizar/")

        self.assertEqual(autorizado.status_code, 200)
        hora_extra.refresh_from_db()
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(hora_extra.autorizado_por, user)
        self.assertEqual(hora_extra.monto_calculado, Decimal("300.00"))

        rechazar_autorizada = self.client.post(f"/api/bonos-produccion/horas-extra/{hora_extra.id}/rechazar/")
        self.assertEqual(rechazar_autorizada.status_code, 400)
        hora_extra.refresh_from_db()
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_AUTORIZADO)

        editar_autorizada = self.client.post(
            f"/api/bonos-produccion/horas-extra/{hora_extra.id}/editar/",
            json.dumps(
                {
                    "fecha": "2026-05-22",
                    "horas": "2.00",
                    "notas": "Captura corregida despues de autorizar",
                    "motivo_cambio": "Se autorizo con horas incorrectas",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(editar_autorizada.status_code, 200)
        hora_extra.refresh_from_db()
        self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_AUTORIZADO)
        self.assertEqual(hora_extra.horas, Decimal("2.00"))
        self.assertEqual(hora_extra.monto_calculado, Decimal("200.00"))
        self.assertIn("Se autorizo con horas incorrectas", hora_extra.notas)

    def test_jefe_directo_corrige_y_elimina_permiso_aprobado_con_auditoria(self):
        user = get_user_model().objects.create_user(username="jefe-produccion-audit")
        user.groups.add(Group.objects.get_or_create(name=ROLE_PRODUCCION)[0])
        user.groups.add(Group.objects.get_or_create(name=ROLE_RRHH)[0])
        self.client.force_login(user)
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        jefe = Empleado.objects.create(nombre="Jefe Produccion Audit", departamento=Empleado.DEP_PRODUCCION, usuario_erp=user)
        empleado = Empleado.objects.create(
            nombre="Empleado Hornos Audit",
            area="PRODUCCION",
            jefe_directo=jefe,
            participa_bonos_produccion=True,
        )
        BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_HORNOS)
        permiso = PermisoSalida.objects.create(
            empleado=empleado,
            tipo=PermisoSalida.TIPO_PERMISO_HORA,
            fecha_inicio="2026-05-21T08:00:00-07:00",
            fecha_fin="2026-05-21T10:00:00-07:00",
            motivo="Cita",
            estado=PermisoSalida.ESTADO_APROBADO,
            estado_jefe=PermisoSalida.ESTADO_JEFE_PREAUTORIZADO,
            autorizado_jefe_por=user,
            autorizado_por=user,
            origen_solicitud=PermisoSalida.ORIGEN_BONOS_PRODUCCION,
        )

        listado = self.client.get("/api/bonos-produccion/permisos/?mes=5&anio=2026&area=HORNOS")

        self.assertEqual(listado.status_code, 200)
        permiso_payload = listado.json()["permisos"][0]
        self.assertTrue(permiso_payload["puede_editar"])
        self.assertTrue(permiso_payload["puede_eliminar"])
        self.assertFalse(permiso_payload["puede_preautorizar"])

        sin_motivo = self.client.post(
            f"/api/bonos-produccion/permisos/{permiso.id}/editar/?mes=5&anio=2026&area=TODAS",
            json.dumps(
                {
                    "tipo": PermisoSalida.TIPO_PERMISO_HORA,
                    "fecha_inicio": "2026-05-21T09:00:00",
                    "fecha_fin": "2026-05-21T11:00:00",
                    "goce_sueldo": True,
                    "motivo": "Cita corregida",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(sin_motivo.status_code, 400)
        self.assertEqual(PermisoSalidaCambio.objects.count(), 0)

        corregido = self.client.post(
            f"/api/bonos-produccion/permisos/{permiso.id}/editar/?mes=5&anio=2026&area=TODAS",
            json.dumps(
                {
                    "tipo": PermisoSalida.TIPO_PERMISO_HORA,
                    "fecha_inicio": "2026-05-21T09:00:00",
                    "fecha_fin": "2026-05-21T11:00:00",
                    "goce_sueldo": False,
                    "motivo": "Cita corregida",
                    "motivo_cambio": "Se capturo una hora incorrecta",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(corregido.status_code, 200)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_APROBADO)
        self.assertFalse(permiso.goce_sueldo)
        cambio = PermisoSalidaCambio.objects.get(accion=PermisoSalidaCambio.ACCION_EDITAR)
        self.assertEqual(cambio.motivo, "Se capturo una hora incorrecta")
        self.assertIn("fecha_inicio", cambio.cambios)
        self.assertIn("goce_sueldo", cambio.cambios)

        eliminado = self.client.post(
            f"/api/bonos-produccion/permisos/{permiso.id}/eliminar/?mes=5&anio=2026&area=TODAS",
            json.dumps({"motivo_cambio": "Permiso duplicado"}),
            content_type="application/json",
        )

        self.assertEqual(eliminado.status_code, 200)
        permiso.refresh_from_db()
        self.assertEqual(permiso.estado, PermisoSalida.ESTADO_CANCELADO)
        self.assertTrue(
            PermisoSalidaCambio.objects.filter(
                accion=PermisoSalidaCambio.ACCION_ELIMINAR,
                motivo="Permiso duplicado",
                folio=permiso.folio,
            ).exists()
        )

    def test_permiso_produccion_se_crea_con_roster_del_periodo_aunque_rrhh_tenga_otra_area(self):
        user = get_user_model().objects.create_user(username="julissa.angulo")
        user.groups.add(Group.objects.get_or_create(name=ROLE_PRODUCCION)[0])
        self.client.force_login(user)
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        empleado = Empleado.objects.create(
            nombre="MEZA TABIZON JESUS ADRIAN",
            area="ADMINISTRACION",
            activo=True,
            participa_bonos_produccion=True,
        )
        BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=empleado, area=AREA_LOGISTICA)

        response = self.client.post(
            "/api/bonos-produccion/permisos/",
            json.dumps(
                {
                    "empleado": empleado.id,
                    "mes": 5,
                    "anio": 2026,
                    "area": AREA_LOGISTICA,
                    "tipo": PermisoSalida.TIPO_PERMISO_HORA,
                    "fecha_inicio": "2026-05-21T12:00:00",
                    "fecha_fin": "2026-05-21T13:00:00",
                    "goce_sueldo": True,
                    "motivo": "Cita medica programada",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        permiso = PermisoSalida.objects.get(pk=response.json()["id"])
        self.assertEqual(permiso.empleado, empleado)
        self.assertEqual(permiso.origen_solicitud, PermisoSalida.ORIGEN_BONOS_PRODUCCION)

    def test_permisos_produccion_incluye_equipo_directo_sin_bono_periodo(self):
        user = get_user_model().objects.create_user(
            username="test.carolina.permisos",
            first_name="Carolina",
            last_name="Cayetano",
        )
        user.groups.add(Group.objects.get_or_create(name=ROLE_PRODUCCION)[0])
        self.client.force_login(user)
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        carolina = Empleado.objects.create(
            nombre="CAYETANO VALENZUELA CAROLINA",
            departamento="PRODUCCION",
            usuario_erp=user,
        )
        roxana = Empleado.objects.create(
            nombre="RIVAS SOLIS ROXANA",
            area="ADMINISTRACION",
            departamento="PRODUCCION",
            puesto="Supervisora de Producción",
            nivel_organizacional=Empleado.NIVEL_SUPERVISION,
            jefe_directo=carolina,
        )
        julissa = Empleado.objects.create(
            nombre="ANGULO PARRA JULISSA",
            area="PRODUCCION",
            departamento="PRODUCCION",
            puesto="Encargada de Producción",
            puesto_operativo="ENCARGADA_PRODUCCION",
            jefe_directo=carolina,
        )
        operativo = Empleado.objects.create(
            nombre="ACOSTA FLORES MARTINA",
            area="PRODUCCION",
            departamento="PRODUCCION",
            puesto="Producción",
            jefe_directo=carolina,
        )
        BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=julissa, area=AREA_PRODUCCION)

        listado = self.client.get("/api/bonos-produccion/permisos/?mes=5&anio=2026&area=PRODUCCION")

        self.assertEqual(listado.status_code, 200)
        empleados_ids = {row["id"] for row in listado.json()["empleados"]}
        self.assertIn(julissa.id, empleados_ids)
        self.assertIn(roxana.id, empleados_ids)
        self.assertIn(operativo.id, empleados_ids)
        roxana_payload = next(row for row in listado.json()["empleados"] if row["id"] == roxana.id)
        self.assertEqual(roxana_payload["area"], AREA_PRODUCCION)
        primeros = [row["id"] for row in listado.json()["empleados"][:2]]
        self.assertEqual(primeros, [roxana.id, julissa.id])

        creado = self.client.post(
            "/api/bonos-produccion/permisos/",
            json.dumps(
                {
                    "empleado": roxana.id,
                    "mes": 5,
                    "anio": 2026,
                    "area": AREA_PRODUCCION,
                    "tipo": PermisoSalida.TIPO_PERMISO_HORA,
                    "fecha_inicio": "2026-05-21T12:00:00",
                    "fecha_fin": "2026-05-21T13:00:00",
                    "goce_sueldo": True,
                    "motivo": "Permiso supervisora",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(creado.status_code, 201)
        permiso = PermisoSalida.objects.get(pk=creado.json()["id"])
        self.assertEqual(permiso.empleado, roxana)

        hora_extra = self.client.post(
            "/api/bonos-produccion/horas-extra/",
            json.dumps(
                {
                    "empleado": roxana.id,
                    "mes": 5,
                    "anio": 2026,
                    "area": "ADMINISTRACION",
                    "fecha": "2026-05-22",
                    "horas": "1.50",
                    "notas": "Cierre supervisora",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(hora_extra.status_code, 201)
        self.assertEqual(HoraExtra.objects.get(pk=hora_extra.json()["id"]).empleado, roxana)

        listado_horas = self.client.get("/api/bonos-produccion/horas-extra/?mes=5&anio=2026&area=PRODUCCION")

        self.assertEqual(listado_horas.status_code, 200)
        roxana_horas_payload = next(row for row in listado_horas.json()["empleados"] if row["id"] == roxana.id)
        self.assertEqual(roxana_horas_payload["area"], AREA_PRODUCCION)

    def test_permisos_produccion_superusuario_ve_area_sin_bono_periodo(self):
        user = get_user_model().objects.create_superuser(username="test.dg.permisos", password="x")
        self.client.force_login(user)
        periodo = ConfigBonoPeriodo.objects.create(mes=5, anio=2026)
        carolina = Empleado.objects.create(
            nombre="CAYETANO VALENZUELA CAROLINA",
            area="ADMINISTRACION",
            departamento="PRODUCCION",
            puesto="Jefe de Producción",
            nivel_organizacional=Empleado.NIVEL_JEFATURA,
        )
        roxana = Empleado.objects.create(
            nombre="RIVAS SOLIS ROXANA",
            area="PRODUCCION",
            departamento="PRODUCCION",
            puesto="Supervisora de Producción",
            puesto_operativo="SUPERVISION_PRODUCCION",
            jefe_directo=carolina,
        )
        julissa = Empleado.objects.create(
            nombre="ANGULO PARRA JULISSA",
            area="PRODUCCION",
            departamento="PRODUCCION",
            puesto="Encargada de Producción",
            puesto_operativo="ENCARGADA_PRODUCCION",
            jefe_directo=carolina,
        )
        envio = Empleado.objects.create(
            nombre="MEZA TABIZON JESUS ADRIAN",
            area="ENVIO A SUCURSAL",
            departamento="PRODUCCION",
            departamento_origen="LOGISTICA",
            puesto="Envio a sucursales",
            puesto_operativo="ENVIO_SUCURSAL",
            jefe_directo=carolina,
        )
        BonoProduccionEmpleado.objects.create(periodo=periodo, empleado=julissa, area=AREA_PRODUCCION)

        listado = self.client.get("/api/bonos-produccion/permisos/?mes=5&anio=2026")

        self.assertEqual(listado.status_code, 200)
        empleados_ids = {row["id"] for row in listado.json()["empleados"]}
        self.assertIn(carolina.id, empleados_ids)
        self.assertIn(julissa.id, empleados_ids)
        self.assertIn(roxana.id, empleados_ids)
        self.assertIn(envio.id, empleados_ids)
        primeros = [row["id"] for row in listado.json()["empleados"][:3]]
        self.assertEqual(primeros, [carolina.id, roxana.id, julissa.id])

        creado = self.client.post(
            "/api/bonos-produccion/permisos/",
            json.dumps(
                {
                    "empleado": envio.id,
                    "mes": 5,
                    "anio": 2026,
                    "area": AREA_LOGISTICA,
                    "tipo": PermisoSalida.TIPO_PERMISO_HORA,
                    "fecha_inicio": "2026-05-22T12:00:00",
                    "fecha_fin": "2026-05-22T13:00:00",
                    "goce_sueldo": True,
                    "motivo": "Permiso equipo de produccion desde DG",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(creado.status_code, 201)
        permiso = PermisoSalida.objects.get(pk=creado.json()["id"])
        self.assertEqual(permiso.empleado, envio)
