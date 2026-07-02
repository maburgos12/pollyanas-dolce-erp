import json
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.test import RequestFactory, TestCase
from django.urls import reverse

from core.models import Sucursal
from recetas.models import Receta
from rentabilidad.models import SucursalRentabilidad
from reportes.models import ProductoSucursalContribucionMensual

from . import views
from .models import ConsejoConsulta
from .services import _llamar_rol, analizar_pregunta, construir_snapshot


def _mock_openai_response(content: str):
    response = MagicMock()
    response.choices = [MagicMock(message=MagicMock(content=content))]
    return response


class LlamarRolTests(TestCase):
    @patch("consejo_ia.services.OpenAI")
    def test_json_valido_se_parsea(self, mock_openai_cls):
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _mock_openai_response(
            '{"analisis": "todo bien", "supuestos": [], "datos_faltantes": []}'
        )
        mock_openai_cls.return_value = mock_client

        resultado = _llamar_rol("system", "user")

        self.assertEqual(resultado["analisis"], "todo bien")

    @patch("consejo_ia.services.OpenAI")
    def test_json_invalido_cae_a_fallback(self, mock_openai_cls):
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = _mock_openai_response("no es json")
        mock_openai_cls.return_value = mock_client

        resultado = _llamar_rol("system", "user")

        self.assertIn("error", resultado)

    @patch("consejo_ia.services.OpenAI")
    def test_excepcion_cae_a_fallback(self, mock_openai_cls):
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = RuntimeError("boom")
        mock_openai_cls.return_value = mock_client

        resultado = _llamar_rol("system", "user")

        self.assertIn("error", resultado)


class ConstruirSnapshotTests(TestCase):
    def test_dashboard_no_disponible_se_declara_honesto(self):
        with patch("consejo_ia.services.get_materialized_dashboard_full_payload", return_value=None):
            snapshot = construir_snapshot()

        self.assertFalse(snapshot["dashboard_ejecutivo"]["disponible"])

    def test_sucursales_y_productos_disponibles(self):
        sucursal = Sucursal.objects.create(codigo="S1", nombre="Sucursal Uno", fecha_apertura=date(2020, 1, 1))
        SucursalRentabilidad.objects.create(
            sucursal=sucursal,
            periodo=date(2026, 5, 1),
            ventas_brutas=Decimal("100000"),
            descuentos=Decimal("0"),
            devoluciones=Decimal("0"),
            costo_materia_prima=Decimal("0"),
            costo_reventa=Decimal("0"),
            empaque=Decimal("0"),
            otros_costos_variables=Decimal("0"),
            renta=Decimal("0"),
            nomina_directa=Decimal("0"),
            servicios_luz_agua=Decimal("0"),
            mantenimiento=Decimal("0"),
            gastos_admin_prorrateados=Decimal("0"),
            otros_gastos_fijos=Decimal("0"),
        )
        receta = Receta.objects.create(
            nombre="Pastel Test",
            codigo_point="CONSEJO1",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            modo_costeo=Receta.MODO_COSTEO_FABRICADO,
            familia="ERP",
            hash_contenido=f"h-{uuid4()}",
        )
        ProductoSucursalContribucionMensual.objects.create(
            periodo=date(2026, 5, 1),
            receta=receta,
            sucursal=sucursal,
            contribucion_total=1000,
        )

        with patch("consejo_ia.services.get_materialized_dashboard_full_payload", return_value=None):
            snapshot = construir_snapshot()

        self.assertTrue(snapshot["rentabilidad_sucursal"]["disponible"])
        self.assertIn("Sucursal Uno", snapshot["rentabilidad_sucursal"]["texto"])
        self.assertTrue(snapshot["rentabilidad_producto"]["disponible"])
        self.assertIn("Pastel Test", snapshot["rentabilidad_producto"]["texto"])


ROL_OK = {"analisis": "ok", "supuestos": [], "datos_faltantes": []}
CEO_OK = {"veredicto": "PILOTO", "resumen_ejecutivo": "Probar piloto", "conclusion": "Porque sí"}


class AnalizarPreguntaTests(TestCase):
    def setUp(self):
        self.user_model = get_user_model()
        self.usuario = self.user_model.objects.create_user(username="dg_test", password="pass12345")

    @patch("consejo_ia.services.construir_snapshot", return_value={
        "dashboard_ejecutivo": {"disponible": False, "texto": "no disponible"},
        "rentabilidad_sucursal": {"disponible": False, "texto": "no disponible"},
        "rentabilidad_producto": {"disponible": False, "texto": "no disponible"},
    })
    @patch("consejo_ia.services._llamar_rol")
    def test_persiste_veredicto_y_respuestas(self, mock_llamar_rol, _mock_snapshot):
        mock_llamar_rol.side_effect = [ROL_OK] * 8 + [CEO_OK]

        consulta = analizar_pregunta("¿Conviene abrir en Los Mochis?", usuario=self.usuario)

        self.assertEqual(consulta.veredicto_ceo, "PILOTO")
        self.assertEqual(consulta.resumen_ejecutivo_ceo, "Probar piloto")
        self.assertEqual(len(consulta.respuestas_json), 9)  # 8 roles + ceo
        self.assertTrue(ConsejoConsulta.objects.filter(pk=consulta.pk).exists())

    @patch("consejo_ia.services.construir_snapshot", return_value={
        "dashboard_ejecutivo": {"disponible": False, "texto": "no disponible"},
        "rentabilidad_sucursal": {"disponible": False, "texto": "no disponible"},
        "rentabilidad_producto": {"disponible": False, "texto": "no disponible"},
    })
    @patch("consejo_ia.services._llamar_rol")
    def test_un_rol_fallido_no_tumba_la_consulta(self, mock_llamar_rol, _mock_snapshot):
        rol_error = {"error": "boom"}
        mock_llamar_rol.side_effect = [rol_error] + [ROL_OK] * 7 + [CEO_OK]

        consulta = analizar_pregunta("¿Conviene abrir en Los Mochis?", usuario=self.usuario)

        self.assertEqual(consulta.veredicto_ceo, "PILOTO")
        self.assertEqual(consulta.respuestas_json["cfo"], rol_error)

    @patch("consejo_ia.services.construir_snapshot", return_value={
        "dashboard_ejecutivo": {"disponible": False, "texto": "no disponible"},
        "rentabilidad_sucursal": {"disponible": False, "texto": "no disponible"},
        "rentabilidad_producto": {"disponible": False, "texto": "no disponible"},
    })
    @patch("consejo_ia.services._llamar_rol")
    def test_veredicto_invalido_cae_a_pedir_datos(self, mock_llamar_rol, _mock_snapshot):
        ceo_malo = {"veredicto": "ALGO_RARO", "resumen_ejecutivo": "x"}
        mock_llamar_rol.side_effect = [ROL_OK] * 8 + [ceo_malo]

        consulta = analizar_pregunta("¿Conviene abrir en Los Mochis?", usuario=self.usuario)

        self.assertEqual(consulta.veredicto_ceo, ConsejoConsulta.VEREDICTO_PEDIR_DATOS)


class ConsejoIaViewTests(TestCase):
    def setUp(self):
        self.user_model = get_user_model()
        self.superuser = self.user_model.objects.create_superuser(
            username="dg_super", email="dg_super@example.com", password="pass12345",
        )
        self.usuario_normal = self.user_model.objects.create_user(username="normal", password="pass12345")

    def test_usuario_sin_permiso_recibe_403(self):
        factory = RequestFactory()
        request = factory.get("/consejo-ia/")
        request.user = self.usuario_normal

        with self.assertRaises(PermissionDenied):
            views.consejo_ia_home(request)

    def test_superuser_puede_ver_formulario(self):
        self.client.force_login(self.superuser)
        response = self.client.get(reverse("consejo_ia:consejo_ia_home"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Pregunta al Consejo")

    def test_pregunta_muy_corta_no_llama_openai(self):
        self.client.force_login(self.superuser)
        with patch("consejo_ia.services._llamar_rol") as mock_llamar_rol:
            response = self.client.post(reverse("consejo_ia:consejo_ia_home"), {"pregunta": "corta"})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "más completa")
        mock_llamar_rol.assert_not_called()

    @patch("consejo_ia.services.construir_snapshot", return_value={
        "dashboard_ejecutivo": {"disponible": False, "texto": "no disponible"},
        "rentabilidad_sucursal": {"disponible": False, "texto": "no disponible"},
        "rentabilidad_producto": {"disponible": False, "texto": "no disponible"},
    })
    @patch("consejo_ia.services._llamar_rol")
    def test_post_valido_renderiza_resultado_y_guarda_historial(self, mock_llamar_rol, _mock_snapshot):
        mock_llamar_rol.side_effect = [ROL_OK] * 8 + [CEO_OK]
        self.client.force_login(self.superuser)

        response = self.client.post(
            reverse("consejo_ia:consejo_ia_home"),
            {"pregunta": "¿Conviene abrir una sucursal en Los Mochis?"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Probar piloto")
        self.assertEqual(ConsejoConsulta.objects.count(), 1)
