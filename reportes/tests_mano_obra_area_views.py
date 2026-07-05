from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from maestros.models import Insumo
from reportes.models import FamiliaGrupoManoObra, RecetaAreaProduccion


class ManoObraAreaViewsRBACTests(TestCase):
    def setUp(self):
        self.user_model = get_user_model()
        self.superuser = self.user_model.objects.create_superuser(
            username="moa_super", email="moa_super@example.com", password="pass12345",
        )
        self.usuario_normal = self.user_model.objects.create_user(username="moa_normal", password="pass12345")

    def test_usuario_sin_permiso_recibe_403_en_clasificacion(self):
        self.client.force_login(self.usuario_normal)
        response = self.client.get(reverse("reportes:mano_obra_area_clasificacion"))
        self.assertEqual(response.status_code, 403)

    def test_usuario_sin_permiso_recibe_403_en_reporte(self):
        self.client.force_login(self.usuario_normal)
        response = self.client.get(reverse("reportes:mano_obra_area_reporte"))
        self.assertEqual(response.status_code, 403)

    def test_superuser_ve_clasificacion(self):
        self.client.force_login(self.superuser)
        response = self.client.get(reverse("reportes:mano_obra_area_clasificacion"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Productos")
        self.assertContains(response, "Catálogos")

    def test_superuser_ve_reporte(self):
        self.client.force_login(self.superuser)
        response = self.client.get(reverse("reportes:mano_obra_area_reporte"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Hornos")

    def test_reporte_calcula_pct_aprovechamiento_sin_dividir_en_template(self):
        from datetime import timedelta

        from django.utils import timezone

        from rrhh.models import Empleado, NominaLinea, NominaPeriodo

        hoy = timezone.localdate()
        empleado = Empleado.objects.create(
            codigo="E-APROV1", nombre="Empleado Test", departamento=Empleado.DEP_PRODUCCION,
            puesto_operativo="HORNOS", fecha_ingreso=hoy - timedelta(days=365), salario_diario=Decimal("400.00"),
        )
        periodo = NominaPeriodo.objects.create(
            fecha_inicio=hoy - timedelta(days=7), fecha_fin=hoy + timedelta(days=7),
            estatus=NominaPeriodo.ESTATUS_CERRADA,
        )
        NominaLinea.objects.create(periodo=periodo, empleado=empleado, salario_base=Decimal("4200.00"))

        self.client.force_login(self.superuser)
        response = self.client.get(reverse("reportes:mano_obra_area_reporte"))

        bloque_hornos = next(b for b in response.context["bloques"] if b["valor"] == "HORNOS")
        # 1 empleado * 480 min disponibles, sin producción calibrada ese
        # período -> 0 minutos demandados -> 0% de aprovechamiento, no None.
        self.assertEqual(bloque_hornos["hoy"].minutos_disponibles, Decimal("480"))
        self.assertEqual(bloque_hornos["hoy"].pct_aprovechamiento, Decimal("0"))


class ClasificacionAreaProduccionTests(TestCase):
    def setUp(self):
        self.user_model = get_user_model()
        self.superuser = self.user_model.objects.create_superuser(
            username="moa_super2", email="moa_super2@example.com", password="pass12345",
        )
        self.client.force_login(self.superuser)
        FamiliaGrupoManoObra.objects.create(familia_real="Pastel Chico", grupo="Pastel")
        FamiliaGrupoManoObra.objects.create(familia_real="Pastel Grande", grupo="Pastel")

    def test_toggle_familia_crea_y_quita(self):
        url = reverse("reportes:mano_obra_area_clasificacion")

        self.client.post(url, {"accion": "toggle_familia", "familia": "Pastel", "area": "HORNOS"})
        self.assertTrue(RecetaAreaProduccion.objects.filter(familia="Pastel", area="HORNOS").exists())

        self.client.post(url, {"accion": "toggle_familia", "familia": "Pastel", "area": "HORNOS"})
        self.assertFalse(RecetaAreaProduccion.objects.filter(familia="Pastel", area="HORNOS").exists())

    def test_toggle_familia_variante_guarda_grupo_canonico(self):
        # El formulario puede enviar cualquier familia real de Point que
        # pertenezca al grupo (ej. "Pastel Chico"); se persiste el grupo
        # canónico ("Pastel"), no el texto crudo del formulario.
        url = reverse("reportes:mano_obra_area_clasificacion")

        self.client.post(url, {"accion": "toggle_familia", "familia": "Pastel Chico", "area": "EMBETUNADO"})

        self.assertTrue(RecetaAreaProduccion.objects.filter(familia="Pastel", area="EMBETUNADO").exists())
        self.assertFalse(RecetaAreaProduccion.objects.filter(familia="Pastel Chico", area="EMBETUNADO").exists())

    def test_clasificacion_agrega_variantes_de_pastel_en_una_sola_tarjeta(self):
        from uuid import uuid4

        from recetas.models import Receta

        for nombre, familia in [
            ("Pastel Chico Fresa", "Pastel Chico"),
            ("Pastel Grande Chocolate", "Pastel Grande"),
            ("Pastel Tres Leches", "Pastel"),
        ]:
            Receta.objects.create(
                nombre=nombre,
                codigo_point=f"COD-{uuid4().hex[:6]}",
                tipo=Receta.TIPO_PRODUCTO_FINAL,
                modo_costeo=Receta.MODO_COSTEO_FABRICADO,
                familia=familia,
                hash_contenido=f"h-{uuid4()}",
            )

        url = reverse("reportes:mano_obra_area_clasificacion")
        response = self.client.get(url)

        familias_ctx = response.context["familias"]
        grupo_pastel = next(entrada for entrada in familias_ctx if entrada["nombre"] == "Pastel")

        self.assertEqual(grupo_pastel["cantidad"], 3)
        self.assertEqual(
            grupo_pastel["familias_reales"], ["Pastel", "Pastel Chico", "Pastel Grande"]
        )
        self.assertContains(response, "Incluye de Point:")
        # No debe aparecer una tarjeta separada por cada variante cruda.
        nombres = [entrada["nombre"] for entrada in familias_ctx]
        self.assertNotIn("Pastel Chico", nombres)
        self.assertNotIn("Pastel Grande", nombres)

    def test_capturar_lote_calcula_minutos_estandar_pieza(self):
        url = reverse("reportes:mano_obra_area_clasificacion")

        self.client.post(url, {
            "accion": "capturar_lote",
            "familia": "Pastel",
            "area": "HORNOS",
            "lote_personas": "2",
            "lote_minutos": "20",
            "lote_piezas": "30",
        })

        fila = RecetaAreaProduccion.objects.get(familia="Pastel", area="HORNOS")
        self.assertEqual(fila.lote_personas, 2)
        self.assertEqual(fila.lote_minutos, Decimal("20"))
        self.assertEqual(fila.lote_piezas, 30)
        self.assertEqual(fila.minutos_estandar_pieza, Decimal("40") / Decimal("30"))

    def test_fusionar_grupo_cambia_grupo_sin_tocar_codigo(self):
        # Simula el caso real encontrado: "RELLENOS Y CREMAS" es una
        # familia real nueva de Point que Carolina fusiona a un grupo ya
        # existente, sin depender de un cambio de código.
        FamiliaGrupoManoObra.objects.create(familia_real="RELLENOS Y CREMAS", grupo="RELLENOS Y CREMAS")
        url = reverse("reportes:mano_obra_area_clasificacion")

        self.client.post(url, {
            "accion": "fusionar_grupo",
            "familia_real": "RELLENOS Y CREMAS",
            "grupo_destino": "Betún, Cremas, Rellenos (INSUMO PRODUCIDO)",
        })

        fila = FamiliaGrupoManoObra.objects.get(familia_real="RELLENOS Y CREMAS")
        self.assertEqual(fila.grupo, "Betún, Cremas, Rellenos (INSUMO PRODUCIDO)")

    def test_agregar_y_quitar_excepcion(self):
        from uuid import uuid4

        from recetas.models import Receta

        receta = Receta.objects.create(
            nombre="Pay Especial",
            codigo_point=f"COD-{uuid4().hex[:6]}",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            modo_costeo=Receta.MODO_COSTEO_FABRICADO,
            familia="Pay",
            hash_contenido=f"h-{uuid4()}",
        )
        url = reverse("reportes:mano_obra_area_clasificacion")

        self.client.post(url, {"accion": "agregar_excepcion", "receta_id": receta.id, "area": "EMBETUNADO"})
        fila = RecetaAreaProduccion.objects.get(receta=receta, area="EMBETUNADO")

        self.client.post(url, {"accion": "quitar_excepcion", "fila_id": fila.id})
        self.assertFalse(RecetaAreaProduccion.objects.filter(id=fila.id).exists())


class ClasificacionCatalogosTests(TestCase):
    """Catálogos de Point (Insumo) — namespace separado de Productos,
    calibrado por preparación específica con unidad real detectada."""

    def setUp(self):
        from datetime import date
        from uuid import uuid4

        from pos_bridge.models import PointBranch, PointProductionLine

        self.user_model = get_user_model()
        self.superuser = self.user_model.objects.create_superuser(
            username="moa_catalogos", email="moa_catalogos@example.com", password="pass12345",
        )
        self.client.force_login(self.superuser)
        self.branch = PointBranch.objects.create(external_id=f"B-{uuid4().hex[:6]}", name="Sucursal Test")
        self.betun = Insumo.objects.create(
            nombre="Betún Dream Whip Pastel", tipo_item=Insumo.TIPO_INTERNO,
            categoria="Betún, Cremas, Rellenos (INSUMO PRODUCIDO)",
        )
        PointProductionLine.objects.create(
            branch=self.branch, insumo=self.betun, item_name=self.betun.nombre, unit="KG",
            produced_quantity=Decimal("20"), production_date=date(2026, 6, 1),
            source_hash=str(uuid4()),
        )

    def test_get_clasificacion_muestra_catalogos_con_unidad_detectada(self):
        url = reverse("reportes:mano_obra_area_clasificacion")
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        catalogos_ctx = response.context["catalogos"]
        grupo_betun = next(e for e in catalogos_ctx if e["nombre"] == "Betún Dream Whip Pastel")
        self.assertEqual(grupo_betun["unidad_detectada"], "KG")
        self.assertContains(response, "Betún Dream Whip Pastel")

    def test_capturar_lote_insumo_no_colisiona_con_producto_mismo_texto(self):
        from uuid import uuid4

        from recetas.models import Receta

        Receta.objects.create(
            nombre="Betún Dream Whip Pastel Receta", codigo_point=f"COD-{uuid4().hex[:6]}",
            tipo=Receta.TIPO_PRODUCTO_FINAL, modo_costeo=Receta.MODO_COSTEO_FABRICADO,
            familia="Betún Dream Whip Pastel", hash_contenido=f"h-{uuid4()}",
        )
        url = reverse("reportes:mano_obra_area_clasificacion")

        self.client.post(url, {
            "accion": "capturar_lote", "es_grupo_insumo": "1",
            "familia": "Betún Dream Whip Pastel", "area": "EMBETUNADO",
            "lote_personas": "1", "lote_minutos": "30", "lote_piezas": "10",
        })
        self.client.post(url, {
            "accion": "toggle_familia", "familia": "Betún Dream Whip Pastel", "area": "HORNOS",
        })

        fila_insumo = RecetaAreaProduccion.objects.get(
            familia="Betún Dream Whip Pastel", area="EMBETUNADO", es_grupo_insumo=True
        )
        fila_producto = RecetaAreaProduccion.objects.get(
            familia="Betún Dream Whip Pastel", area="HORNOS", es_grupo_insumo=False
        )
        self.assertEqual(fila_insumo.minutos_estandar_pieza, Decimal("3"))
        self.assertIsNone(fila_producto.minutos_estandar_pieza)

    def test_fusionar_insumo_propaga_a_grupo_ya_fusionado(self):
        pan_chico = Insumo.objects.create(
            nombre="Pan Vainilla Dawn Chico", tipo_item=Insumo.TIPO_INTERNO,
            categoria="PAN", grupo_mano_obra="Pan Vainilla Dawn",
        )
        pan_grande = Insumo.objects.create(
            nombre="Pan Vainilla Dawn Grande", tipo_item=Insumo.TIPO_INTERNO,
            categoria="PAN", grupo_mano_obra="Pan Vainilla Dawn",
        )
        url = reverse("reportes:mano_obra_area_clasificacion")

        self.client.post(url, {
            "accion": "fusionar_insumo",
            "grupo_actual": "Pan Vainilla Dawn",
            "grupo_destino": "Pan Dawn General",
        })

        pan_chico.refresh_from_db()
        pan_grande.refresh_from_db()
        self.assertEqual(pan_chico.grupo_mano_obra, "Pan Dawn General")
        self.assertEqual(pan_grande.grupo_mano_obra, "Pan Dawn General")
