from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.exceptions import ValidationError
from django.test import TestCase
from django.urls import reverse

from core.models import UserModuleAccess
from core.private_operational_media import _can_access_operational_media

from maestros.models import Proveedor
from reportes.models import (
    AreaPresupuesto,
    AreaPresupuestoResponsable,
    LineaPresupuestoMensual,
    RubroPresupuesto,
)

from .models import (
    CompromisoCompraDepartamental,
    CotizacionCompraDepartamental,
    ItemCompraDepartamental,
    RecepcionItemDepartamental,
    SolicitudCompraDepartamental,
)
from .services_departamentales import (
    confirmar_recepcion_departamental,
    evaluar_presupuesto_item,
    generar_ordenes_departamentales,
    seleccionar_cotizacion,
)


class ComprasDepartamentalesDomainTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.responsable = user_model.objects.create_user("johana", password="test")
        self.comprador = user_model.objects.create_user("comprador", password="test")
        self.area = AreaPresupuesto.objects.create(nombre="Ventas", codigo="ventas")
        AreaPresupuestoResponsable.objects.create(area=self.area, usuario=self.responsable)
        self.rubro = RubroPresupuesto.objects.create(
            area=self.area,
            concepto="Herramientas y equipo",
            tipo=RubroPresupuesto.TIPO_EGRESO,
        )
        LineaPresupuestoMensual.objects.create(
            rubro=self.rubro,
            periodo=date(2026, 9, 1),
            version=LineaPresupuestoMensual.VERSION_REVISADO,
            monto_presupuesto=Decimal("10000.00"),
            monto_real=Decimal("2500.00"),
        )
        self.proveedor_a = Proveedor.objects.create(nombre="Proveedor A")
        self.proveedor_b = Proveedor.objects.create(nombre="Proveedor B")

    def crear_solicitud(self, tipo=SolicitudCompraDepartamental.TIPO_MENSUAL):
        return SolicitudCompraDepartamental.objects.create(
            area=self.area,
            solicitante=self.responsable,
            tipo=tipo,
            periodo=date(2026, 9, 1),
            motivo="Necesidades del área",
        )

    def test_solicitud_admite_varios_articulos_independientes_y_costo_estimado_opcional(self):
        solicitud = self.crear_solicitud()
        rack = ItemCompraDepartamental.objects.create(
            solicitud=solicitud,
            descripcion="Rack nuevo",
            cantidad=2,
            unidad="pieza",
            rubro=self.rubro,
            costo_unitario_estimado=Decimal("1800.00"),
        )
        placa = ItemCompraDepartamental.objects.create(
            solicitud=solicitud,
            descripcion="Placa de reconocimiento",
            cantidad=1,
            unidad="pieza",
            rubro=self.rubro,
        )

        self.assertEqual(solicitud.items.count(), 2)
        self.assertEqual(rack.subtotal_estimado, Decimal("3600.00"))
        self.assertIsNone(placa.subtotal_estimado)
        self.assertEqual(rack.estado, ItemCompraDepartamental.ESTADO_POR_REVISAR)

    def test_extraordinaria_requiere_justificacion(self):
        solicitud = self.crear_solicitud(SolicitudCompraDepartamental.TIPO_EXTRAORDINARIA)
        solicitud.justificacion_extraordinaria = ""
        with self.assertRaises(ValidationError):
            solicitud.full_clean()

    def test_cotizacion_calcula_total_y_costo_efectivo_unitario(self):
        item = ItemCompraDepartamental.objects.create(
            solicitud=self.crear_solicitud(), descripcion="Aire acondicionado", cantidad=2, rubro=self.rubro
        )
        cotizacion = CotizacionCompraDepartamental.objects.create(
            item=item,
            proveedor=self.proveedor_a,
            cantidad_ofertada=2,
            costo_unitario=Decimal("3000.00"),
            descuento=Decimal("500.00"),
            impuestos=Decimal("880.00"),
            envio=Decimal("120.00"),
        )

        self.assertEqual(cotizacion.subtotal, Decimal("6000.00"))
        self.assertEqual(cotizacion.total_adquisicion, Decimal("6500.00"))
        self.assertEqual(cotizacion.costo_efectivo_unitario, Decimal("3250.00"))

    def test_seleccion_evalua_presupuesto_y_solo_escala_a_dg_si_hay_exceso(self):
        item = ItemCompraDepartamental.objects.create(
            solicitud=self.crear_solicitud(), descripcion="Equipo de etiquetas", cantidad=1, rubro=self.rubro
        )
        cotizacion = CotizacionCompraDepartamental.objects.create(
            item=item,
            proveedor=self.proveedor_a,
            cantidad_ofertada=1,
            costo_unitario=Decimal("8000.00"),
        )

        resultado = seleccionar_cotizacion(cotizacion, actor=self.comprador)

        item.refresh_from_db()
        self.assertEqual(resultado.disponible_antes, Decimal("7500.00"))
        self.assertEqual(resultado.disponible_despues, Decimal("-500.00"))
        self.assertEqual(resultado.exceso, Decimal("500.00"))
        self.assertEqual(item.estado, ItemCompraDepartamental.ESTADO_ESPERANDO_DG)
        self.assertEqual(item.siguiente_responsable, ItemCompraDepartamental.RESPONSABLE_DG)

    def test_compromisos_activos_reducen_disponible_sin_contarse_como_gasto(self):
        primera = ItemCompraDepartamental.objects.create(
            solicitud=self.crear_solicitud(), descripcion="Rack", cantidad=1, rubro=self.rubro
        )
        cotizacion = CotizacionCompraDepartamental.objects.create(
            item=primera, proveedor=self.proveedor_a, cantidad_ofertada=1, costo_unitario=Decimal("3000.00")
        )
        seleccionar_cotizacion(cotizacion, actor=self.comprador)

        segunda = ItemCompraDepartamental.objects.create(
            solicitud=self.crear_solicitud(), descripcion="Uniformes", cantidad=1, rubro=self.rubro
        )
        resultado = evaluar_presupuesto_item(segunda, Decimal("5000.00"))

        self.assertEqual(resultado.gasto_real, Decimal("2500.00"))
        self.assertEqual(resultado.compromisos_previos, Decimal("3000.00"))
        self.assertEqual(resultado.disponible_antes, Decimal("4500.00"))
        self.assertEqual(resultado.exceso, Decimal("500.00"))

    def test_una_solicitud_se_divide_en_ordenes_por_proveedor(self):
        solicitud = self.crear_solicitud()
        items = [
            ItemCompraDepartamental.objects.create(solicitud=solicitud, descripcion="Rack", cantidad=1, rubro=self.rubro),
            ItemCompraDepartamental.objects.create(solicitud=solicitud, descripcion="Espátulas", cantidad=3, rubro=self.rubro),
            ItemCompraDepartamental.objects.create(solicitud=solicitud, descripcion="Uniformes", cantidad=4, rubro=self.rubro),
        ]
        for item, proveedor, monto in zip(
            items,
            [self.proveedor_a, self.proveedor_a, self.proveedor_b],
            ["1000.00", "500.00", "500.00"],
        ):
            cotizacion = CotizacionCompraDepartamental.objects.create(
                item=item, proveedor=proveedor, cantidad_ofertada=item.cantidad, costo_unitario=Decimal(monto)
            )
            seleccionar_cotizacion(cotizacion, actor=self.comprador)

        ordenes = generar_ordenes_departamentales(items, actor=self.comprador)

        self.assertEqual(len(ordenes), 2)
        self.assertEqual(sorted(orden.lineas.count() for orden in ordenes), [1, 2])
        self.assertTrue(all(item.estado == ItemCompraDepartamental.ESTADO_ORDENADO for item in items))
        self.assertEqual(
            CompromisoCompraDepartamental.objects.filter(
                item__in=items, formalizado_en__isnull=False, activo=True
            ).count(),
            3,
        )

    def test_recepcion_parcial_permanece_pendiente_hasta_confirmacion_del_area(self):
        item = ItemCompraDepartamental.objects.create(
            solicitud=self.crear_solicitud(), descripcion="Moldes", cantidad=4, rubro=self.rubro
        )
        cotizacion = CotizacionCompraDepartamental.objects.create(
            item=item, proveedor=self.proveedor_a, cantidad_ofertada=4, costo_unitario=Decimal("200.00")
        )
        seleccionar_cotizacion(cotizacion, actor=self.comprador)
        orden = generar_ordenes_departamentales([item], actor=self.comprador)[0]
        linea = orden.lineas.get()
        RecepcionItemDepartamental.objects.create(linea_orden=linea, cantidad_recibida=2, registrado_por=self.comprador)

        item.refresh_from_db()
        self.assertEqual(item.estado, ItemCompraDepartamental.ESTADO_RECIBIDO_PARCIAL)

        RecepcionItemDepartamental.objects.create(linea_orden=linea, cantidad_recibida=2, registrado_por=self.comprador)
        confirmar_recepcion_departamental(item, conforme=True, actor=self.responsable)
        item.refresh_from_db()
        item.solicitud.refresh_from_db()
        self.assertEqual(item.estado, ItemCompraDepartamental.ESTADO_RECIBIDO_CONFORME)
        self.assertEqual(item.solicitud.estado, SolicitudCompraDepartamental.ESTADO_COMPLETADA)


class ComprasDepartamentalesViewTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.responsable = user_model.objects.create_user("paula", password="test")
        self.ajena = user_model.objects.create_user("otra", password="test")
        self.comprador = user_model.objects.create_user("jorge", password="test")
        self.dg = user_model.objects.create_user("direccion", password="test")
        self.area = AreaPresupuesto.objects.create(nombre="Capital Humano", codigo="capital-humano")
        self.otra_area = AreaPresupuesto.objects.create(nombre="Administración", codigo="administracion")
        AreaPresupuestoResponsable.objects.create(area=self.area, usuario=self.responsable)
        AreaPresupuestoResponsable.objects.create(area=self.otra_area, usuario=self.ajena)
        UserModuleAccess.objects.create(
            user=self.comprador, module="compras", access=UserModuleAccess.ACCESS_MANAGE
        )
        self.dg.user_permissions.add(Permission.objects.get(codename="decidir_exceso_compra_departamental"))

    def test_responsable_crea_solicitud_con_varios_articulos_y_imagen_opcional(self):
        self.client.force_login(self.responsable)
        response = self.client.post(
            reverse("compras:departamental_nueva"),
            {
                "area": self.area.pk,
                "tipo": SolicitudCompraDepartamental.TIPO_EXTRAORDINARIA,
                "periodo": "2026-09",
                "motivo": "Necesidades no previstas",
                "justificacion_extraordinaria": "Se requiere antes del próximo ciclo",
                "accion": "enviar",
                "descripcion": ["Uniformes", "Placa de reconocimiento"],
                "cantidad": ["6", "1"],
                "unidad": ["pieza", "pieza"],
                "categoria": ["Uniformes", "Reconocimientos"],
                "costo_unitario_estimado": ["350.00", ""],
            },
        )

        self.assertEqual(response.status_code, 302)
        solicitud = SolicitudCompraDepartamental.objects.get()
        self.assertEqual(solicitud.items.count(), 2)
        self.assertEqual(solicitud.estado, SolicitudCompraDepartamental.ESTADO_ENVIADA)
        self.assertIsNone(solicitud.items.get(descripcion="Placa de reconocimiento").costo_unitario_estimado)

    def test_responsable_no_puede_crear_para_un_area_ajena(self):
        self.client.force_login(self.responsable)
        response = self.client.post(
            reverse("compras:departamental_nueva"),
            {
                "area": self.otra_area.pk,
                "tipo": SolicitudCompraDepartamental.TIPO_EXTRAORDINARIA,
                "periodo": "2026-09",
                "justificacion_extraordinaria": "Urgente",
                "descripcion": ["Rack"],
                "cantidad": ["1"],
                "unidad": ["pieza"],
            },
        )
        self.assertEqual(response.status_code, 403)

    def test_bandeja_compras_es_compartida_y_no_muestra_solicitudes_de_insumos(self):
        solicitud = SolicitudCompraDepartamental.objects.create(
            area=self.area,
            solicitante=self.responsable,
            tipo=SolicitudCompraDepartamental.TIPO_EXTRAORDINARIA,
            periodo=date(2026, 9, 1),
            justificacion_extraordinaria="Necesidad imprevista",
            estado=SolicitudCompraDepartamental.ESTADO_ENVIADA,
        )
        ItemCompraDepartamental.objects.create(solicitud=solicitud, descripcion="Refrigerador", cantidad=1)
        self.client.force_login(self.comprador)

        response = self.client.get(reverse("compras:departamental_bandeja"))

        self.assertContains(response, "Refrigerador")
        self.assertContains(response, "Bandeja de Compras")
        self.assertNotContains(response, "Materia prima")

    def test_seguimiento_muestra_siguiente_responsable_y_no_convierte_diferencia_en_falla(self):
        solicitud = SolicitudCompraDepartamental.objects.create(
            area=self.area,
            solicitante=self.responsable,
            tipo=SolicitudCompraDepartamental.TIPO_EXTRAORDINARIA,
            periodo=date(2026, 9, 1),
            justificacion_extraordinaria="Necesidad imprevista",
        )
        ItemCompraDepartamental.objects.create(
            solicitud=solicitud,
            descripcion="Aire acondicionado",
            cantidad=1,
            estado=ItemCompraDepartamental.ESTADO_ESPERANDO_AREA,
            siguiente_responsable=ItemCompraDepartamental.RESPONSABLE_COMPRAS,
            comentario_reciente="Llegó golpeado; Compras debe resolver con proveedor.",
        )
        self.client.force_login(self.responsable)

        response = self.client.get(reverse("compras:departamental_detalle", args=[solicitud.pk]))

        self.assertContains(response, "Siguiente acción")
        self.assertContains(response, "Compras")
        self.assertContains(response, "Llegó golpeado")
        self.assertNotContains(response, "Crear reporte de falla")

    def test_direccion_ve_formula_presupuestal_de_la_cotizacion_definitiva(self):
        rubro = RubroPresupuesto.objects.create(
            area=self.area, concepto="Equipo", tipo=RubroPresupuesto.TIPO_EGRESO
        )
        LineaPresupuestoMensual.objects.create(
            rubro=rubro,
            periodo=date(2026, 9, 1),
            version=LineaPresupuestoMensual.VERSION_REVISADO,
            monto_presupuesto=Decimal("10000"),
            monto_real=Decimal("2500"),
        )
        solicitud = SolicitudCompraDepartamental.objects.create(
            area=self.area,
            solicitante=self.responsable,
            tipo=SolicitudCompraDepartamental.TIPO_EXTRAORDINARIA,
            periodo=date(2026, 9, 1),
            justificacion_extraordinaria="Imprevisto",
        )
        item = ItemCompraDepartamental.objects.create(
            solicitud=solicitud, descripcion="Impresora de etiquetas", cantidad=1, rubro=rubro
        )
        proveedor = Proveedor.objects.create(nombre="Tecnología del Pacífico")
        cotizacion = CotizacionCompraDepartamental.objects.create(
            item=item, proveedor=proveedor, cantidad_ofertada=1, costo_unitario=Decimal("8000")
        )
        seleccionar_cotizacion(cotizacion, actor=self.comprador)
        self.client.force_login(self.dg)

        response = self.client.get(reverse("compras:departamental_detalle", args=[solicitud.pk]))

        self.assertContains(response, "Disponible proyectado")
        self.assertContains(response, "Exceso a decidir")
        self.assertContains(response, "500.00")

    def test_imagen_del_articulo_es_privada_para_el_area_y_compras(self):
        solicitud = SolicitudCompraDepartamental.objects.create(
            area=self.area,
            solicitante=self.responsable,
            tipo=SolicitudCompraDepartamental.TIPO_EXTRAORDINARIA,
            periodo=date(2026, 9, 1),
            justificacion_extraordinaria="Imprevisto",
        )
        ItemCompraDepartamental.objects.create(
            solicitud=solicitud,
            descripcion="Rack",
            cantidad=1,
            imagen="compras/departamentales/2026/09/rack.jpg",
        )

        self.assertTrue(
            _can_access_operational_media(
                self.responsable, "compras/departamentales/2026/09/rack.jpg"
            )
        )
        self.assertTrue(
            _can_access_operational_media(
                self.comprador, "compras/departamentales/2026/09/rack.jpg"
            )
        )
        self.assertFalse(
            _can_access_operational_media(
                self.ajena, "compras/departamentales/2026/09/rack.jpg"
            )
        )
