from __future__ import annotations

from decimal import Decimal

from django.db.models import Count, F
from django.utils.dateparse import parse_date
from django.utils import timezone
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from crm.models import Cliente, PedidoCliente
from integraciones.models import PublicApiAccessLog, PublicApiClient
from inventario.models import ExistenciaInsumo
from maestros.models import CostoInsumo, Insumo
from recetas.models import LineaReceta, Receta
from recetas.utils.normalizacion import normalizar_nombre


def _bounded_int(value, *, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(min_value, min(parsed, max_value))


def _to_decimal(value, *, default: Decimal = Decimal("0")) -> Decimal:
    if value in (None, ""):
        return default
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _auth_public_client(request):
    raw_key = (request.headers.get("X-API-Key") or "").strip()
    if not raw_key:
        return None, Response({"detail": "X-API-Key es obligatorio"}, status=status.HTTP_401_UNAUTHORIZED)

    prefix = raw_key[:12]
    client = PublicApiClient.objects.filter(clave_prefijo=prefix, activo=True).first()
    if not client or not client.validate(raw_key):
        return None, Response({"detail": "API key inválida"}, status=status.HTTP_401_UNAUTHORIZED)

    client.mark_used()
    return client, None


def _log_access(client: PublicApiClient, request, status_code: int):
    PublicApiAccessLog.objects.create(
        client=client,
        endpoint=request.path,
        method=request.method,
        status_code=int(status_code),
    )


class PublicHealthView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        return Response(
            {
                "status": "ok",
                "service": "public-api",
                "version": "v1",
                "timestamp": timezone.now().isoformat(),
            },
            status=status.HTTP_200_OK,
        )


class PublicInsumosView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        client, error = _auth_public_client(request)
        if error:
            return error

        limit = _bounded_int(request.query_params.get("limit"), default=100, min_value=1, max_value=1000)
        offset = _bounded_int(request.query_params.get("offset"), default=0, min_value=0, max_value=100000)
        q = (request.query_params.get("q") or "").strip()

        qs = Insumo.objects.filter(activo=True).select_related("unidad_base")
        if q:
            qs = qs.filter(nombre__icontains=q)

        total = qs.count()
        rows = list(qs.order_by("nombre", "id")[offset : offset + limit])
        insumo_ids = [row.id for row in rows]

        latest_cost_by_insumo = {}
        for cost in CostoInsumo.objects.filter(insumo_id__in=insumo_ids).order_by("insumo_id", "-fecha", "-id"):
            if cost.insumo_id not in latest_cost_by_insumo:
                latest_cost_by_insumo[cost.insumo_id] = cost

        existencias_by_insumo = {
            row.insumo_id: row
            for row in ExistenciaInsumo.objects.filter(insumo_id__in=insumo_ids)
        }

        data = []
        for insumo in rows:
            costo = latest_cost_by_insumo.get(insumo.id)
            ex = existencias_by_insumo.get(insumo.id)
            data.append(
                {
                    "id": insumo.id,
                    "nombre": insumo.nombre,
                    "categoria": insumo.categoria,
                    "unidad": insumo.unidad_base.codigo if insumo.unidad_base else "",
                    "stock_actual": str(ex.stock_actual) if ex else "0",
                    "punto_reorden": str(ex.punto_reorden) if ex else "0",
                    "costo_unitario": str(costo.costo_unitario) if costo else "0",
                    "costo_fecha": costo.fecha.isoformat() if costo else None,
                }
            )

        payload = {
            "count": total,
            "limit": limit,
            "offset": offset,
            "results": data,
        }
        _log_access(client, request, status.HTTP_200_OK)
        return Response(payload, status=status.HTTP_200_OK)


class PublicRecetasView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        client, error = _auth_public_client(request)
        if error:
            return error

        limit = _bounded_int(request.query_params.get("limit"), default=100, min_value=1, max_value=1000)
        offset = _bounded_int(request.query_params.get("offset"), default=0, min_value=0, max_value=100000)
        q = (request.query_params.get("q") or "").strip()

        qs = Receta.objects.all()
        if q:
            qs = qs.filter(nombre__icontains=q)

        total = qs.count()
        rows = list(qs.order_by("nombre", "id")[offset : offset + limit])

        line_counts = {
            row["receta_id"]: row["total"]
            for row in LineaReceta.objects.filter(receta_id__in=[r.id for r in rows])
            .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
            .values("receta_id")
            .annotate(total=Count("id"))
        }

        payload = {
            "count": total,
            "limit": limit,
            "offset": offset,
            "results": [
                {
                    "id": r.id,
                    "nombre": r.nombre,
                    "codigo_point": r.codigo_point,
                    "tipo_producto": r.tipo,
                    "rendimiento": str(r.rendimiento_cantidad or Decimal("0")),
                    "unidad_rendimiento": r.rendimiento_unidad.codigo if r.rendimiento_unidad else "",
                    "costo_por_kg_estimado": str(r.costo_por_kg_estimado or Decimal("0")),
                    "lineas": int(line_counts.get(r.id, 0)),
                }
                for r in rows
            ],
        }
        _log_access(client, request, status.HTTP_200_OK)
        return Response(payload, status=status.HTTP_200_OK)


class PublicResumenView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        client, error = _auth_public_client(request)
        if error:
            return error

        payload = {
            "insumos_activos": Insumo.objects.filter(activo=True).count(),
            "recetas_activas": Receta.objects.count(),
            "alertas_stock": ExistenciaInsumo.objects.filter(stock_actual__lt=F("punto_reorden")).count(),
            "stock_critico": ExistenciaInsumo.objects.filter(stock_actual__lte=0).count(),
            "timestamp": timezone.now().isoformat(),
        }
        _log_access(client, request, status.HTTP_200_OK)
        return Response(payload, status=status.HTTP_200_OK)


class PublicPedidosCreateView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        client, error = _auth_public_client(request)
        if error:
            return error

        cliente_nombre = (request.data.get("cliente_nombre") or "").strip()
        descripcion = (request.data.get("descripcion") or "").strip()
        if not cliente_nombre or not descripcion:
            return Response(
                {"detail": "cliente_nombre y descripcion son obligatorios"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        prioridad = (request.data.get("prioridad") or PedidoCliente.PRIORIDAD_MEDIA).strip()
        if prioridad not in {
            PedidoCliente.PRIORIDAD_BAJA,
            PedidoCliente.PRIORIDAD_MEDIA,
            PedidoCliente.PRIORIDAD_ALTA,
            PedidoCliente.PRIORIDAD_URGENTE,
        }:
            prioridad = PedidoCliente.PRIORIDAD_MEDIA

        fecha_compromiso_raw = request.data.get("fecha_compromiso")
        fecha_compromiso = None
        if fecha_compromiso_raw not in (None, ""):
            fecha_compromiso = parse_date(str(fecha_compromiso_raw))
            if fecha_compromiso is None:
                return Response(
                    {"detail": "fecha_compromiso inválida. Usa formato YYYY-MM-DD"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        cliente = Cliente.objects.filter(nombre_normalizado=normalizar_nombre(cliente_nombre)).first()
        if not cliente:
            cliente = Cliente.objects.create(nombre=cliente_nombre)

        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            descripcion=descripcion,
            sucursal=(request.data.get("sucursal") or "").strip(),
            canal=PedidoCliente.CANAL_WEB,
            prioridad=prioridad,
            estatus=PedidoCliente.ESTATUS_NUEVO,
            monto_estimado=_to_decimal(request.data.get("monto_estimado")),
            fecha_compromiso=fecha_compromiso,
        )

        payload = {
            "id": pedido.id,
            "folio": pedido.folio,
            "cliente": pedido.cliente.nombre,
            "estatus": pedido.estatus,
            "fecha_compromiso": pedido.fecha_compromiso.isoformat() if pedido.fecha_compromiso else None,
        }
        _log_access(client, request, status.HTTP_201_CREATED)
        return Response(payload, status=status.HTTP_201_CREATED)
