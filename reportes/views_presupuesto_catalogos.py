from __future__ import annotations

from datetime import date
from urllib.parse import unquote, urlsplit

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.http import url_has_allowed_host_and_scheme

from core.models import Sucursal
from reportes.models import AreaPresupuesto, AreaPresupuestoResponsable, CategoriaGasto, RubroPresupuesto
from reportes.services_presupuesto_catalogos import (
    CATEGORY_STATUS_ACTIVE,
    CATEGORY_STATUS_CHOICES,
    SOURCE_MANUAL,
    RECORD_STATUS_ACTIVE,
    RECORD_STATUS_CHOICES,
    SOURCE_STATE_LABELS,
    SOURCE_UNCONFIGURED,
    PresupuestoCatalogoService,
)
from reportes.services_presupuesto_maestro import MONTH_COLUMNS, ensure_master_budget_areas, normalize_version


CATALOG_MANAGER_AREAS = {"administracion", "compras"}


def puede_ver_catalogos_presupuesto(user) -> bool:
    if not (user and user.is_authenticated and getattr(user, "pk", None)):
        return False
    if user.is_superuser:
        return True
    return AreaPresupuestoResponsable.objects.filter(
        usuario=user,
        puede_capturar=True,
        area__activa=True,
    ).exists()


def puede_gestionar_catalogos_presupuesto(user) -> bool:
    if not puede_ver_catalogos_presupuesto(user):
        return False
    if user.is_superuser:
        return True
    return AreaPresupuestoResponsable.objects.filter(
        usuario=user,
        puede_capturar=True,
        area__activa=True,
        area__codigo__in=CATALOG_MANAGER_AREAS,
    ).exists()


def areas_gestionables_catalogos_presupuesto(user):
    if not (user and user.is_authenticated and getattr(user, "pk", None)):
        return AreaPresupuesto.objects.none()
    if user.is_superuser:
        return AreaPresupuesto.objects.filter(activa=True).order_by("orden", "nombre")
    return AreaPresupuesto.objects.filter(
        activa=True,
        codigo__in=CATALOG_MANAGER_AREAS,
        responsables__usuario=user,
        responsables__puede_capturar=True,
    ).distinct().order_by("orden", "nombre")


def _as_int(value, default=0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _wants_json(request: HttpRequest) -> bool:
    return (
        "application/json" in request.headers.get("Accept", "")
        or request.headers.get("X-Requested-With") == "XMLHttpRequest"
    )


def _safe_return_to(request: HttpRequest) -> str:
    fallback = reverse("reportes:presupuesto_catalogos")
    target = str(request.POST.get("return_to") or fallback)
    decoded_target = unquote(target)
    if "\\" in decoded_target or any(ord(char) < 32 for char in decoded_target):
        return fallback
    if not url_has_allowed_host_and_scheme(
        target,
        allowed_hosts={request.get_host()},
        require_https=request.is_secure(),
    ):
        return fallback
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc or parsed.path != fallback:
        return fallback
    return target.split("#", 1)[0]


def _action_response(request: HttpRequest, *, ok: bool, message: str, status: int = 200) -> HttpResponse:
    if _wants_json(request):
        payload = {
            "ok": ok,
            "toast": {
                "type": "success" if ok else "error",
                "message": message,
                "persistent": not ok,
            },
        }
        if ok:
            payload.update(
                {
                    "redirect": f"{_safe_return_to(request)}#catalog-actions",
                    "reload": True,
                }
            )
        return JsonResponse(payload, status=status)
    (messages.success if ok else messages.error)(request, message)
    return redirect(f"{_safe_return_to(request)}#catalog-actions")


@login_required
def presupuesto_catalogos(request: HttpRequest) -> HttpResponse:
    if not puede_ver_catalogos_presupuesto(request.user):
        raise PermissionDenied("No tienes una responsabilidad activa de presupuesto.")
    can_manage = puede_gestionar_catalogos_presupuesto(request.user)
    manageable_areas = areas_gestionables_catalogos_presupuesto(request.user)
    service = PresupuestoCatalogoService()

    if request.method == "POST":
        if not can_manage:
            raise PermissionDenied("La gestión del catálogo corresponde a Administración y Compras.")
        action = (request.POST.get("action") or "").strip()
        if action == "create_rubro":
            requested_area = (request.POST.get("area") or "").strip()
            if not manageable_areas.filter(codigo=requested_area).exists():
                raise PermissionDenied(
                    "Solo puedes crear rubros en tus responsabilidades activas de Administración y Compras."
                )
        try:
            if action == "create_category":
                category = service.create_category(
                    user=request.user,
                    codigo=request.POST.get("codigo") or "",
                    nombre=request.POST.get("nombre") or "",
                    capa_objetivo=request.POST.get("capa_objetivo") or "",
                    bucket=request.POST.get("bucket") or "",
                )
                return _action_response(
                    request,
                    ok=True,
                    message=f"Categoría {category.nombre} creada y disponible para nuevas altas.",
                )
            if action == "create_rubro":
                result = service.create_rubro(
                    user=request.user,
                    area_code=request.POST.get("area") or "",
                    concepto=request.POST.get("concepto") or "",
                    tipo=request.POST.get("tipo") or "",
                    year=max(2020, min(_as_int(request.POST.get("year"), timezone.localdate().year), 2035)),
                    version=normalize_version(request.POST.get("version")),
                    codigo_cuenta=request.POST.get("codigo_cuenta") or "",
                    sucursal_id=_as_int(request.POST.get("sucursal_id")) or None,
                    categoria_id=_as_int(request.POST.get("categoria_id")) or None,
                    fuente_mode=request.POST.get("fuente_mode") or SOURCE_MANUAL,
                )
                return _action_response(
                    request,
                    ok=True,
                    message=f"Rubro {result.rubro.concepto} creado con 12 meses listos para presupuesto.",
                )
            raise ValueError("Selecciona una acción válida del catálogo.")
        except ValueError as exc:
            return _action_response(request, ok=False, message=str(exc), status=400)

    ensure_master_budget_areas()
    today = timezone.localdate()
    year = max(2020, min(_as_int(request.GET.get("year"), today.year), 2035))
    month = max(1, min(_as_int(request.GET.get("month"), today.month), 12))
    version = normalize_version(request.GET.get("version"))
    area_code = (request.GET.get("area") or "").strip()
    category_id = _as_int(request.GET.get("category")) or None
    source_state = (request.GET.get("source_state") or "").strip()
    record_status = (request.GET.get("status") or RECORD_STATUS_ACTIVE).strip().upper()
    if record_status not in dict(RECORD_STATUS_CHOICES):
        record_status = RECORD_STATUS_ACTIVE
    category_status = (
        request.GET.get("category_status") or CATEGORY_STATUS_ACTIVE
    ).strip().upper()
    if category_status not in dict(CATEGORY_STATUS_CHOICES):
        category_status = CATEGORY_STATUS_ACTIVE
    query = (request.GET.get("q") or "").strip()
    rows = service.list_rows(
        period=date(year, month, 1),
        version=version,
        area_code=area_code,
        category_id=category_id,
        source_state=source_state,
        query=query,
        record_status=record_status,
    )
    assigned_areas = list(
        AreaPresupuestoResponsable.objects.filter(
            usuario=request.user, puede_capturar=True, area__activa=True
        ).select_related("area").order_by("area__orden", "area__nombre")
    )
    state_counts = {key: 0 for key in SOURCE_STATE_LABELS}
    for row in rows:
        state_counts[row["source_state"]] += 1
    catalog_categories = service.list_categories(category_status)
    active_categories = CategoriaGasto.objects.filter(activo=True).order_by("nombre", "codigo")
    context = {
        "catalog_rows": rows,
        "areas": AreaPresupuesto.objects.filter(activa=True).order_by("orden", "nombre"),
        "manageable_areas": manageable_areas,
        "assigned_areas": assigned_areas,
        "catalog_categories": catalog_categories,
        "active_categories": active_categories,
        "branches": Sucursal.objects.filter(activa=True).order_by("nombre"),
        "rubro_types": RubroPresupuesto.TIPO_CHOICES,
        "category_layers": CategoriaGasto.CAPA_CHOICES,
        "category_buckets": CategoriaGasto.BUCKET_CHOICES,
        "source_states": SOURCE_STATE_LABELS.items(),
        "record_statuses": RECORD_STATUS_CHOICES,
        "category_statuses": CATEGORY_STATUS_CHOICES,
        "source_modes": (
            (SOURCE_MANUAL, "Manual · el área captura el real"),
            (SOURCE_UNCONFIGURED, "Sin configurar · requiere revisión técnica"),
        ),
        "state_counts": state_counts,
        "can_manage_catalog": can_manage,
        "selected_year": year,
        "selected_month": month,
        "selected_version": version,
        "selected_area": area_code,
        "selected_category": category_id,
        "selected_source_state": source_state,
        "selected_record_status": record_status,
        "selected_category_status": category_status,
        "search_query": query,
        "month_options": tuple((number, name) for name, number in MONTH_COLUMNS),
        "versions": ("ORIGINAL", "REVISADO"),
    }
    return render(request, "reportes/presupuesto_catalogos.html", context)
