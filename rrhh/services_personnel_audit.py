from __future__ import annotations

from collections import Counter, defaultdict
import re
import unicodedata

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group

from core.access import ROLE_ORDER
from core.models import Departamento, Sucursal, UserModuleAccess, UserProfile
from rrhh.models import Empleado


SPECIAL_OPERATIONAL_GROUPS = {
    "bonos_produccion_captura",
    "compras_logistica",
    "mantenimiento",
    "personal_sucursal",
    "repartidor",
    "supervisor_logistica",
}


def normalize_catalog_key(value: object) -> str:
    """Stable comparison key for legacy text fields and canonical catalogs."""
    text = str(value or "").strip()
    text = "".join(
        char
        for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )
    text = re.sub(r"[^A-Za-z0-9]+", "_", text.upper()).strip("_")
    return text


def _catalog_index(model, *fields: str) -> dict[str, object]:
    index: dict[str, object] = {}
    for item in model.objects.all():
        for field in fields:
            key = normalize_catalog_key(getattr(item, field, ""))
            if key:
                index[key] = item
    return index


def build_personnel_identity_audit(*, limit: int = 25) -> dict:
    User = get_user_model()
    limit = max(int(limit or 0), 0)
    role_order = set(ROLE_ORDER)
    module_codes = {code for code, _label in UserModuleAccess.MODULOS}
    sucursales = _catalog_index(Sucursal, "codigo", "nombre")
    departamentos = _catalog_index(Departamento, "codigo", "nombre")

    counts: Counter[str] = Counter()
    findings: dict[str, dict] = {}

    def record(category: str, severity: str, title: str, detail: dict) -> None:
        counts[category] += 1
        bucket = findings.setdefault(
            category,
            {
                "category": category,
                "severity": severity,
                "title": title,
                "count": 0,
                "examples": [],
            },
        )
        bucket["count"] = counts[category]
        if len(bucket["examples"]) < limit:
            bucket["examples"].append(detail)

    groups_by_lower: dict[str, list[str]] = defaultdict(list)
    for group in Group.objects.order_by("name"):
        groups_by_lower[group.name.lower()].append(group.name)
        normalized = normalize_catalog_key(group.name)
        if group.name not in role_order and group.name.upper() in role_order:
            record(
                "group_case_alias",
                "warning",
                "Grupo equivalente a rol canonico con distinta capitalizacion",
                {"group": group.name, "canonical": group.name.upper(), "users": group.user_set.count()},
            )
        elif group.name not in role_order and group.name not in SPECIAL_OPERATIONAL_GROUPS:
            record(
                "group_unknown",
                "warning",
                "Grupo fuera del catalogo de roles y excepciones operativas",
                {"group": group.name, "normalized": normalized, "users": group.user_set.count()},
            )

    for lower_name, names in groups_by_lower.items():
        if len(names) > 1:
            record(
                "group_case_duplicate",
                "risk",
                "Grupos duplicados por mayusculas/minusculas",
                {"key": lower_name, "groups": sorted(names)},
            )

    active_users = (
        User.objects.filter(is_active=True)
        .prefetch_related("groups", "module_access")
        .select_related("userprofile")
        .order_by("username")
    )
    for user in active_users:
        group_names = list(user.groups.values_list("name", flat=True))
        has_employee = hasattr(user, "empleado_rrhh")
        has_profile = hasattr(user, "userprofile")
        has_module_access = user.module_access.exists()
        if not user.is_superuser and not has_employee:
            record(
                "active_user_without_employee",
                "warning",
                "Usuario activo sin empleado vinculado",
                {
                    "username": user.username,
                    "groups": group_names,
                    "has_profile": has_profile,
                    "has_module_access": has_module_access,
                },
            )
        if not user.is_superuser and not has_profile:
            record(
                "active_user_without_profile",
                "warning",
                "Usuario activo sin UserProfile",
                {"username": user.username, "groups": group_names},
            )
        if not user.is_superuser and not group_names and not has_module_access:
            record(
                "active_user_without_access_anchor",
                "risk",
                "Usuario activo sin rol ni acceso explicito",
                {"username": user.username},
            )
        if len(group_names) > 1:
            record(
                "active_user_multiple_groups",
                "warning",
                "Usuario activo con multiples grupos",
                {"username": user.username, "groups": group_names},
            )

    active_employees = (
        Empleado.objects.filter(activo=True)
        .select_related("jefe_directo", "usuario_erp", "usuario_erp__userprofile")
        .order_by("departamento", "sucursal", "nombre")
    )
    for empleado in active_employees:
        if not empleado.usuario_erp_id:
            record(
                "active_employee_without_user",
                "info",
                "Empleado activo sin usuario ERP vinculado",
                _employee_detail(empleado),
            )

        sucursal_key = normalize_catalog_key(empleado.sucursal)
        if not sucursal_key:
            record(
                "active_employee_without_branch_text",
                "warning",
                "Empleado activo sin sucursal en texto legacy",
                _employee_detail(empleado),
            )
        elif sucursal_key not in sucursales:
            record(
                "employee_branch_text_unmapped",
                "risk",
                "Sucursal legacy del empleado no existe en core.Sucursal",
                {**_employee_detail(empleado), "sucursal_key": sucursal_key},
            )

        departamento_key = normalize_catalog_key(empleado.departamento)
        if not departamento_key:
            record(
                "active_employee_without_department",
                "risk",
                "Empleado activo sin departamento",
                _employee_detail(empleado),
            )
        elif departamento_key not in departamentos:
            record(
                "employee_department_unmapped",
                "warning",
                "Departamento del empleado no mapea a core.Departamento",
                {**_employee_detail(empleado), "departamento_key": departamento_key},
            )

        if (
            not empleado.jefe_directo_id
            and empleado.puesto_operativo != "JEFATURA"
            and empleado.nivel_organizacional not in {Empleado.NIVEL_JEFATURA, Empleado.NIVEL_DIRECCION}
        ):
            record(
                "active_employee_without_direct_boss",
                "warning",
                "Empleado activo sin jefe directo y no marcado como jefatura",
                _employee_detail(empleado),
            )

        if empleado.usuario_erp_id:
            profile = getattr(empleado.usuario_erp, "userprofile", None)
            mapped_sucursal = sucursales.get(sucursal_key) if sucursal_key else None
            if not profile:
                record(
                    "linked_employee_user_without_profile",
                    "risk",
                    "Empleado vinculado a usuario sin UserProfile",
                    {**_employee_detail(empleado), "username": empleado.usuario_erp.username},
                )
            elif mapped_sucursal and profile.sucursal_id and mapped_sucursal.id != profile.sucursal_id:
                record(
                    "employee_user_branch_mismatch",
                    "risk",
                    "Sucursal de Empleado y UserProfile no coinciden",
                    {
                        **_employee_detail(empleado),
                        "username": empleado.usuario_erp.username,
                        "employee_branch": empleado.sucursal_display,
                        "profile_branch": profile.sucursal.nombre,
                    },
                )

    for boss in (
        Empleado.objects.filter(activo=True, colaboradores_directos__activo=True)
        .distinct()
        .order_by("departamento", "nombre")
    ):
        if not boss.usuario_erp_id:
            record(
                "authorizer_without_user",
                "risk",
                "Jefe con colaboradores activos sin usuario ERP",
                {
                    **_employee_detail(boss),
                    "active_direct_reports": boss.colaboradores_directos.filter(activo=True).count(),
                },
            )

    for access in UserModuleAccess.objects.select_related("user").order_by("user__username", "module"):
        if access.module not in module_codes:
            record(
                "module_access_unknown_module",
                "risk",
                "Acceso explicito apunta a modulo fuera del catalogo UserModuleAccess.MODULOS",
                {"username": access.user.username, "module": access.module, "access": access.access},
            )

    severity_order = {"risk": 0, "warning": 1, "info": 2}
    ordered_findings = sorted(
        findings.values(),
        key=lambda item: (severity_order.get(item["severity"], 9), item["category"]),
    )
    return {
        "dry_run": True,
        "summary": {
            "users": User.objects.count(),
            "active_users": User.objects.filter(is_active=True).count(),
            "employees": Empleado.objects.count(),
            "active_employees": Empleado.objects.filter(activo=True).count(),
            "profiles": UserProfile.objects.count(),
            "groups": Group.objects.count(),
            "module_access": UserModuleAccess.objects.count(),
            "finding_categories": len(ordered_findings),
            "finding_count": sum(counts.values()),
        },
        "findings": ordered_findings,
    }


def _employee_detail(empleado: Empleado) -> dict:
    return {
        "employee_id": empleado.id,
        "employee": empleado.nombre,
        "departamento": empleado.departamento or "",
        "puesto_operativo": empleado.puesto_operativo or "",
        "nivel_organizacional": empleado.nivel_organizacional or "",
        "sucursal": empleado.sucursal_display,
        "jefe_directo": empleado.jefe_directo.nombre if empleado.jefe_directo_id else "",
    }
