from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass

from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction

from core.models import Departamento, Sucursal, UserProfile
from rrhh.models import Empleado
from rrhh.services_identidad import asegurar_repartidor_logistica, sincronizar_nombre_usuario_desde_empleado
from rrhh.services_personnel_audit import normalize_catalog_key


@dataclass
class IdentityProjectionAction:
    action: str
    employee_id: int
    employee_name: str
    user_id: int
    username: str
    current_value: str
    proposed_value: str
    reason: str
    safe_apply: bool
    applied: bool = False

    def as_dict(self) -> dict:
        return asdict(self)


def build_personnel_identity_projection_plan(
    *,
    apply: bool = False,
    include_repartidores: bool = False,
    limit: int = 200,
) -> dict:
    """
    Proyecta datos seguros desde RRHH hacia usuarios ya vinculados.

    No crea usuarios, no liga empleados por nombre, no cambia passwords, no
    desactiva cuentas y no retira grupos/permisos. Repartidores requiere una
    bandera explicita porque puede afectar el ruteo PWA del usuario.
    """
    limit = max(int(limit or 0), 0)
    actions = _build_actions(include_repartidores=include_repartidores)

    if apply:
        with transaction.atomic():
            _apply_actions(actions)

    visible = actions[:limit] if limit else actions
    by_action = Counter(item.action for item in actions)
    applied = sum(1 for item in actions if item.applied)
    safe_pending = sum(1 for item in actions if item.safe_apply and not item.applied)
    manual_pending = sum(1 for item in actions if not item.safe_apply)
    linked_employees = Empleado.objects.filter(activo=True, usuario_erp__isnull=False).count()
    return {
        "dry_run": not apply,
        "writes": bool(apply),
        "include_repartidores": bool(include_repartidores),
        "guardrails": [
            "no_crea_usuarios",
            "no_liga_empleados_por_nombre",
            "no_cambia_passwords",
            "no_desactiva_usuarios",
            "no_elimina_grupos",
            "no_retira_permisos",
        ],
        "summary": {
            "linked_active_employees": linked_employees,
            "actions": len(actions),
            "shown": len(visible),
            "applied": applied,
            "safe_pending": safe_pending,
            "manual_pending": manual_pending,
            "by_action": dict(sorted(by_action.items())),
        },
        "actions": [item.as_dict() for item in visible],
    }


def _build_actions(*, include_repartidores: bool) -> list[IdentityProjectionAction]:
    actions: list[IdentityProjectionAction] = []
    empleados = (
        Empleado.objects.filter(activo=True, usuario_erp__isnull=False)
        .select_related("usuario_erp")
        .order_by("nombre", "id")
    )
    for empleado in empleados:
        user = empleado.usuario_erp
        if empleado.nombre and not user.get_full_name():
            actions.append(
                IdentityProjectionAction(
                    action="sincronizar_nombre_usuario_desde_empleado",
                    employee_id=empleado.id,
                    employee_name=empleado.nombre,
                    user_id=user.id,
                    username=user.username,
                    current_value="User.get_full_name()=(vacio)",
                    proposed_value=f"first_name={empleado.nombre.strip()}, last_name=(vacio)",
                    reason="Completa nombre visible desde RRHH sin cambiar username, password, estado, grupos ni permisos.",
                    safe_apply=True,
                )
            )

        profile = _user_profile(user)
        if profile is None:
            departamento = _resolve_departamento(empleado)
            sucursal = _resolve_sucursal(empleado)
            actions.append(
                IdentityProjectionAction(
                    action="crear_userprofile_desde_empleado_vinculado",
                    employee_id=empleado.id,
                    employee_name=empleado.nombre,
                    user_id=user.id,
                    username=user.username,
                    current_value="Sin UserProfile",
                    proposed_value=(
                        f"departamento={getattr(departamento, 'codigo', '') or 'sin_departamento'}, "
                        f"sucursal={getattr(sucursal, 'nombre', '') or 'sin_sucursal'}"
                    ),
                    reason="Crea el perfil operativo faltante para un usuario ya vinculado; no modifica accesos existentes.",
                    safe_apply=True,
                )
            )

        if (empleado.puesto_operativo or "").strip().upper() == "REPARTIDOR" and _repartidor_logistica(user) is None:
            sucursal = _resolve_sucursal(empleado)
            if sucursal:
                actions.append(
                    IdentityProjectionAction(
                        action="crear_repartidor_logistica_desde_empleado_vinculado",
                        employee_id=empleado.id,
                        employee_name=empleado.nombre,
                        user_id=user.id,
                        username=user.username,
                        current_value="Sin logistica.Repartidor",
                        proposed_value=f"Repartidor.sucursal={sucursal.nombre}; asegurar grupo repartidor",
                        reason="Solo se aplica con --include-repartidores porque puede afectar el ruteo PWA.",
                        safe_apply=include_repartidores,
                    )
                )
            else:
                actions.append(
                    IdentityProjectionAction(
                        action="resolver_sucursal_repartidor_antes_de_proyectar",
                        employee_id=empleado.id,
                        employee_name=empleado.nombre,
                        user_id=user.id,
                        username=user.username,
                        current_value=f"sucursal={empleado.sucursal or '(vacio)'}",
                        proposed_value="Capturar o normalizar sucursal del empleado antes de crear Repartidor",
                        reason="No se crea Repartidor si la sucursal no mapea a core.Sucursal.",
                        safe_apply=False,
                    )
                )
    return actions


def _apply_actions(actions: list[IdentityProjectionAction]) -> None:
    for action in actions:
        if not action.safe_apply:
            continue
        empleado = Empleado.objects.select_related("usuario_erp").get(pk=action.employee_id)
        if not empleado.usuario_erp_id:
            continue

        if action.action == "sincronizar_nombre_usuario_desde_empleado":
            action.applied = sincronizar_nombre_usuario_desde_empleado(empleado)
        elif action.action == "crear_userprofile_desde_empleado_vinculado":
            profile = _user_profile(empleado.usuario_erp)
            if profile is None:
                UserProfile.objects.create(
                    user=empleado.usuario_erp,
                    departamento=_resolve_departamento(empleado),
                    sucursal=_resolve_sucursal(empleado),
                    telefono=empleado.telefono or "",
                )
                action.applied = True
        elif action.action == "crear_repartidor_logistica_desde_empleado_vinculado":
            action.applied = asegurar_repartidor_logistica(empleado, sucursal=_resolve_sucursal(empleado))


def _user_profile(user) -> UserProfile | None:
    try:
        return user.userprofile
    except ObjectDoesNotExist:
        return None


def _repartidor_logistica(user):
    try:
        return user.repartidor_logistica
    except ObjectDoesNotExist:
        return None


def _resolve_departamento(empleado: Empleado) -> Departamento | None:
    key = normalize_catalog_key(empleado.departamento)
    if not key:
        return None
    for departamento in Departamento.objects.all():
        if normalize_catalog_key(departamento.codigo) == key or normalize_catalog_key(departamento.nombre) == key:
            return departamento
    return None


def _resolve_sucursal(empleado: Empleado) -> Sucursal | None:
    key = normalize_catalog_key(empleado.sucursal)
    if not key:
        return None
    for sucursal in Sucursal.objects.filter(activa=True):
        if normalize_catalog_key(sucursal.codigo) == key or normalize_catalog_key(sucursal.nombre) == key:
            return sucursal
    return None
