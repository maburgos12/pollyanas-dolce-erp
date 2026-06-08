from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from typing import Iterable

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Count, Q

from core.access import ROLE_ORDER
from core.models import Departamento, Sucursal, UserModuleAccess, UserProfile
from rrhh.models import Empleado
from rrhh.services_catalogos import AREA_DIVISION_MAP, AREA_DIVISION_VALUES, PUESTO_OPERATIVO_VALUES
from rrhh.services_personnel_audit import SPECIAL_OPERATIONAL_GROUPS, normalize_catalog_key


SEVERITY_ORDER = {"risk": 0, "warning": 1, "info": 2}
MANUAL_ONLY = False
AUTHORIZED_TECHNICAL_USERNAMES = frozenset(
    {
        "ad_agent_service",
        "omnichannel_service",
    }
)


@dataclass(frozen=True)
class NormalizationProposal:
    key: str
    plane: str
    entity_type: str
    entity_id: str
    display: str
    current_value: str
    proposed_value: str
    action: str
    severity: str
    auto_apply: bool
    reason: str

    def as_dict(self) -> dict:
        return asdict(self)


def build_personnel_normalization_plan(*, limit: int = 200) -> dict:
    """
    Construye una lista dry-run de normalizacion de personal.

    Este reporte no aplica cambios. Su objetivo es separar decisiones seguras
    de decisiones que requieren validacion operativa antes de una migracion.
    """
    limit = max(int(limit or 0), 0)
    proposals: list[NormalizationProposal] = []
    proposals.extend(_group_proposals())
    proposals.extend(_employee_catalog_proposals())
    proposals.extend(_department_catalog_proposals())
    proposals.extend(_user_access_proposals())
    proposals.extend(_repartidor_proposals())

    proposals = _deduplicate(proposals)
    proposals.sort(
        key=lambda item: (
            SEVERITY_ORDER.get(item.severity, 9),
            item.plane,
            item.action,
            item.display,
            item.key,
        )
    )
    visible = proposals[:limit] if limit else proposals
    by_action = Counter(item.action for item in proposals)
    by_severity = Counter(item.severity for item in proposals)
    return {
        "dry_run": True,
        "writes": False,
        "summary": {
            "users": get_user_model().objects.count(),
            "active_users": get_user_model().objects.filter(is_active=True).count(),
            "employees": Empleado.objects.count(),
            "active_employees": Empleado.objects.filter(activo=True).count(),
            "proposals": len(proposals),
            "shown": len(visible),
            "by_severity": dict(sorted(by_severity.items())),
            "by_action": dict(sorted(by_action.items())),
        },
        "proposals": [item.as_dict() for item in visible],
    }


def _proposal(
    *,
    key: str,
    plane: str,
    entity_type: str,
    entity_id: object,
    display: str,
    current_value: object,
    proposed_value: object,
    action: str,
    severity: str,
    reason: str,
    auto_apply: bool = MANUAL_ONLY,
) -> NormalizationProposal:
    return NormalizationProposal(
        key=key,
        plane=plane,
        entity_type=entity_type,
        entity_id=str(entity_id or ""),
        display=str(display or ""),
        current_value=_stringify(current_value),
        proposed_value=_stringify(proposed_value),
        action=action,
        severity=severity,
        auto_apply=bool(auto_apply),
        reason=str(reason or ""),
    )


def _stringify(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set, frozenset)):
        return ", ".join(str(item) for item in value)
    return str(value)


def _deduplicate(proposals: Iterable[NormalizationProposal]) -> list[NormalizationProposal]:
    dedup: dict[str, NormalizationProposal] = {}
    for item in proposals:
        dedup.setdefault(item.key, item)
    return list(dedup.values())


def _group_proposals() -> list[NormalizationProposal]:
    role_order = set(ROLE_ORDER)
    proposals: list[NormalizationProposal] = []
    groups_by_lower: dict[str, list[Group]] = defaultdict(list)
    for group in Group.objects.annotate(user_count=Count("user")).order_by("name"):
        groups_by_lower[group.name.lower()].append(group)

    for lower_name, groups in groups_by_lower.items():
        if len(groups) < 2:
            continue
        canonical = _canonical_group_name([group.name for group in groups], role_order)
        users = {
            group.name: list(group.user_set.order_by("username").values_list("username", flat=True))
            for group in groups
        }
        proposals.append(
            _proposal(
                key=f"group.case_duplicate:{lower_name}",
                plane="accesos",
                entity_type="Group",
                entity_id=lower_name,
                display=lower_name,
                current_value=sorted(group.name for group in groups),
                proposed_value=canonical,
                action="revisar_fusion_grupo_mayusculas",
                severity="risk",
                reason=f"Mover usuarios al grupo canonico y eliminar alias solo despues de validar accesos: {users}",
            )
        )

    for group in Group.objects.annotate(user_count=Count("user")).order_by("name"):
        if group.name in role_order or group.name in SPECIAL_OPERATIONAL_GROUPS:
            continue
        upper = group.name.upper()
        if upper in role_order:
            proposals.append(
                _proposal(
                    key=f"group.case_alias:{group.pk}",
                    plane="accesos",
                    entity_type="Group",
                    entity_id=group.pk,
                    display=group.name,
                    current_value=group.name,
                    proposed_value=upper,
                    action="revisar_alias_grupo_canonico",
                    severity="warning",
                    reason=f"El grupo equivale a {upper}, pero la capitalizacion cambia reglas y filtros.",
                )
            )
        else:
            proposals.append(
                _proposal(
                    key=f"group.unknown:{group.pk}",
                    plane="accesos",
                    entity_type="Group",
                    entity_id=group.pk,
                    display=group.name,
                    current_value=group.name,
                    proposed_value="Agregar a catalogo de excepciones o reemplazar por rol oficial",
                    action="clasificar_grupo_no_catalogado",
                    severity="warning",
                    reason="Grupo fuera del catalogo de roles oficiales y excepciones operativas conocidas.",
                )
            )
    return proposals


def _canonical_group_name(names: list[str], role_order: set[str]) -> str:
    for name in names:
        if name in role_order:
            return name
    for name in names:
        upper = name.upper()
        if upper in role_order:
            return upper
    return sorted(names)[0]


def _employee_catalog_proposals() -> list[NormalizationProposal]:
    proposals: list[NormalizationProposal] = []
    sucursales = _catalog_index(Sucursal, "codigo", "nombre")
    area_by_puesto = {
        data["puesto_operativo"]: area
        for area, data in AREA_DIVISION_MAP.items()
        if data.get("puesto_operativo")
    }
    employees = (
        Empleado.objects.filter(activo=True)
        .select_related("jefe_directo", "usuario_erp")
        .prefetch_related("colaboradores_directos")
        .order_by("nombre", "id")
    )
    for empleado in employees:
        suggested_level = _suggest_level(empleado)
        if suggested_level and empleado.nivel_organizacional != suggested_level:
            proposals.append(
                _proposal(
                    key=f"employee.level:{empleado.pk}",
                    plane="personal",
                    entity_type="Empleado",
                    entity_id=empleado.pk,
                    display=empleado.nombre,
                    current_value=empleado.nivel_organizacional or "(vacio)",
                    proposed_value=suggested_level,
                    action="definir_nivel_organizacional",
                    severity="warning" if suggested_level != Empleado.NIVEL_COLABORADOR else "info",
                    reason="Nivel separado de puesto operativo para evitar mezclar jefe/supervisor con funcion diaria.",
                )
            )

        if empleado.puesto_operativo == "JEFATURA":
            proposals.append(
                _proposal(
                    key=f"employee.role_as_position:{empleado.pk}",
                    plane="personal",
                    entity_type="Empleado",
                    entity_id=empleado.pk,
                    display=empleado.nombre,
                    current_value="puesto_operativo=JEFATURA",
                    proposed_value="nivel_organizacional=JEFATURA y puesto operativo real",
                    action="separar_jefatura_de_puesto_operativo",
                    severity="warning",
                    reason="Jefatura ya no debe vivir como puesto operativo; debe vivir en nivel organizacional.",
                )
            )

        if empleado.area in AREA_DIVISION_MAP:
            expected_puesto = AREA_DIVISION_MAP[empleado.area]["puesto_operativo"]
            if expected_puesto and not empleado.puesto_operativo:
                proposals.append(
                    _proposal(
                        key=f"employee.area_to_position:{empleado.pk}",
                        plane="personal",
                        entity_type="Empleado",
                        entity_id=empleado.pk,
                        display=empleado.nombre,
                        current_value=f"area={empleado.area}, puesto_operativo=(vacio)",
                        proposed_value=f"puesto_operativo={expected_puesto}",
                        action="alinear_area_y_puesto_operativo",
                        severity="warning",
                        reason="El area oficial ya define un puesto operativo base.",
                    )
                )

        if empleado.puesto_operativo in area_by_puesto:
            expected_area = area_by_puesto[empleado.puesto_operativo]
            if empleado.area != expected_area:
                proposals.append(
                    _proposal(
                        key=f"employee.position_to_area:{empleado.pk}",
                        plane="personal",
                        entity_type="Empleado",
                        entity_id=empleado.pk,
                        display=empleado.nombre,
                        current_value=f"area={empleado.area or '(vacio)'}, puesto_operativo={empleado.puesto_operativo}",
                        proposed_value=f"area={expected_area}",
                        action="alinear_puesto_operativo_y_area",
                        severity="warning",
                        reason="El puesto operativo oficial apunta a otra area/division canonica.",
                    )
                )
        elif empleado.puesto_operativo and empleado.puesto_operativo not in PUESTO_OPERATIVO_VALUES:
            proposals.append(
                _proposal(
                    key=f"employee.unknown_position:{empleado.pk}",
                    plane="personal",
                    entity_type="Empleado",
                    entity_id=empleado.pk,
                    display=empleado.nombre,
                    current_value=empleado.puesto_operativo,
                    proposed_value="Seleccionar puesto operativo oficial o mover a nivel organizacional",
                    action="revisar_puesto_operativo_no_catalogado",
                    severity="warning",
                    reason="Valor activo fuera del catalogo cerrado usado por altas nuevas.",
                )
            )

        if (
            normalize_catalog_key(empleado.area) == "PRODUCCION"
            and empleado.puesto_operativo in {"", "PRODUCCION"}
            and not _is_production_leadership_exception(empleado)
        ):
            proposals.append(
                _proposal(
                    key=f"employee.production_embetunado:{empleado.pk}",
                    plane="personal",
                    entity_type="Empleado",
                    entity_id=empleado.pk,
                    display=empleado.nombre,
                    current_value=f"area={empleado.area or '(vacio)'}, puesto_operativo={empleado.puesto_operativo or '(vacio)'}",
                    proposed_value="Confirmar si area/division debe ser EMBETUNADO",
                    action="validar_produccion_vs_embetunado",
                    severity="warning",
                    reason="Produccion ya no debe usarse como funcion especifica si la persona trabaja en embetunado.",
                )
            )

        sucursal_key = normalize_catalog_key(empleado.sucursal)
        if sucursal_key and sucursal_key not in sucursales:
            proposals.append(
                _proposal(
                    key=f"employee.branch_unmapped:{empleado.pk}",
                    plane="personal",
                    entity_type="Empleado",
                    entity_id=empleado.pk,
                    display=empleado.nombre,
                    current_value=empleado.sucursal,
                    proposed_value="Mapear a sucursal real o limpiar si no aplica",
                    action="resolver_sucursal_legacy_no_mapeada",
                    severity="risk",
                    reason="Filtros por sucursal no pueden operar con texto que no existe en core.Sucursal.",
                )
            )
        elif not sucursal_key and _requires_branch(empleado):
            proposals.append(
                _proposal(
                    key=f"employee.branch_required:{empleado.pk}",
                    plane="personal",
                    entity_type="Empleado",
                    entity_id=empleado.pk,
                    display=empleado.nombre,
                    current_value="sucursal=(vacio)",
                    proposed_value="Capturar sucursal operativa",
                    action="capturar_sucursal_requerida",
                    severity="warning",
                    reason="Ventas, repartidores o bonos ventas dependen de sucursal para filtros y permisos.",
                )
            )
    return proposals


def _department_catalog_proposals() -> list[NormalizationProposal]:
    proposals: list[NormalizationProposal] = []
    existing = {normalize_catalog_key(item.codigo) for item in Departamento.objects.all()}
    active_departments = {
        empleado["departamento"]
        for empleado in Empleado.objects.filter(activo=True)
        .exclude(departamento="")
        .values("departamento")
    }
    label_by_code = dict(Empleado.DEP_CHOICES)
    for code in sorted(active_departments):
        key = normalize_catalog_key(code)
        if key and key not in existing:
            proposals.append(
                _proposal(
                    key=f"department.catalog:{key}",
                    plane="catalogos",
                    entity_type="Departamento",
                    entity_id=code,
                    display=code,
                    current_value="No existe en core.Departamento",
                    proposed_value=f"Crear {code} - {label_by_code.get(code, code.title())}",
                    action="crear_departamento_core_faltante",
                    severity="warning",
                    reason="Empleado usa un departamento oficial que no existe en el catalogo core usado por perfiles.",
                )
            )
    return proposals


def _user_access_proposals() -> list[NormalizationProposal]:
    User = get_user_model()
    proposals: list[NormalizationProposal] = []
    module_codes = {code for code, _label in UserModuleAccess.MODULOS}
    sucursales = _catalog_index(Sucursal, "codigo", "nombre")
    departamentos = _catalog_index(Departamento, "codigo", "nombre")
    users = (
        User.objects.filter(is_active=True)
        .select_related("userprofile")
        .prefetch_related("groups", "module_access")
        .order_by("username")
    )
    for user in users:
        if user.is_superuser:
            continue
        employee = getattr(user, "empleado_rrhh", None)
        profile = getattr(user, "userprofile", None)
        group_names = sorted(user.groups.values_list("name", flat=True))
        has_access = user.module_access.exists()
        external_repartidor = _external_logistics_repartidor(user)
        technical_account = _authorized_technical_account(user.username)
        occasional_driver = _occasional_logistics_driver(user)

        if technical_account and not employee:
            proposals.append(
                _proposal(
                    key=f"user.technical_authorized:{user.pk}",
                    plane="usuarios",
                    entity_type="User",
                    entity_id=user.pk,
                    display=user.username,
                    current_value=f"cuenta tecnica; perfil={'si' if profile else 'no'}",
                    proposed_value="Mantener sin rrhh.Empleado; administrar como credencial de servicio",
                    action="cuenta_tecnica_autorizada",
                    severity="info",
                    reason="Cuenta de integracion/IA autorizada; no representa una persona de Capital Humano.",
                )
            )

        if employee and not profile:
            proposed = _profile_suggestion(employee, sucursales, departamentos)
            proposals.append(
                _proposal(
                    key=f"user.profile_from_employee:{user.pk}",
                    plane="usuarios",
                    entity_type="UserProfile",
                    entity_id=user.pk,
                    display=user.username,
                    current_value="Sin UserProfile",
                    proposed_value=proposed,
                    action="crear_perfil_desde_empleado_vinculado",
                    severity="risk",
                    reason="Usuario vinculado a empleado necesita perfil para sucursal/departamento y pantallas de acceso.",
                )
            )
        elif not profile and not external_repartidor and not technical_account:
            action = "clasificar_cuenta_no_personal" if _looks_non_person_user(user.username) else "vincular_usuario_o_crear_perfil"
            proposals.append(
                _proposal(
                    key=f"user.profile_missing:{user.pk}",
                    plane="usuarios",
                    entity_type="User",
                    entity_id=user.pk,
                    display=user.username,
                    current_value=f"empleado={'si' if employee else 'no'}, perfil=no",
                    proposed_value="Definir si es empleado, cuenta de servicio/debug o usuario operativo",
                    action=action,
                    severity="warning",
                    reason="No conviene crear perfiles masivamente sin clasificar primero las cuentas no-persona.",
                )
            )

        if not employee and not external_repartidor and not technical_account:
            severity = "risk" if not group_names and not has_access else "warning"
            proposals.append(
                _proposal(
                    key=f"user.employee_link:{user.pk}",
                    plane="usuarios",
                    entity_type="User",
                    entity_id=user.pk,
                    display=user.username,
                    current_value=f"sin empleado; grupos={group_names}; accesos_explicitos={'si' if has_access else 'no'}",
                    proposed_value="Vincular a empleado o clasificar como cuenta operativa/no-persona",
                    action="clasificar_usuario_sin_empleado",
                    severity=severity,
                    reason="El plano usuario ERP debe separarse del plano persona antes de migrar accesos.",
                )
            )

        alias_groups = [name for name in group_names if name.upper() in ROLE_ORDER and name not in ROLE_ORDER]
        if alias_groups:
            proposals.append(
                _proposal(
                    key=f"user.group_alias:{user.pk}",
                    plane="usuarios",
                    entity_type="User",
                    entity_id=user.pk,
                    display=user.username,
                    current_value=group_names,
                    proposed_value=[name.upper() if name.upper() in ROLE_ORDER else name for name in group_names],
                    action="revisar_grupos_alias_en_usuario",
                    severity="warning",
                    reason="El usuario conserva grupos con distinta capitalizacion que pueden duplicar permisos.",
                )
            )

        if len(group_names) > 1 and not _only_mixes_logistics_access(group_names, occasional_driver):
            proposals.append(
                _proposal(
                    key=f"user.multiple_groups:{user.pk}",
                    plane="usuarios",
                    entity_type="User",
                    entity_id=user.pk,
                    display=user.username,
                    current_value=group_names,
                    proposed_value="Definir rol principal + accesos explicitos si requiere excepciones",
                    action="revisar_usuario_con_multiples_grupos",
                    severity="warning",
                    reason="Multiples grupos mezclan rol base con excepciones y complican autorizaciones.",
                )
            )

        unknown_modules = [
            access.module
            for access in user.module_access.all()
            if access.module not in module_codes
        ]
        if unknown_modules:
            proposals.append(
                _proposal(
                    key=f"user.unknown_modules:{user.pk}",
                    plane="usuarios",
                    entity_type="UserModuleAccess",
                    entity_id=user.pk,
                    display=user.username,
                    current_value=unknown_modules,
                    proposed_value="Reemplazar por modulo oficial o retirar acceso",
                    action="revisar_modulo_no_catalogado",
                    severity="risk",
                    reason="Acceso explicito apunta a modulo que no esta en el catalogo oficial.",
                )
            )
    return proposals


def _repartidor_proposals() -> list[NormalizationProposal]:
    User = get_user_model()
    proposals: list[NormalizationProposal] = []
    repartidores = Empleado.objects.filter(activo=True, puesto_operativo__iexact="REPARTIDOR").order_by("nombre", "id")
    for empleado in repartidores:
        if not empleado.usuario_erp_id:
            proposals.append(
                _proposal(
                    key=f"repartidor.user_required:{empleado.pk}",
                    plane="repartidores",
                    entity_type="Empleado",
                    entity_id=empleado.pk,
                    display=empleado.nombre,
                    current_value="Empleado.usuario_erp=(vacio)",
                    proposed_value="Vincular el usuario real que ya usa en app logistica",
                    action="vincular_usuario_repartidor",
                    severity="risk",
                    reason="Si ya tiene acceso a la PWA, capturar esa cuenta en Empleado.usuario_erp para unir persona RRHH, usuario y logistica.",
                )
            )
        if normalize_catalog_key(empleado.area) != "REPARTIDORES":
            proposals.append(
                _proposal(
                    key=f"repartidor.area:{empleado.pk}",
                    plane="repartidores",
                    entity_type="Empleado",
                    entity_id=empleado.pk,
                    display=empleado.nombre,
                    current_value=empleado.area or "(vacio)",
                    proposed_value="REPARTIDORES",
                    action="alinear_area_repartidor",
                    severity="warning",
                    reason="Repartidores deben quedar separados de ventas/produccion para no mezclarse en bonos y apps.",
                )
            )

    logistics_identity_users = User.objects.filter(
        Q(groups__name__iexact="repartidor")
        | Q(repartidor_logistica__tipo_identidad__in=["externo_autorizado", "empleado_conductor_ocasional"]),
        is_active=True,
    ).distinct()
    for user in logistics_identity_users.order_by("username"):
        groups = sorted(user.groups.values_list("name", flat=True))
        employee = getattr(user, "empleado_rrhh", None)
        external_repartidor = _external_logistics_repartidor(user)
        occasional_driver = _occasional_logistics_driver(user)
        if not employee:
            if external_repartidor:
                proposals.append(
                    _proposal(
                        key=f"repartidor.external_authorized:{user.pk}",
                        plane="repartidores",
                        entity_type="Repartidor",
                        entity_id=external_repartidor.pk,
                        display=user.username,
                        current_value=external_repartidor.empresa_externa or "externo autorizado",
                        proposed_value="Mantener separado de rrhh.Empleado",
                        action="cuenta_externa_logistica_autorizada",
                        severity="info",
                        reason="Cuenta externa autorizada para registrar uso ocasional de unidades sin crear empleado Dolce.",
                    )
                )
            else:
                proposals.append(
                    _proposal(
                        key=f"repartidor.user_without_employee:{user.pk}",
                        plane="repartidores",
                        entity_type="User",
                        entity_id=user.pk,
                        display=user.username,
                        current_value=f"usuario app/logistica; grupos={groups}; empleado_vinculado=no",
                        proposed_value="Vincular a Empleado repartidor correcto o clasificar como cuenta tecnica",
                        action="revisar_usuario_repartidor_sin_empleado",
                        severity="risk",
                        reason="El usuario puede tener acceso operativo, pero falta el enlace a la persona RRHH fuente de verdad.",
                    )
                )
        elif occasional_driver:
            proposals.append(
                _proposal(
                    key=f"repartidor.occasional_authorized:{user.pk}",
                    plane="repartidores",
                    entity_type="Repartidor",
                    entity_id=occasional_driver.pk,
                    display=user.username,
                    current_value=f"empleado={employee.nombre}; grupos={groups}",
                    proposed_value="Mantener como empleado Dolce con acceso logistico ocasional",
                    action="conductor_occasional_logistica_autorizado",
                    severity="info",
                    reason="Empleado no repartidor con permiso controlado para registrar uso ocasional de unidades.",
                )
            )
        non_repartidor_groups = [group for group in groups if group.lower() != "repartidor"]
        if non_repartidor_groups and not occasional_driver:
            proposals.append(
                _proposal(
                    key=f"repartidor.mixed_groups:{user.pk}",
                    plane="repartidores",
                    entity_type="User",
                    entity_id=user.pk,
                    display=user.username,
                    current_value=groups,
                    proposed_value="Rol repartidor aislado + accesos explicitos necesarios",
                    action="separar_grupos_repartidor",
                    severity="warning",
                    reason="Repartidor mezclado con roles ERP puede enredar middleware, PWA y permisos.",
                )
            )
    return proposals


def _external_logistics_repartidor(user):
    try:
        repartidor = user.repartidor_logistica
    except ObjectDoesNotExist:
        return None
    if getattr(repartidor, "tipo_identidad", "") == "externo_autorizado":
        return repartidor
    return None


def _occasional_logistics_driver(user):
    try:
        repartidor = user.repartidor_logistica
    except ObjectDoesNotExist:
        return None
    if getattr(repartidor, "tipo_identidad", "") == "empleado_conductor_ocasional":
        return repartidor
    return None


def _only_mixes_logistics_access(group_names: list[str], occasional_driver) -> bool:
    if not occasional_driver:
        return False
    non_repartidor_groups = [group for group in group_names if group.lower() != "repartidor"]
    return any(group.lower() == "repartidor" for group in group_names) and len(non_repartidor_groups) == 1


def _is_production_leadership_exception(empleado: Empleado) -> bool:
    return empleado.nivel_organizacional in {
        Empleado.NIVEL_DIRECCION,
        Empleado.NIVEL_JEFATURA,
        Empleado.NIVEL_SUPERVISION,
        Empleado.NIVEL_ENCARGADA,
    }


def _catalog_index(model, *fields: str) -> dict[str, object]:
    index: dict[str, object] = {}
    for item in model.objects.all():
        for field in fields:
            key = normalize_catalog_key(getattr(item, field, ""))
            if key:
                index[key] = item
    return index


def _suggest_level(empleado: Empleado) -> str:
    current = empleado.nivel_organizacional or ""
    text = " ".join(
        [
            empleado.puesto_operativo or "",
            empleado.puesto or "",
            empleado.area or "",
            empleado.departamento or "",
        ]
    ).upper()
    if "DIRECCION" in text or "DIRECCIÓN" in text:
        return Empleado.NIVEL_DIRECCION
    if empleado.usuario_erp_id:
        group_names = {name.upper() for name in empleado.usuario_erp.groups.values_list("name", flat=True)}
        if "DG" in group_names:
            return Empleado.NIVEL_DIRECCION
    if "JEF" in text or empleado.puesto_operativo == "JEFATURA":
        return Empleado.NIVEL_JEFATURA
    if "SUPERVIS" in text:
        return Empleado.NIVEL_SUPERVISION
    if "ENCARGAD" in text:
        return Empleado.NIVEL_ENCARGADA
    if not empleado.jefe_directo_id and not current:
        return ""
    if empleado.colaboradores_directos.filter(activo=True).exists():
        return Empleado.NIVEL_SUPERVISION
    if not current and (empleado.area in AREA_DIVISION_VALUES or empleado.puesto_operativo in PUESTO_OPERATIVO_VALUES):
        return Empleado.NIVEL_COLABORADOR
    return ""


def _requires_branch(empleado: Empleado) -> bool:
    return (
        empleado.departamento == Empleado.DEP_VENTAS
        or empleado.puesto_operativo == "REPARTIDOR"
        or empleado.participa_bonos_ventas
        or normalize_catalog_key(empleado.area) in {"CAJAS", "AUXILIAR_CAJAS", "CALL_CENTER", "REPARTIDORES", "VENTAS"}
    )


def _profile_suggestion(
    empleado: Empleado,
    sucursales: dict[str, object],
    departamentos: dict[str, object],
) -> str:
    values = []
    dept = departamentos.get(normalize_catalog_key(empleado.departamento))
    branch = sucursales.get(normalize_catalog_key(empleado.sucursal))
    values.append(f"departamento={getattr(dept, 'codigo', empleado.departamento or 'por definir')}")
    values.append(f"sucursal={getattr(branch, 'nombre', empleado.sucursal or 'sin sucursal')}")
    return ", ".join(values)


def _looks_non_person_user(username: str) -> bool:
    value = (username or "").lower()
    markers = ("service", "debug", "bot", "sync", "omnichannel", "ad_agent")
    return any(marker in value for marker in markers)


def _authorized_technical_account(username: str) -> bool:
    return (username or "").strip().lower() in AUTHORIZED_TECHNICAL_USERNAMES
