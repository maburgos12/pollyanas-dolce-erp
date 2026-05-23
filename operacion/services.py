from __future__ import annotations

from dataclasses import asdict, dataclass

from core.access import (
    ACCESS_MANAGE,
    ROLE_DG,
    ROLE_LOGISTICA,
    can_view_module,
    can_manage_submodule,
    can_view_submodule,
    is_branch_capture_only,
    is_repartidor_only,
    primary_role,
)


@dataclass(frozen=True)
class OperacionTile:
    key: str
    title: str
    detail: str
    href: str
    icon: str
    area: str


def _group_names(user) -> set[str]:
    if not user or not user.is_authenticated:
        return set()
    return set(user.groups.values_list("name", flat=True))


def _profile(user):
    return getattr(user, "userprofile", None)


def _repartidor(user):
    try:
        return user.repartidor_logistica
    except Exception:
        return None


def _can_receive_mermas(user) -> bool:
    if can_manage_submodule(user, "mermas", "recepcion"):
        return True
    try:
        from mermas.models import PersonalEnviosSucursal

        return PersonalEnviosSucursal.objects.filter(user=user, activo=True).exists()
    except Exception:
        return False


def _can_use_mantenimiento(user) -> bool:
    groups = _group_names(user)
    return (
        user.is_superuser
        or ROLE_DG in groups
        or "dg" in groups
        or "compras_logistica" in groups
        or "mantenimiento" in groups
        or "MANTENIMIENTO" in groups
        or can_view_module(user, "activos")
    )


def _append_logistica_tiles(tiles: list[OperacionTile], user, *, mobile_only: bool = False) -> None:
    if mobile_only:
        tiles.extend(
            [
                OperacionTile(
                    key="logistica_nuevo_reporte",
                    title="Nuevo Reporte",
                    detail="Falla, accidente, llanta, combustible u otro evento.",
                    href="/logistica/app/?pantalla=nuevo_reporte",
                    icon="reporte",
                    area="Logística",
                ),
                OperacionTile(
                    key="logistica_mis_reportes",
                    title="Mis Reportes",
                    detail="Seguimiento de reportes abiertos y cerrados.",
                    href="/logistica/app/?pantalla=mis_reportes",
                    icon="historial",
                    area="Logística",
                ),
                OperacionTile(
                    key="logistica_inspeccion",
                    title="Inspección",
                    detail="Checklist y evidencia de la unidad.",
                    href="/logistica/app/?pantalla=inspeccion_vehiculo",
                    icon="inspeccion",
                    area="Logística",
                ),
                OperacionTile(
                    key="logistica_lavado",
                    title="Lavado",
                    detail="Registro de lavado exterior, interior o caja refrigerada.",
                    href="/logistica/app/?pantalla=lavado",
                    icon="lavado",
                    area="Logística",
                ),
                OperacionTile(
                    key="logistica_bitacora",
                    title="Bitácora",
                    detail="Inicio y cierre de turno; incluye registro de gasolina.",
                    href="/logistica/app/?pantalla=bitacora_salida",
                    icon="bitacora",
                    area="Logística",
                ),
            ]
        )
        return

    if _repartidor(user):
        tiles.append(
            OperacionTile(
                key="logistica_app",
                title="Logística móvil",
                detail="Reportes, inspección, lavado, bitácora y combustible.",
                href="/logistica/app/",
                icon="ruta",
                area="Logística",
            )
        )
    if can_manage_submodule(user, "logistica", "tickets"):
        tiles.append(
            OperacionTile(
                key="logistica_tickets",
                title="Tickets logística",
                detail="Incidencias abiertas y seguimiento de unidades.",
                href="/logistica/tickets/",
                icon="tickets",
                area="Logística",
            )
        )
    if can_view_submodule(user, "logistica", "flota") or can_view_submodule(user, "logistica", "unidades"):
        tiles.append(
            OperacionTile(
                key="logistica_flota",
                title="Flota",
                detail="Unidades, servicios y estado operativo.",
                href="/logistica/flota/",
                icon="flota",
                area="Logística",
            )
        )
    if can_view_submodule(user, "logistica", "rutas"):
        tiles.append(
            OperacionTile(
                key="logistica_rutas",
                title="Rutas",
                detail="Planeación y entregas del día.",
                href="/logistica/rutas/",
                icon="entrega",
                area="Logística",
            )
        )


def build_operacion_context(user) -> dict:
    profile = _profile(user)
    repartidor = _repartidor(user)
    groups = _group_names(user)
    tiles: list[OperacionTile] = []

    if is_repartidor_only(user):
        _append_logistica_tiles(tiles, user, mobile_only=True)
        role_label = "Repartidor"
        scope_label = "Bitácora y evidencias"
        location = getattr(getattr(repartidor, "sucursal", None), "nombre", "Logística móvil")
    else:
        if can_view_submodule(user, "mermas", "captura"):
            tiles.append(
                OperacionTile(
                    key="mermas_captura",
                    title="Registrar merma",
                    detail="Captura de sucursal con productos, ticket y evidencia.",
                    href="/mermas/app/?modo=captura",
                    icon="merma",
                    area="Sucursal",
                )
            )
        if _can_receive_mermas(user):
            tiles.append(
                OperacionTile(
                    key="mermas_recepcion",
                    title="Recibir merma",
                    detail="Validación CEDIS de cantidades, evidencia y repartidor.",
                    href="/mermas/app/?modo=recepcion",
                    icon="recibir",
                    area="CEDIS",
                )
            )
        if can_view_submodule(user, "fallas", "reportar"):
            tiles.append(
                OperacionTile(
                    key="fallas_reportar",
                    title="Reportar falla",
                    detail="Foto, sucursal, activo y prioridad del reporte.",
                    href="/fallas/reportar/",
                    icon="falla",
                    area="Sucursal",
                )
            )
        if can_view_submodule(user, "fallas", "mis_reportes"):
            tiles.append(
                OperacionTile(
                    key="fallas_mis_reportes",
                    title="Mis reportes",
                    detail="Seguimiento de fallas enviadas por el usuario.",
                    href="/fallas/mis-reportes/",
                    icon="historial",
                    area="Sucursal",
                )
            )
        if _can_use_mantenimiento(user):
            tiles.append(
                OperacionTile(
                    key="mantenimiento_activos",
                    title="Mantenimiento",
                    detail="Equipos, activos y órdenes permitidas.",
                    href="/mantenimiento/app/",
                    icon="mantenimiento",
                    area="Activos",
                )
            )
        if is_branch_capture_only(user):
            tiles.append(
                OperacionTile(
                    key="reabasto_captura",
                    title="Captura sucursal",
                    detail="Cierre operativo de reabasto CEDIS.",
                    href="/recetas/reabasto-cedis/captura/",
                    icon="cierre",
                    area="Sucursal",
                )
            )
        if can_view_module(user, "logistica"):
            _append_logistica_tiles(tiles, user)

        if _can_receive_mermas(user):
            role_label = "CEDIS"
            scope_label = "Recepción y validación"
        elif can_view_module(user, "logistica") or ROLE_LOGISTICA in groups:
            role_label = "Logística"
            scope_label = "Control operativo"
        elif getattr(profile, "sucursal", None):
            role_label = "Sucursal"
            scope_label = "Captura operativa"
        else:
            role_label = primary_role(user) or "App operativa"
            scope_label = "Accesos permitidos"
        location = getattr(getattr(profile, "sucursal", None), "nombre", "") or role_label

    tiles_dict = [asdict(tile) for tile in tiles]
    return {
        "app_title": "App Operativa",
        "user_label": user.get_full_name() or user.username,
        "username": user.username,
        "role_label": role_label,
        "scope_label": scope_label,
        "location_label": location,
        "tiles": tiles_dict,
        "has_tiles": bool(tiles_dict),
    }
