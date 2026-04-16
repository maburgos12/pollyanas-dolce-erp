from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone as dt_timezone

from django.db.models import Max

from core.branch_catalog import eligible_operational_branch_qs
from core.models import Sucursal
from pos_bridge.models import PointBranch, PointInventorySnapshot
from recetas.utils.normalizacion import normalizar_nombre


@dataclass(slots=True)
class SucursalResolution:
    sucursal: Sucursal
    point_branch: PointBranch | None
    source: str


class SucursalResolutionError(Exception):
    def __init__(self, message: str, *, code: str, payload: dict | None = None):
        super().__init__(message)
        self.code = code
        self.payload = payload or {}


class SucursalResolverService:
    MIN_DATETIME = datetime(1970, 1, 1, tzinfo=dt_timezone.utc)

    def resolve_sucursal(self, raw_input: str) -> SucursalResolution:
        raw_value = (raw_input or "").strip()
        if not raw_value:
            raise SucursalResolutionError("sucursal es obligatoria.", code="missing_branch_code")

        sucursal = self._resolve_erp_sucursal(raw_value)
        source = "ERP"
        point_branch = None

        if sucursal is None:
            point_branch = self._resolve_point_branch_input(raw_value)
            if point_branch is not None:
                sucursal = point_branch.erp_branch
                source = "POINT"

        if sucursal is None:
            raise SucursalResolutionError(
                "Sucursal no encontrada o inactiva.",
                code="branch_not_found",
                payload={"branch_code": raw_value},
            )

        if point_branch is None:
            point_branch = self._resolve_point_branch_for_sucursal(sucursal)

        return SucursalResolution(sucursal=sucursal, point_branch=point_branch, source=source)

    def _resolve_erp_sucursal(self, raw_value: str) -> Sucursal | None:
        operativas = eligible_operational_branch_qs()
        sucursal = operativas.filter(codigo__iexact=raw_value).first()
        if sucursal is None:
            sucursal = operativas.filter(nombre__iexact=raw_value).first()
        if sucursal is None:
            target = normalizar_nombre(raw_value)
            for row in operativas.only("id", "codigo", "nombre"):
                if normalizar_nombre(row.nombre) == target or normalizar_nombre(row.codigo) == target:
                    sucursal = row
                    break
        if sucursal is None:
            target = normalizar_nombre(raw_value)
            scored_matches: list[tuple[int, int, Sucursal]] = []
            for row in operativas.only("id", "codigo", "nombre"):
                codigo_norm = normalizar_nombre(row.codigo)
                nombre_norm = normalizar_nombre(row.nombre)
                score = 0
                if codigo_norm and codigo_norm in target:
                    score = max(score, 3)
                if nombre_norm and nombre_norm in target:
                    score = max(score, 2)
                if score:
                    scored_matches.append((score, row.id, row))
            if scored_matches:
                scored_matches.sort(key=lambda item: (-item[0], item[1]))
                top_score = scored_matches[0][0]
                top_matches = [row for score, _, row in scored_matches if score == top_score]
                if len({row.id for row in top_matches}) == 1:
                    sucursal = top_matches[0]
        return sucursal

    def _resolve_point_branch_input(self, raw_value: str) -> PointBranch | None:
        token = raw_value.strip()
        if not token:
            return None

        queryset = (
            PointBranch.objects.filter(erp_branch__isnull=False)
            .select_related("erp_branch")
            .only(
                "id",
                "external_id",
                "name",
                "normalized_name",
                "status",
                "last_seen_at",
                "updated_at",
                "erp_branch__id",
                "erp_branch__codigo",
                "erp_branch__nombre",
                "erp_branch__activa",
            )
        )

        exact_matches = list(queryset.filter(external_id__iexact=token))
        if not exact_matches:
            exact_matches = list(queryset.filter(name__iexact=token))
        if not exact_matches:
            token_norm = normalizar_nombre(token)
            exact_matches = [branch for branch in queryset if branch.normalized_name == token_norm]
        if not exact_matches:
            exact_matches = list(queryset.filter(name__icontains=token))

        operativas_ids = set(eligible_operational_branch_qs().values_list("id", flat=True))
        candidates = [branch for branch in exact_matches if branch.erp_branch_id in operativas_ids]
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        return self._pick_best_point_branch(candidates)

    def _resolve_point_branch_for_sucursal(self, sucursal: Sucursal) -> PointBranch | None:
        branches = list(
            PointBranch.objects.filter(erp_branch=sucursal)
            .select_related("erp_branch")
            .only("id", "external_id", "name", "status", "last_seen_at", "updated_at", "erp_branch__id")
        )
        if not branches:
            return None
        return self._pick_best_point_branch(branches)

    def _pick_best_point_branch(self, branches: list[PointBranch]) -> PointBranch:
        latest_snapshot_map = {
            row["branch_id"]: row["latest_captured_at"]
            for row in (
                PointInventorySnapshot.objects.filter(branch_id__in=[branch.id for branch in branches])
                .values("branch_id")
                .annotate(latest_captured_at=Max("captured_at"))
            )
        }

        def _stamp(value):
            return value or self.MIN_DATETIME

        return max(
            branches,
            key=lambda branch: (
                branch.status == PointBranch.STATUS_ACTIVE,
                latest_snapshot_map.get(branch.id) is not None,
                _stamp(latest_snapshot_map.get(branch.id)),
                _stamp(branch.last_seen_at),
                _stamp(branch.updated_at),
                branch.id,
            ),
        )


def resolve_sucursal(raw_input: str) -> SucursalResolution:
    return SucursalResolverService().resolve_sucursal(raw_input)
