from decimal import Decimal

from django.db import transaction
from django.utils import timezone

from pos_bridge.models import PointBranch, PointInsumoInventorySnapshot
from pos_bridge.services.inventory_baseline import TRUSTED_INSUMO_MATCH_METHODS
from pos_bridge.services.point_inventory_cost_capture_service import PointInventoryCostCaptureService
from pos_bridge.services.recipe_identity_service import PointRecipeIdentityService
from pos_bridge.services.unidades import cantidad_en_unidad_erp
from recetas.utils.costeo_snapshot import POINT_UNIT_ALIASES
from maestros.utils.canonical_catalog import canonical_insumo_by_id


class CanonicalInsumoInventoryCaptureService:
    LOCATIONS = ("ALMACEN", "CEDIS")

    def __init__(self, *, capture_service=None, identity_service=None):
        self.capture_service = capture_service or PointInventoryCostCaptureService()
        self.identity_service = identity_service or PointRecipeIdentityService()

    @transaction.atomic
    def capture(self, *, sync_job, locations=None, captured_at=None):
        captured_at = captured_at or timezone.now()
        blockers = []
        counts = {}
        complete = True
        for location in locations or self.LOCATIONS:
            branch = self._official_branch(location)
            rows = [
                row
                for row in self.capture_service.capture_all_rows(
                    branch_hint=location,
                    branch_external_id=branch.external_id,
                )
                if getattr(row, "kind", "supply") == "supply"
            ]
            seen = {}
            conflicted_insumo_ids = set()
            for row in rows:
                resolved = self.identity_service.resolve_insumo(
                    point_code=row.point_code,
                    point_name=row.point_name,
                )
                method = str(getattr(resolved, "method", "") or "")
                score = float(getattr(resolved, "score", 0) or 0)
                insumo = getattr(resolved, "insumo", None)
                if insumo is None or method not in TRUSTED_INSUMO_MATCH_METHODS or score < 100:
                    blockers.append({"location": location, "point_code": row.point_code, "reason": "MATCH_NO_CONFIABLE"})
                    continue
                insumo = canonical_insumo_by_id(insumo.id) or insumo
                normalized_unit = " ".join(str(row.unit or "").strip().lower().split())
                if normalized_unit not in POINT_UNIT_ALIASES:
                    blockers.append({"location": location, "point_code": row.point_code, "reason": "UNIDAD_DESCONOCIDA"})
                    continue
                quantity_base, note = cantidad_en_unidad_erp(row.quantity, row.unit, insumo)
                if note.startswith("UNIDAD INCOMPATIBLE"):
                    blockers.append({"location": location, "point_code": row.point_code, "reason": note})
                    continue
                previous = seen.get(insumo.id)
                quantity_base = Decimal(str(quantity_base))
                if insumo.id in conflicted_insumo_ids:
                    continue
                if previous is not None and previous != quantity_base:
                    blockers.append({"location": location, "point_code": row.point_code, "reason": "SALDO_DUPLICADO_CONFLICTIVO"})
                    conflicted_insumo_ids.add(insumo.id)
                    PointInsumoInventorySnapshot.objects.filter(
                        sync_job=sync_job,
                        branch=branch,
                        insumo=insumo,
                    ).delete()
                    continue
                seen[insumo.id] = quantity_base
                PointInsumoInventorySnapshot.objects.update_or_create(
                    sync_job=sync_job,
                    branch=branch,
                    insumo=insumo,
                    defaults={
                        "point_code": row.point_code,
                        "point_name": row.point_name,
                        "point_quantity": row.quantity,
                        "point_unit": row.unit,
                        "quantity_base": quantity_base,
                        "captured_at": captured_at,
                        "raw_payload": {
                            "kind": "supply",
                            "category": row.point_category,
                            "row": row.raw_row,
                        },
                    },
                )
            snapshot_count = PointInsumoInventorySnapshot.objects.filter(
                sync_job=sync_job,
                branch=branch,
            ).count()
            counts[location] = {"rows": len(rows), "snapshots": snapshot_count}
            if not rows or not snapshot_count:
                blockers.append({"location": location, "reason": "SIN_INSUMOS_CONFIRMADOS"})
                complete = False
        return {
            # Los bloqueos por renglón se aíslan al insumo afectado. El ciclo solo
            # es parcial cuando una ubicación completa no produjo evidencia usable.
            "complete": complete,
            "locations": counts,
            "blockers": blockers,
        }

    @staticmethod
    def _official_branch(location):
        candidates = list(
            PointBranch.objects.filter(
                normalized_name=location.lower(),
                status=PointBranch.STATUS_ACTIVE,
            ).order_by("id")
        )
        numeric = [row for row in candidates if str(row.external_id or "").strip().isdigit()]
        if len(numeric) != 1:
            raise ValueError(f"Se requiere una sucursal Point numérica activa para {location}.")
        return numeric[0]
