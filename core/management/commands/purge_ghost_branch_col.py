from __future__ import annotations

from dataclasses import dataclass

from django.apps import apps
from django.core.management.base import BaseCommand, CommandError
from django.db import IntegrityError, transaction
from django.utils import timezone

from core.models import AuditLog, Sucursal, UserProfile
from recetas.models import PoliticaStockSucursalProducto
from reportes.models import CentroCosto, GastoOperativoMensual, ReglaAsignacionGasto


@dataclass
class PurgeSummary:
    userprofiles_migrated: int = 0
    stock_policies_migrated: int = 0
    stock_policies_deleted: int = 0
    centros_deleted: int = 0
    centros_relinked: int = 0


class Command(BaseCommand):
    help = "Purga la sucursal fantasma COL y migra sus dependencias a COLOSIO."

    def add_arguments(self, parser):
        parser.add_argument("--ghost-code", default="COL")
        parser.add_argument("--canonical-code", default="COLOSIO")
        parser.add_argument(
            "--execute",
            action="store_true",
            help="Aplica la purga. Sin este flag solo muestra la auditoría.",
        )

    def handle(self, *args, **options):
        ghost_code = str(options["ghost_code"]).strip().upper()
        canonical_code = str(options["canonical_code"]).strip().upper()
        execute = bool(options["execute"])

        ghost = Sucursal.objects.filter(codigo=ghost_code).first()
        canonical = Sucursal.objects.filter(codigo=canonical_code).first()
        if not ghost:
            raise CommandError(f"No existe la sucursal fantasma {ghost_code}.")
        if not canonical:
            raise CommandError(f"No existe la sucursal canónica {canonical_code}.")
        if ghost.pk == canonical.pk:
            raise CommandError("La sucursal fantasma y la canónica no pueden ser la misma.")

        dependencies = self._dependency_counts(ghost)
        self.stdout.write(self.style.WARNING("Auditoría de sucursal fantasma"))
        self.stdout.write(f"  - ghost: {ghost.id} {ghost.codigo} - {ghost.nombre}")
        self.stdout.write(f"  - canonical: {canonical.id} {canonical.codigo} - {canonical.nombre}")
        for label, count in dependencies:
            self.stdout.write(f"  - {label}: {count}")

        if not execute:
            self.stdout.write("Modo auditoría: sin cambios. Usa --execute para aplicar la purga.")
            return

        with transaction.atomic():
            summary = PurgeSummary()
            summary.userprofiles_migrated = UserProfile.objects.filter(sucursal=ghost).update(sucursal=canonical)

            for policy in PoliticaStockSucursalProducto.objects.select_for_update().filter(sucursal=ghost):
                duplicate = PoliticaStockSucursalProducto.objects.filter(
                    sucursal=canonical,
                    receta=policy.receta,
                ).first()
                if duplicate:
                    policy.delete()
                    summary.stock_policies_deleted += 1
                    continue
                policy.sucursal = canonical
                policy.save(update_fields=["sucursal", "actualizado_en"])
                summary.stock_policies_migrated += 1

            canonical_center = CentroCosto.objects.filter(
                sucursal=canonical,
                nombre__iexact="Sucursal Colosio",
            ).first()
            for center in CentroCosto.objects.select_for_update().filter(sucursal=ghost):
                rules_count = ReglaAsignacionGasto.objects.filter(centro_costo=center).count()
                expenses_count = GastoOperativoMensual.objects.filter(centro_costo=center).count()
                if rules_count or expenses_count:
                    target_center = canonical_center or CentroCosto.objects.filter(sucursal=canonical, tipo=center.tipo).order_by("id").first()
                    if not target_center:
                        center.sucursal = canonical
                        center.save(update_fields=["sucursal", "actualizado_en"])
                        summary.centros_relinked += 1
                        continue
                    ReglaAsignacionGasto.objects.filter(centro_costo=center).update(centro_costo=target_center)
                    GastoOperativoMensual.objects.filter(centro_costo=center).update(centro_costo=target_center)
                center.delete()
                summary.centros_deleted += 1

            remaining = self._dependency_counts(ghost)
            non_zero = [(label, count) for label, count in remaining if count]
            if non_zero:
                details = ", ".join(f"{label}={count}" for label, count in non_zero)
                raise CommandError(f"La sucursal {ghost_code} aún tiene dependencias: {details}")

            AuditLog.objects.create(
                timestamp=timezone.now(),
                action="DELETE",
                model="core.Sucursal",
                object_id=str(ghost.pk),
                payload={
                    "trigger": "purge_ghost_branch_col",
                    "ghost_code": ghost_code,
                    "canonical_code": canonical_code,
                    "summary": {
                        "userprofiles_migrated": summary.userprofiles_migrated,
                        "stock_policies_migrated": summary.stock_policies_migrated,
                        "stock_policies_deleted": summary.stock_policies_deleted,
                        "centros_deleted": summary.centros_deleted,
                        "centros_relinked": summary.centros_relinked,
                    },
                },
            )
            ghost.delete()

        self.stdout.write(self.style.SUCCESS("Purga aplicada"))
        self.stdout.write(f"  - perfiles migrados: {summary.userprofiles_migrated}")
        self.stdout.write(f"  - políticas migradas: {summary.stock_policies_migrated}")
        self.stdout.write(f"  - políticas eliminadas: {summary.stock_policies_deleted}")
        self.stdout.write(f"  - centros eliminados: {summary.centros_deleted}")
        self.stdout.write(f"  - centros relinked: {summary.centros_relinked}")

    def _dependency_counts(self, branch: Sucursal) -> list[tuple[str, int]]:
        counts: list[tuple[str, int]] = []
        for model in apps.get_models():
            for field in model._meta.get_fields():
                if getattr(field, "many_to_one", False) and getattr(field, "related_model", None) is Sucursal:
                    try:
                        count = model.objects.filter(**{field.name: branch}).count()
                    except Exception:
                        continue
                    if count:
                        counts.append((f"{model._meta.label}.{field.name}", count))
        return sorted(counts)
