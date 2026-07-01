from __future__ import annotations

from django.apps import apps
from django.core.management.base import BaseCommand, CommandError
from django.db import IntegrityError, transaction

from core.branch_catalog import POINT_BRANCH_CODE_ALIASES
from core.models import Sucursal


class Command(BaseCommand):
    help = "Normaliza sucursales alias hacia sus códigos Point canónicos."

    def add_arguments(self, parser):
        parser.add_argument(
            "--execute",
            action="store_true",
            help="Aplica la normalización. Sin este flag solo audita dependencias.",
        )

    def handle(self, *args, **options):
        execute = bool(options["execute"])
        for ghost_code, canonical_code in POINT_BRANCH_CODE_ALIASES.items():
            ghost = Sucursal.objects.filter(codigo=ghost_code).first()
            canonical = Sucursal.objects.filter(codigo=canonical_code).first()
            if not ghost:
                self.stdout.write(f"{ghost_code}: no existe, nada que normalizar.")
                continue
            if not canonical:
                raise CommandError(f"No existe la sucursal canónica {canonical_code} para {ghost_code}.")

            dependencies = self._dependency_counts(ghost)
            self.stdout.write(self.style.WARNING(f"Alias {ghost_code} -> {canonical_code}"))
            self.stdout.write(f"  - ghost: {ghost.id} {ghost.nombre}")
            self.stdout.write(f"  - canonical: {canonical.id} {canonical.nombre}")
            for label, count in dependencies:
                self.stdout.write(f"  - {label}: {count}")

            if not execute:
                continue

            with transaction.atomic():
                self._migrate_foreign_keys(ghost=ghost, canonical=canonical)
                remaining = self._dependency_counts(ghost)
                if remaining:
                    details = ", ".join(f"{label}={count}" for label, count in remaining)
                    raise CommandError(f"{ghost_code} todavía tiene dependencias: {details}")
                ghost.delete()
                self.stdout.write(self.style.SUCCESS(f"  - {ghost_code} eliminado"))

        if not execute:
            self.stdout.write("Modo auditoría: sin cambios. Usa --execute para aplicar la normalización.")

    def _dependency_counts(self, branch: Sucursal) -> list[tuple[str, int]]:
        counts: list[tuple[str, int]] = []
        for model in apps.get_models(include_auto_created=True):
            for field in model._meta.get_fields():
                if not getattr(field, "concrete", False):
                    continue
                if getattr(field, "related_model", None) is not Sucursal:
                    continue
                if not (getattr(field, "many_to_one", False) or getattr(field, "one_to_one", False)):
                    continue
                count = model.objects.filter(**{field.name: branch}).count()
                if count:
                    counts.append((f"{model._meta.label}.{field.name}", count))
        return sorted(counts)

    def _migrate_foreign_keys(self, *, ghost: Sucursal, canonical: Sucursal) -> None:
        for model in apps.get_models(include_auto_created=True):
            for field in model._meta.get_fields():
                if not getattr(field, "concrete", False):
                    continue
                if getattr(field, "related_model", None) is not Sucursal:
                    continue
                if not (getattr(field, "many_to_one", False) or getattr(field, "one_to_one", False)):
                    continue
                queryset = model.objects.filter(**{field.name: ghost})
                if not queryset.exists():
                    continue
                try:
                    queryset.update(**{field.name: canonical})
                except IntegrityError as exc:
                    raise CommandError(
                        f"No pude migrar {model._meta.label}.{field.name} de {ghost.codigo} a {canonical.codigo}: {exc}"
                    ) from exc
