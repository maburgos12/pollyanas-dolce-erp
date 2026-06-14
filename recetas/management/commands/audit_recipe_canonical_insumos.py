import json

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count, Q

from maestros.models import Insumo
from recetas.models import LineaReceta


LEGACY_NO_CODE_NAMES = ["Galleta Para Pay", "Mermelada Fresa"]
CANONICAL_TEXT_RULES = {
    "Galleta Pay": "01GP13",
    "Mermelada Fresa Liquida": "01MF06",
}


class Command(BaseCommand):
    help = "Audita que las líneas activas de receta apunten a insumos canónicos de Point."

    def add_arguments(self, parser):
        parser.add_argument("--json", action="store_true", help="Imprime salida JSON.")
        parser.add_argument(
            "--no-fail",
            action="store_true",
            help="No falla aunque encuentre observaciones.",
        )
        parser.add_argument(
            "--include-missing-point-code",
            action="store_true",
            help="Incluye materias primas activas sin código Point como observación.",
        )

    def handle(self, *args, **options):
        issues = {
            "legacy_recipe_lines": self._legacy_recipe_lines(),
            "inactive_recipe_lines": self._inactive_recipe_lines(),
            "canonical_text_mismatches": self._canonical_text_mismatches(),
            "duplicate_active_point_codes": self._duplicate_active_point_codes(),
        }
        if options["include_missing_point_code"]:
            issues["active_materials_without_point_code"] = self._active_materials_without_point_code()

        summary = {key: len(value) for key, value in issues.items()}
        total = sum(summary.values())
        payload = {
            "ok": total == 0,
            "summary": summary,
            "issues": issues,
        }

        if options["json"]:
            self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        else:
            self.stdout.write("Auditoría de insumos canónicos en recetas")
            for key, count in summary.items():
                self.stdout.write(f"  - {key}: {count}")
            if total:
                for key, rows in issues.items():
                    for row in rows[:10]:
                        self.stdout.write(f"    * {key}: {row}")

        if total and not options["no_fail"]:
            raise CommandError(f"Auditoría fallida: {total} observaciones de insumos no canónicos.")

    def _active_recipe_lines(self):
        return (
            LineaReceta.objects.select_related("receta", "insumo")
            .exclude(match_status=LineaReceta.STATUS_REJECTED)
            .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        )

    def _line_payload(self, line):
        insumo = line.insumo
        return {
            "linea_id": line.id,
            "receta_codigo": line.receta.codigo_point,
            "receta": line.receta.nombre,
            "posicion": line.posicion,
            "insumo_texto": line.insumo_texto,
            "insumo_id": line.insumo_id,
            "insumo_codigo_point": insumo.codigo_point if insumo else "",
            "insumo": insumo.nombre if insumo else "",
            "insumo_activo": insumo.activo if insumo else None,
        }

    def _legacy_recipe_lines(self):
        return [
            self._line_payload(line)
            for line in self._active_recipe_lines()
            .filter(insumo__codigo_point="", insumo__nombre__in=LEGACY_NO_CODE_NAMES)
            .order_by("receta__codigo_point", "posicion", "id")
        ]

    def _inactive_recipe_lines(self):
        return [
            self._line_payload(line)
            for line in self._active_recipe_lines().filter(insumo__isnull=False, insumo__activo=False).order_by("receta__codigo_point", "posicion", "id")
        ]

    def _canonical_text_mismatches(self):
        mismatch_filter = Q()
        for text, code in CANONICAL_TEXT_RULES.items():
            mismatch_filter |= Q(insumo_texto__iexact=text) & ~Q(insumo__codigo_point__iexact=code)
        return [
            self._line_payload(line)
            for line in self._active_recipe_lines().filter(mismatch_filter).order_by("receta__codigo_point", "posicion", "id")
        ]

    def _duplicate_active_point_codes(self):
        rows = (
            Insumo.objects.filter(activo=True)
            .exclude(codigo_point="")
            .values("codigo_point")
            .annotate(total=Count("id"))
            .filter(total__gt=1)
            .order_by("-total", "codigo_point")
        )
        result = []
        for row in rows:
            members = list(
                Insumo.objects.filter(activo=True, codigo_point=row["codigo_point"])
                .order_by("id")
                .values("id", "nombre", "tipo_item")
            )
            result.append({"codigo_point": row["codigo_point"], "total": row["total"], "members": members})
        return result

    def _active_materials_without_point_code(self):
        rows = (
            Insumo.objects.filter(activo=True, tipo_item=Insumo.TIPO_MATERIA_PRIMA, codigo_point="")
            .order_by("nombre", "id")
            .values("id", "nombre", "tipo_item", "categoria")[:200]
        )
        return list(rows)
