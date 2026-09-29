"""Inventario de metadatos para revisar la reutilización de datos del ERP."""

from __future__ import annotations

import json
import re

from django.apps import apps
from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from django.db.models import UniqueConstraint
from unidecode import unidecode


# Pistas de búsqueda, no reglas de identidad ni de fusión.
SEARCH_HINTS = {
    "colaborador": ("empleado",),
    "personal": ("empleado",),
    "tienda": ("sucursal",),
    "sede": ("sucursal",),
    "material": ("insumo",),
    "materia prima": ("insumo",),
    "abastecedor": ("proveedor",),
}


def _normalize(value: object) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", unidecode(str(value or "")).lower()))


def _expand_terms(terms: list[str]) -> list[str]:
    expanded = []
    for term in terms:
        for candidate in (term, *SEARCH_HINTS.get(term, ())):
            if candidate not in expanded:
                expanded.append(candidate)
    return expanded


def _candidate(model, terms: list[str]) -> tuple[int, list[str]]:
    meta = model._meta
    names = [model.__name__, meta.model_name, meta.verbose_name, meta.verbose_name_plural]
    names = [_normalize(name) for name in names]
    table = _normalize(meta.db_table)
    fields = [
        (_normalize(field.name), _normalize(field.verbose_name), _normalize(field.help_text))
        for field in meta.concrete_fields
    ]
    score = 0
    matches = []
    for term in terms:
        if term in names:
            score += 100
            matches.append(f"modelo:{term}")
        elif any(term in name for name in names) or term in table:
            score += 60
            matches.append(f"modelo_parcial:{term}")
        elif any(any(term in part for part in field) for field in fields):
            score += 15
            matches.append(f"campo:{term}")
    return score, matches


def _describe_model(model, matches: list[str], score: int, existing_tables: set[str], details: bool) -> dict:
    meta = model._meta
    unique_rules = [
        {
            "name": constraint.name,
            "fields": list(constraint.fields),
            "condicion": str(constraint.condition) if constraint.condition else None,
        }
        for constraint in meta.constraints
        if isinstance(constraint, UniqueConstraint)
    ]
    for field in meta.concrete_fields:
        if field.unique:
            unique_rules.append({"name": f"{field.name}_unique", "fields": [field.name]})
    for fields in meta.unique_together:
        unique_rules.append({"name": "unique_together", "fields": list(fields)})
    result = {
        "modelo": meta.label,
        "tabla": meta.db_table,
        "tabla_existe": meta.db_table in existing_tables,
        "descripcion": str(meta.verbose_name),
        "coincidencias_lexicas": matches,
        "puntaje_busqueda": score,
        "identificadores_y_unicidad": unique_rules,
        "columnas": [field.column for field in meta.concrete_fields],
        "relaciones": [
            {"campo": field.name, "modelo": field.related_model._meta.label}
            for field in meta.concrete_fields
            if field.is_relation and field.related_model
        ],
    }
    if details:
        result["campos"] = [
            {
                "nombre": field.name,
                "columna": field.column,
                "tipo": field.get_internal_type(),
                "descripcion": str(field.verbose_name),
                "relacion": field.related_model._meta.label if field.is_relation and field.related_model else None,
                "indice": field.db_index,
            }
            for field in meta.concrete_fields
        ]
    return result


class Command(BaseCommand):
    help = "Consulta modelos, columnas, llaves y relaciones existentes sin modificar datos."

    def add_arguments(self, parser):
        parser.add_argument("--term", action="append", default=[], help="Concepto o sinónimo; repetible.")
        parser.add_argument("--all", action="store_true", help="Incluye todos los modelos.")
        parser.add_argument("--presence", action="store_true", help="Comprueba si hay al menos una fila, sin leer valores.")
        parser.add_argument("--limit", type=int, default=15, help="Máximo de candidatos; por defecto 15.")
        parser.add_argument("--details", action="store_true", help="Incluye tipo y descripción de cada campo.")

    def handle(self, *args, **options):
        if connection.vendor != "postgresql":
            raise CommandError("Este inventario requiere PostgreSQL.")
        requested_terms = [_normalize(term) for term in options["term"] if _normalize(term)]
        if not requested_terms and not options["all"]:
            raise CommandError("Indica --term <concepto> o --all.")
        if options["limit"] < 1:
            raise CommandError("--limit debe ser positivo.")
        terms = _expand_terms(requested_terms)

        existing_tables = set(connection.introspection.table_names())
        mapped_tables = {model._meta.db_table for model in apps.get_models(include_auto_created=True)}
        unmapped_tables = sorted(existing_tables - mapped_tables)
        unmapped_candidates = [
            table for table in unmapped_tables
            if options["all"] or any(term in _normalize(table) for term in terms)
        ]
        models = []
        for model in apps.get_models():
            score, matches = _candidate(model, terms)
            if options["all"] or score:
                models.append(_describe_model(model, matches, score, existing_tables, options["details"]))
        models.sort(key=lambda item: (-item["puntaje_busqueda"], item["modelo"]))
        total = len(models)
        models = models[: options["limit"]]

        if options["presence"]:
            with transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute("SET TRANSACTION READ ONLY")
                    for item in models:
                        if not item["tabla_existe"]:
                            item["contiene_registros"] = None
                            continue
                        table_name = connection.ops.quote_name(item["tabla"])
                        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM {table_name} LIMIT 1)")
                        item["contiene_registros"] = cursor.fetchone()[0]

        self.stdout.write(
            json.dumps(
                {
                    "alcance": "metadatos_postgresql_y_django",
                    "terminos_solicitados": requested_terms,
                    "terminos_de_busqueda": terms,
                    "total_modelos_candidatos": total,
                    "modelos_mostrados": len(models),
                    "total_tablas_sin_modelo": len(unmapped_tables),
                    "tablas_sin_modelo_candidatas": unmapped_candidates,
                    "valores_de_registros_consultados": False,
                    "nota": "Las pistas y coincidencias son de búsqueda; no confirman equivalencia de entidades.",
                    "modelos": models,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
