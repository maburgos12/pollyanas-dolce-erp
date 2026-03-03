from __future__ import annotations

import csv
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import transaction

from inventario.models import AlmacenSyncRun
from inventario.views import (
    _apply_cross_filters,
    _build_cross_unified_rows,
    _build_pending_grouped,
    _resolve_cross_source_with_alias,
)
from maestros.models import Insumo, InsumoAlias, PointPendingMatch
from recetas.utils.normalizacion import normalizar_nombre


class Command(BaseCommand):
    help = (
        "Resuelve pendientes unificados (ALMACEN/POINT/RECETAS) priorizados "
        "por fuentes activas y frecuencia. Usa --apply para ejecutar cambios reales."
    )

    def add_arguments(self, parser):
        parser.add_argument("--runs", type=int, default=5, help="Corridas de almacén a escanear (default: 5).")
        parser.add_argument("--limit", type=int, default=120, help="Máximo de candidatos a procesar (default: 120).")
        parser.add_argument("--offset", type=int, default=0, help="Offset sobre candidatos filtrados (default: 0).")
        parser.add_argument("--q", default="", help="Filtro texto sobre nombre/sugerencia.")
        parser.add_argument(
            "--source",
            default="TODOS",
            choices=["TODOS", "ALMACEN", "POINT", "RECETAS"],
            help="Fuente a priorizar (default: TODOS).",
        )
        parser.add_argument(
            "--point-tipo",
            default=PointPendingMatch.TIPO_INSUMO,
            choices=[PointPendingMatch.TIPO_INSUMO, PointPendingMatch.TIPO_PROVEEDOR, PointPendingMatch.TIPO_PRODUCTO, "TODOS"],
            help="Tipo de pendiente Point a incluir (default: INSUMO).",
        )
        parser.add_argument("--min-sources", type=int, default=2, help="Mínimo de fuentes activas para incluir (default: 2).")
        parser.add_argument("--score-min", type=float, default=0.0, help="Score mínimo sugerido [0-100] (default: 0).")
        parser.add_argument(
            "--only-suggested",
            action="store_true",
            default=True,
            help="Incluir solo filas con sugerencia (default: true).",
        )
        parser.add_argument(
            "--include-no-suggested",
            action="store_true",
            help="Incluye filas sin sugerencia (sobrescribe --only-suggested).",
        )
        parser.add_argument(
            "--sort-by",
            default="sources_active",
            choices=[
                "sources_active",
                "total_count",
                "score_max",
                "point_count",
                "almacen_count",
                "receta_count",
                "nombre_muestra",
                "nombre_normalizado",
            ],
            help="Campo de ordenamiento (default: sources_active).",
        )
        parser.add_argument(
            "--sort-dir",
            default="desc",
            choices=["asc", "desc"],
            help="Dirección de ordenamiento (default: desc).",
        )
        parser.add_argument(
            "--nombre",
            action="append",
            default=[],
            help="Nombre explícito a procesar (se puede repetir).",
        )
        parser.add_argument(
            "--output-dir",
            default="logs",
            help="Carpeta para reportes (default: logs).",
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica cambios (crear/actualizar aliases y resolver Point/Recetas).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Ejecuta solo simulación (default implícito si no usas --apply).",
        )

    def handle(self, *args, **options):
        runs_to_scan = max(1, int(options["runs"]))
        limit = max(1, min(2000, int(options["limit"])))
        offset = max(0, int(options["offset"]))
        q = str(options["q"] or "").strip()
        q_norm = normalizar_nombre(q)
        source = str(options["source"] or "TODOS").strip().upper()
        point_tipo = str(options["point_tipo"] or PointPendingMatch.TIPO_INSUMO).strip().upper()
        min_sources = max(1, min(3, int(options["min_sources"])))
        score_min = max(0.0, min(100.0, float(options["score_min"] or 0.0)))
        only_suggested = bool(options.get("only_suggested", True))
        if bool(options.get("include_no_suggested")):
            only_suggested = False
        sort_by = str(options["sort_by"])
        sort_dir = str(options["sort_dir"])
        selected_norms = {
            normalizar_nombre(str(name or "").strip())
            for name in (options.get("nombre") or [])
            if normalizar_nombre(str(name or "").strip())
        }
        dry_run = bool(options.get("dry_run")) or not bool(options.get("apply"))

        output_dir = Path(str(options["output_dir"])).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        point_tipos_filter = None if point_tipo in {"TODOS", "ALL"} else [point_tipo]
        allowed_sort = {
            "sources_active": lambda row: int(row.get("sources_active") or 0),
            "total_count": lambda row: int(row.get("total_count") or 0),
            "score_max": lambda row: float(row.get("score_max") or 0.0),
            "point_count": lambda row: int(row.get("point_count") or 0),
            "almacen_count": lambda row: int(row.get("almacen_count") or 0),
            "receta_count": lambda row: int(row.get("receta_count") or 0),
            "nombre_muestra": lambda row: str(row.get("nombre_muestra") or "").lower(),
            "nombre_normalizado": lambda row: str(row.get("nombre_normalizado") or "").lower(),
        }

        pending_rows: list[dict] = []
        sync_runs = list(
            AlmacenSyncRun.objects.only("id", "started_at", "pending_preview")
            .order_by("-started_at")[:runs_to_scan]
        )
        for run in sync_runs:
            for row in run.pending_preview or []:
                nombre_origen = str((row or {}).get("nombre_origen") or "").strip()
                if not nombre_origen:
                    continue
                pending_rows.append(
                    {
                        "nombre_origen": nombre_origen,
                        "nombre_normalizado": str((row or {}).get("nombre_normalizado") or normalizar_nombre(nombre_origen)),
                        "sugerencia": str((row or {}).get("suggestion") or ""),
                        "score": float((row or {}).get("score") or 0),
                        "source": str((row or {}).get("fuente") or "ALMACEN"),
                    }
                )

        pending_grouped = _build_pending_grouped(pending_rows)
        unified_rows, point_unmatched_count, receta_pending_lines = _build_cross_unified_rows(
            pending_grouped,
            point_tipos=point_tipos_filter,
        )
        filtered_rows = _apply_cross_filters(
            unified_rows,
            cross_q_norm=q_norm,
            cross_only_suggested=only_suggested,
            cross_min_sources=min_sources,
            cross_score_min=score_min,
        )

        if source == "ALMACEN":
            filtered_rows = [row for row in filtered_rows if int(row.get("almacen_count") or 0) > 0]
        elif source == "POINT":
            filtered_rows = [row for row in filtered_rows if int(row.get("point_count") or 0) > 0]
        elif source == "RECETAS":
            filtered_rows = [row for row in filtered_rows if int(row.get("receta_count") or 0) > 0]

        if selected_norms:
            filtered_rows = [
                row for row in filtered_rows if (row.get("nombre_normalizado") or "") in selected_norms
            ]

        sort_key = allowed_sort[sort_by]
        reverse = sort_dir == "desc"
        filtered_rows = sorted(
            filtered_rows,
            key=lambda row: (sort_key(row), str(row.get("nombre_muestra") or "").lower()),
            reverse=reverse,
        )
        rows_to_process = filtered_rows[offset : offset + limit]

        processed = 0
        resolved = 0
        created_aliases = 0
        updated_aliases = 0
        preview_create_aliases = 0
        preview_update_aliases = 0
        unchanged = 0
        skipped_no_suggestion = 0
        skipped_no_target = 0
        point_resolved_total = 0
        recetas_resolved_total = 0
        preview_actions: list[dict] = []

        write_context = nullcontext() if dry_run else transaction.atomic()

        with write_context:
            for row in rows_to_process:
                processed += 1
                alias_name = str(row.get("nombre_muestra") or "").strip()
                alias_norm = normalizar_nombre(alias_name)
                suggestion_name = str(row.get("suggestion") or "").strip()
                suggestion_norm = normalizar_nombre(suggestion_name)

                if not suggestion_norm:
                    skipped_no_suggestion += 1
                    preview_actions.append(
                        {
                            "nombre_muestra": alias_name,
                            "sugerencia": suggestion_name,
                            "action": "skip_no_suggestion",
                            "sources_active": int(row.get("sources_active") or 0),
                            "total_count": int(row.get("total_count") or 0),
                        }
                    )
                    continue

                insumo_target = Insumo.objects.filter(activo=True, nombre_normalizado=suggestion_norm).first()
                if not insumo_target:
                    skipped_no_target += 1
                    preview_actions.append(
                        {
                            "nombre_muestra": alias_name,
                            "sugerencia": suggestion_name,
                            "action": "skip_no_target",
                            "sources_active": int(row.get("sources_active") or 0),
                            "total_count": int(row.get("total_count") or 0),
                        }
                    )
                    continue

                action_name = "noop"
                if not alias_norm or alias_norm == insumo_target.nombre_normalizado:
                    unchanged += 1
                    action_name = "noop_same_name"
                else:
                    alias_obj = InsumoAlias.objects.filter(nombre_normalizado=alias_norm).first()
                    if dry_run:
                        if alias_obj is None:
                            action_name = "create_alias"
                            preview_create_aliases += 1
                        elif alias_obj.insumo_id != insumo_target.id or alias_obj.nombre != alias_name[:250]:
                            action_name = "update_alias"
                            preview_update_aliases += 1
                        else:
                            action_name = "noop_alias_exists"
                            unchanged += 1
                    else:
                        if alias_obj is None:
                            InsumoAlias.objects.create(
                                nombre=alias_name[:250],
                                nombre_normalizado=alias_norm,
                                insumo=insumo_target,
                            )
                            created_aliases += 1
                            action_name = "create_alias"
                        else:
                            changed = []
                            if alias_obj.insumo_id != insumo_target.id:
                                alias_obj.insumo = insumo_target
                                changed.append("insumo")
                            if alias_obj.nombre != alias_name[:250]:
                                alias_obj.nombre = alias_name[:250]
                                changed.append("nombre")
                            if changed:
                                alias_obj.save(update_fields=changed)
                                updated_aliases += 1
                                action_name = "update_alias"
                            else:
                                unchanged += 1
                                action_name = "noop_alias_exists"

                if not dry_run:
                    p_count, r_count = _resolve_cross_source_with_alias(alias_name or suggestion_name, insumo_target)
                    point_resolved_total += int(p_count)
                    recetas_resolved_total += int(r_count)

                resolved += 1
                preview_actions.append(
                    {
                        "nombre_muestra": alias_name,
                        "nombre_normalizado": row.get("nombre_normalizado") or "",
                        "sugerencia": suggestion_name,
                        "insumo_target": insumo_target.nombre,
                        "insumo_id": insumo_target.id,
                        "action": action_name,
                        "sources_active": int(row.get("sources_active") or 0),
                        "total_count": int(row.get("total_count") or 0),
                        "score_max": float(row.get("score_max") or 0.0),
                    }
                )

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        mode = "dryrun" if dry_run else "apply"
        report_path = output_dir / f"resolver_pendientes_unificados_{mode}_{ts}.csv"
        self._write_report(report_path, preview_actions)

        self.stdout.write(self.style.SUCCESS("Resolver pendientes unificados"))
        self.stdout.write(f"  - modo: {'DRY-RUN' if dry_run else 'APPLY'}")
        self.stdout.write(f"  - report: {report_path}")
        self.stdout.write(
            "  - filtros: "
            f"runs={runs_to_scan}, limit={limit}, offset={offset}, source={source}, point_tipo={point_tipo}, "
            f"min_sources={min_sources}, score_min={score_min:.2f}, only_suggested={only_suggested}, "
            f"sort_by={sort_by}, sort_dir={sort_dir}, q={q or '-'}"
        )
        self.stdout.write(
            "  - universo: "
            f"raw_almacen={len(pending_rows)}, grouped_almacen={len(pending_grouped)}, "
            f"unified={len(unified_rows)}, filtered={len(filtered_rows)}, page={len(rows_to_process)}, "
            f"point_unmatched={point_unmatched_count}, receta_pending_lines={receta_pending_lines}"
        )
        self.stdout.write(
            "  - resultado: "
            f"procesados={processed}, resueltos={resolved}, "
            f"aliases_creados={created_aliases}, aliases_actualizados={updated_aliases}, "
            f"aliases_creados_preview={preview_create_aliases if dry_run else created_aliases}, "
            f"aliases_actualizados_preview={preview_update_aliases if dry_run else updated_aliases}, "
            f"sin_cambio={unchanged}, sin_sugerencia={skipped_no_suggestion}, sin_insumo_objetivo={skipped_no_target}, "
            f"point_resueltos={point_resolved_total}, recetas_resueltas={recetas_resolved_total}"
        )

    @staticmethod
    def _write_report(path: Path, rows: list[dict]) -> None:
        headers = [
            "nombre_muestra",
            "nombre_normalizado",
            "sugerencia",
            "insumo_target",
            "insumo_id",
            "action",
            "sources_active",
            "total_count",
            "score_max",
        ]
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "nombre_muestra": row.get("nombre_muestra", ""),
                        "nombre_normalizado": row.get("nombre_normalizado", ""),
                        "sugerencia": row.get("sugerencia", ""),
                        "insumo_target": row.get("insumo_target", ""),
                        "insumo_id": row.get("insumo_id", ""),
                        "action": row.get("action", ""),
                        "sources_active": row.get("sources_active", 0),
                        "total_count": row.get("total_count", 0),
                        "score_max": f"{float(row.get('score_max') or 0.0):.2f}",
                    }
                )
