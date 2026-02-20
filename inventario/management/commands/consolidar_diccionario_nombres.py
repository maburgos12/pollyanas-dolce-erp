from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db.models import Count, Q

from inventario.models import AlmacenSyncRun, ExistenciaInsumo, MovimientoInventario
from maestros.models import CostoInsumo, Insumo, InsumoAlias, PointPendingMatch
from recetas.models import LineaReceta
from recetas.utils.matching import match_insumo
from recetas.utils.normalizacion import normalizar_nombre


NON_ACTIONABLE_RECIPE_TOKENS = {
    "presentacion",
    "armado",
    "subtotal",
    "margen",
    "rendimiento",
    "banado",
}


class Command(BaseCommand):
    help = (
        "Consolida nombres entre Almacén/Point/Recetas y genera un diccionario maestro con "
        "pendientes de unificación e inconsistencias."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--output-dir",
            default="logs",
            help="Carpeta de salida para CSV (default: logs)",
        )
        parser.add_argument(
            "--runs-lookback",
            type=int,
            default=20,
            help="Cantidad de corridas de Almacén a revisar para pendientes históricos (default: 20).",
        )
        parser.add_argument(
            "--include-inactive",
            action="store_true",
            help="Incluye insumos inactivos en el diccionario maestro.",
        )
        parser.add_argument(
            "--apply-point-name-aliases",
            action="store_true",
            help=(
                "Crea alias automáticamente desde Insumo.nombre_point cuando difiere del nombre oficial "
                "y no existe conflicto."
            ),
        )

    def handle(self, *args, **options):
        output_dir = Path(str(options["output_dir"])).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        include_inactive = bool(options["include_inactive"])
        runs_lookback = int(options["runs_lookback"])
        apply_point_aliases = bool(options["apply_point_name_aliases"])

        insumos_qs = Insumo.objects.select_related("unidad_base", "proveedor_principal").order_by("nombre")
        if not include_inactive:
            insumos_qs = insumos_qs.filter(activo=True)
        insumos = list(insumos_qs)
        insumo_ids = [i.id for i in insumos]

        alias_stats = self._apply_point_name_aliases(insumos, enabled=apply_point_aliases)
        latest_cost_map = self._latest_cost_map(insumo_ids)
        alias_map = self._alias_map(insumo_ids)
        receta_use_map = {
            row["insumo_id"]: row["total"]
            for row in (
                LineaReceta.objects.exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
                .filter(insumo_id__in=insumo_ids)
                .values("insumo_id")
                .annotate(total=Count("id"))
            )
        }
        mov_use_map = {
            row["insumo_id"]: row["total"]
            for row in MovimientoInventario.objects.filter(insumo_id__in=insumo_ids)
            .values("insumo_id")
            .annotate(total=Count("id"))
        }
        existencia_map = {e.insumo_id: e for e in ExistenciaInsumo.objects.filter(insumo_id__in=insumo_ids)}

        master_rows: list[dict] = []
        inconsistencies: list[dict] = []

        for insumo in insumos:
            cost_data = latest_cost_map.get(insumo.id, {})
            costo = cost_data.get("costo_unitario")
            fecha_costo = cost_data.get("fecha")
            aliases = alias_map.get(insumo.id, [])
            existencia = existencia_map.get(insumo.id)

            master_rows.append(
                {
                    "insumo_id": insumo.id,
                    "activo": "1" if insumo.activo else "0",
                    "nombre_oficial": insumo.nombre,
                    "nombre_normalizado": insumo.nombre_normalizado,
                    "codigo_interno": insumo.codigo or "",
                    "codigo_point": insumo.codigo_point or "",
                    "nombre_point": insumo.nombre_point or "",
                    "unidad_base": insumo.unidad_base.codigo if insumo.unidad_base else "",
                    "proveedor_principal": insumo.proveedor_principal.nombre if insumo.proveedor_principal else "",
                    "aliases_count": len(aliases),
                    "aliases": " | ".join(aliases),
                    "costo_unitario_actual": str(costo or ""),
                    "fecha_costo_actual": str(fecha_costo or ""),
                    "usos_recetas": receta_use_map.get(insumo.id, 0),
                    "movimientos_inventario": mov_use_map.get(insumo.id, 0),
                    "stock_actual": str(existencia.stock_actual) if existencia else "",
                    "punto_reorden": str(existencia.punto_reorden) if existencia else "",
                    "stock_minimo": str(existencia.stock_minimo) if existencia else "",
                    "stock_maximo": str(existencia.stock_maximo) if existencia else "",
                }
            )

            if not insumo.unidad_base_id:
                inconsistencies.append(
                    {
                        "tipo": "INSUMO_SIN_UNIDAD",
                        "insumo_id": insumo.id,
                        "insumo_nombre": insumo.nombre,
                        "detalle": "No tiene unidad base asignada.",
                    }
                )
            has_usage = (
                receta_use_map.get(insumo.id, 0) > 0
                or mov_use_map.get(insumo.id, 0) > 0
                or (existencia is not None and (existencia.stock_actual or 0) > 0)
            )

            if costo is None and has_usage:
                inconsistencies.append(
                    {
                        "tipo": "INSUMO_SIN_COSTO",
                        "insumo_id": insumo.id,
                        "insumo_nombre": insumo.nombre,
                        "detalle": "No tiene costo vigente en CostoInsumo.",
                    }
                )
            if (
                not (insumo.codigo or "").startswith("DERIVADO:")
                and has_usage
                and not (insumo.codigo_point or "").strip()
            ):
                inconsistencies.append(
                    {
                        "tipo": "INSUMO_SIN_CODIGO_POINT",
                        "insumo_id": insumo.id,
                        "insumo_nombre": insumo.nombre,
                        "detalle": "No tiene código Point homologado.",
                    }
                )

        for row in (
            Insumo.objects.values("nombre_normalizado")
            .exclude(nombre_normalizado="")
            .annotate(total=Count("id"))
            .filter(total__gt=1)
        ):
            nombres = list(
                Insumo.objects.filter(nombre_normalizado=row["nombre_normalizado"])
                .order_by("nombre")
                .values_list("nombre", flat=True)
            )
            inconsistencies.append(
                {
                    "tipo": "NOMBRE_NORMALIZADO_DUPLICADO",
                    "insumo_id": "",
                    "insumo_nombre": "",
                    "detalle": f"{row['nombre_normalizado']} -> {', '.join(nombres[:8])}",
                }
            )

        for row in (
            Insumo.objects.values("codigo_point")
            .exclude(codigo_point="")
            .annotate(total=Count("id"))
            .filter(total__gt=1)
        ):
            nombres = list(
                Insumo.objects.filter(codigo_point=row["codigo_point"])
                .order_by("nombre")
                .values_list("nombre", flat=True)
            )
            inconsistencies.append(
                {
                    "tipo": "CODIGO_POINT_DUPLICADO",
                    "insumo_id": "",
                    "insumo_nombre": "",
                    "detalle": f"{row['codigo_point']} -> {', '.join(nombres[:8])}",
                }
            )

        for conflict in alias_stats["conflicts"]:
            inconsistencies.append(
                {
                    "tipo": "ALIAS_POINT_CONFLICTO",
                    "insumo_id": conflict["insumo_id"],
                    "insumo_nombre": conflict["insumo_nombre"],
                    "detalle": (
                        f"Alias '{conflict['alias']}' ya ligado a '{conflict['current_target']}' "
                        f"(id {conflict['current_target_id']})."
                    ),
                }
            )

        pending_rows = self._pending_unification_rows(runs_lookback=runs_lookback)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_master = output_dir / f"diccionario_maestro_nombres_{ts}.csv"
        path_pending = output_dir / f"pendientes_unificacion_nombres_{ts}.csv"
        path_issues = output_dir / f"inconsistencias_diccionario_{ts}.csv"

        self._write_csv(
            path_master,
            master_rows,
            [
                "insumo_id",
                "activo",
                "nombre_oficial",
                "nombre_normalizado",
                "codigo_interno",
                "codigo_point",
                "nombre_point",
                "unidad_base",
                "proveedor_principal",
                "aliases_count",
                "aliases",
                "costo_unitario_actual",
                "fecha_costo_actual",
                "usos_recetas",
                "movimientos_inventario",
                "stock_actual",
                "punto_reorden",
                "stock_minimo",
                "stock_maximo",
            ],
        )
        self._write_csv(
            path_pending,
            pending_rows,
            [
                "fuente",
                "nombre_origen",
                "nombre_normalizado",
                "frecuencia",
                "score_sugerido",
                "metodo_sugerido",
                "insumo_sugerido",
                "detalle",
            ],
        )
        self._write_csv(
            path_issues,
            inconsistencies,
            ["tipo", "insumo_id", "insumo_nombre", "detalle"],
        )

        self.stdout.write(self.style.SUCCESS("Consolidación de diccionario completada"))
        self.stdout.write(f"  - insumos en diccionario: {len(master_rows)}")
        self.stdout.write(f"  - pendientes unificación: {len(pending_rows)}")
        self.stdout.write(f"  - inconsistencias: {len(inconsistencies)}")
        self.stdout.write(
            f"  - aliases desde nombre_point: creados={alias_stats['created']} "
            f"existentes={alias_stats['existing']} conflictos={len(alias_stats['conflicts'])}"
        )
        self.stdout.write(f"  - reporte maestro: {path_master}")
        self.stdout.write(f"  - reporte pendientes: {path_pending}")
        self.stdout.write(f"  - reporte inconsistencias: {path_issues}")

    def _apply_point_name_aliases(self, insumos: list[Insumo], *, enabled: bool) -> dict:
        created = 0
        existing = 0
        conflicts: list[dict] = []

        for insumo in insumos:
            point_name = (insumo.nombre_point or "").strip()
            if not point_name:
                continue
            alias_norm = normalizar_nombre(point_name)
            if not alias_norm or alias_norm == (insumo.nombre_normalizado or ""):
                continue

            alias = InsumoAlias.objects.filter(nombre_normalizado=alias_norm).select_related("insumo").first()
            if alias:
                existing += 1
                if alias.insumo_id != insumo.id:
                    conflicts.append(
                        {
                            "insumo_id": insumo.id,
                            "insumo_nombre": insumo.nombre,
                            "alias": point_name,
                            "current_target_id": alias.insumo_id,
                            "current_target": alias.insumo.nombre,
                        }
                    )
                continue

            if enabled:
                InsumoAlias.objects.create(
                    nombre=point_name[:250],
                    nombre_normalizado=alias_norm,
                    insumo=insumo,
                )
                created += 1

        return {"created": created, "existing": existing, "conflicts": conflicts}

    def _latest_cost_map(self, insumo_ids: list[int]) -> dict[int, dict]:
        latest: dict[int, dict] = {}
        if not insumo_ids:
            return latest

        rows = (
            CostoInsumo.objects.filter(insumo_id__in=insumo_ids)
            .order_by("insumo_id", "-fecha", "-id")
            .values("insumo_id", "costo_unitario", "fecha")
        )
        for row in rows:
            iid = row["insumo_id"]
            if iid not in latest:
                latest[iid] = {
                    "costo_unitario": row.get("costo_unitario"),
                    "fecha": row.get("fecha"),
                }
        return latest

    def _alias_map(self, insumo_ids: list[int]) -> dict[int, list[str]]:
        data: dict[int, list[str]] = defaultdict(list)
        if not insumo_ids:
            return data
        for row in (
            InsumoAlias.objects.filter(insumo_id__in=insumo_ids)
            .order_by("nombre")
            .values("insumo_id", "nombre")
        ):
            data[row["insumo_id"]].append(row["nombre"])
        return data

    def _pending_unification_rows(self, *, runs_lookback: int) -> list[dict]:
        rows: list[dict] = []

        # 1) Pendientes actuales de Point.
        for p in PointPendingMatch.objects.all().order_by("-fuzzy_score", "point_nombre"):
            rows.append(
                {
                    "fuente": f"POINT_{p.tipo}",
                    "nombre_origen": p.point_nombre,
                    "nombre_normalizado": normalizar_nombre(p.point_nombre or ""),
                    "frecuencia": 1,
                    "score_sugerido": f"{p.fuzzy_score:.1f}",
                    "metodo_sugerido": p.method,
                    "insumo_sugerido": p.fuzzy_sugerencia or "",
                    "detalle": f"codigo={p.point_codigo}",
                }
            )

        # 2) Pendientes recientes de almacén guardados en sync runs.
        agg: dict[tuple[str, str], dict] = {}
        runs = AlmacenSyncRun.objects.only("pending_preview").order_by("-started_at")[:runs_lookback]
        for run in runs:
            pending = list(getattr(run, "pending_preview", []) or [])
            for item in pending:
                raw_name = str((item or {}).get("nombre_origen") or "").strip()
                if not raw_name:
                    continue
                source = str((item or {}).get("source") or "ALMACEN")
                norm = str((item or {}).get("nombre_normalizado") or normalizar_nombre(raw_name))
                key = (source, norm)
                bucket = agg.setdefault(
                    key,
                    {
                        "fuente": f"ALMACEN_{source.upper()}",
                        "nombre_origen": raw_name,
                        "nombre_normalizado": norm,
                        "frecuencia": 0,
                        "score_sugerido": 0.0,
                        "metodo_sugerido": "",
                        "insumo_sugerido": "",
                        "detalle": "",
                    },
                )
                bucket["frecuencia"] += 1
                score = float((item or {}).get("score") or 0)
                if score > float(bucket["score_sugerido"]):
                    bucket["score_sugerido"] = score
                    bucket["insumo_sugerido"] = str((item or {}).get("sugerencia") or "")
                    bucket["metodo_sugerido"] = str((item or {}).get("metodo") or "")

        rows.extend(agg.values())

        # 3) Líneas de receta que siguen sin resolver.
        receta_pending = (
            LineaReceta.objects.exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
            .filter(Q(insumo__isnull=True) | Q(match_status=LineaReceta.STATUS_NEEDS_REVIEW) | Q(match_status=LineaReceta.STATUS_REJECTED))
            .values("insumo_texto")
            .annotate(freq=Count("id"))
            .order_by("-freq")
        )
        for row in receta_pending:
            raw_name = (row.get("insumo_texto") or "").strip()
            if not raw_name:
                continue
            norm_name = normalizar_nombre(raw_name)
            if norm_name in NON_ACTIONABLE_RECIPE_TOKENS:
                continue
            sugerido, score, method = match_insumo(raw_name)
            rows.append(
                {
                    "fuente": "RECETAS",
                    "nombre_origen": raw_name,
                    "nombre_normalizado": norm_name,
                    "frecuencia": row.get("freq", 0),
                    "score_sugerido": f"{score:.1f}",
                    "metodo_sugerido": method,
                    "insumo_sugerido": sugerido.nombre if sugerido else "",
                    "detalle": "LineaReceta sin match definitivo",
                }
            )

        rows.sort(key=lambda x: (str(x.get("fuente", "")), -int(x.get("frecuencia", 0)), str(x.get("nombre_normalizado", ""))))
        for row in rows:
            score = row.get("score_sugerido", "")
            if isinstance(score, float):
                row["score_sugerido"] = f"{score:.1f}"
        return rows

    def _write_csv(self, filepath: Path, rows: list[dict], headers: list[str]) -> None:
        with filepath.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                writer.writerow({h: row.get(h, "") for h in headers})
