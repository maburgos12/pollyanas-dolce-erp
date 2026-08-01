from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from datetime import date

from django.db import connection, transaction
from django.db.models import Prefetch

from core.audit import log_event
from core.models import Sucursal
from reportes.models import (
    AreaPresupuesto,
    CategoriaGasto,
    LineaPresupuestoMensual,
    ReglaFuenteRubro,
    RubroPresupuesto,
)
from reportes.services_presupuesto_maestro import (
    ensure_master_budget_areas,
    month_periods,
    normalize_area_code,
    normalize_rubro_type,
    normalize_version,
)


SOURCE_AUTO_WITH_DATA = "AUTO_CON_DATOS"
SOURCE_AUTO_WITHOUT_DATA = "AUTO_SIN_DATOS"
SOURCE_MANUAL = "MANUAL"
SOURCE_UNCONFIGURED = "SIN_CONFIGURAR"

SOURCE_STATE_LABELS = {
    SOURCE_AUTO_WITH_DATA: "Automático con datos",
    SOURCE_AUTO_WITHOUT_DATA: "Automático sin datos",
    SOURCE_MANUAL: "Manual",
    SOURCE_UNCONFIGURED: "Sin configurar",
}

RECORD_STATUS_ACTIVE = "ACTIVOS"
RECORD_STATUS_INACTIVE = "INACTIVOS"
RECORD_STATUS_ALL = "TODOS"
RECORD_STATUS_CHOICES = (
    (RECORD_STATUS_ACTIVE, "Activos"),
    (RECORD_STATUS_INACTIVE, "Inactivos"),
    (RECORD_STATUS_ALL, "Todos"),
)

CATEGORY_STATUS_ACTIVE = "ACTIVAS"
CATEGORY_STATUS_INACTIVE = "INACTIVAS"
CATEGORY_STATUS_ALL = "TODAS"
CATEGORY_STATUS_CHOICES = (
    (CATEGORY_STATUS_ACTIVE, "Activas"),
    (CATEGORY_STATUS_INACTIVE, "Inactivas"),
    (CATEGORY_STATUS_ALL, "Todas"),
)

PRESERVED_ACRONYMS = {
    "IMSS",
    "IVA",
    "ISR",
    "POS",
    "CEDIS",
    "CAPEX",
    "CFDI",
    "SAT",
    "ERP",
    "RRHH",
    "CFE",
    "PTU",
}

CATEGORY_BUCKETS_BY_LAYER = {
    CategoriaGasto.CAPA_FABRICACION: {
        CategoriaGasto.BUCKET_MANO_OBRA,
        CategoriaGasto.BUCKET_INDIRECTO,
        CategoriaGasto.BUCKET_EMPAQUE,
    },
    CategoriaGasto.CAPA_SUCURSAL: {
        CategoriaGasto.BUCKET_COMERCIAL,
        CategoriaGasto.BUCKET_LOGISTICA,
    },
    CategoriaGasto.CAPA_EMPRESA: {
        CategoriaGasto.BUCKET_CORPORATIVO,
        CategoriaGasto.BUCKET_OTRO,
    },
}


def canonical_catalog_value(value: object) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    without_accents = "".join(char for char in normalized if not unicodedata.combining(char))
    return "".join(char for char in without_accents.casefold() if char.isalnum())


def normalize_display_name(value: object) -> str:
    clean = " ".join(str(value or "").strip().split())
    if not clean:
        raise ValueError("Captura un nombre o concepto.")
    if clean.isupper():
        clean = clean.lower()
    clean = clean[:1].upper() + clean[1:]
    for acronym in PRESERVED_ACRONYMS:
        clean = re.sub(rf"\b{acronym}\b", acronym, clean, flags=re.IGNORECASE)
    return clean


def normalize_account_code(value: object) -> str:
    clean = unicodedata.normalize("NFKC", str(value or "").strip())
    clean = " ".join(clean.split())
    return clean.upper()


def normalize_catalog_code(value: object) -> str:
    clean = unicodedata.normalize("NFKD", str(value or "").strip())
    clean = "".join(char for char in clean if not unicodedata.combining(char))
    clean = re.sub(r"[^A-Za-z0-9]+", "_", clean).strip("_")
    return clean.upper()


def _validate_max_length(value: str, *, label: str, max_length: int) -> None:
    if len(value) > max_length:
        raise ValueError(f"El {label} no puede exceder {max_length} caracteres.")


def _advisory_key(value: str) -> int:
    return int.from_bytes(hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest(), "big", signed=True)


def _lock_catalog_key(value: str) -> None:
    if connection.vendor != "postgresql":
        return
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", [_advisory_key(value)])


def _rubro_duplicate_key(*, area_id: int, concepto: str, codigo_cuenta: str, sucursal_id: int | None) -> str:
    return ":".join(
        (
            str(area_id),
            canonical_catalog_value(concepto),
            canonical_catalog_value(codigo_cuenta),
            str(sucursal_id or 0),
        )
    )


def source_state_for(rubro: RubroPresupuesto, line: LineaPresupuestoMensual | None) -> str:
    rules = [rule for rule in rubro.reglas_fuente.all() if rule.activa]
    if not rules:
        return SOURCE_UNCONFIGURED
    if line and line.fuente_real.startswith("MANUAL:"):
        return SOURCE_MANUAL
    automatic = [rule for rule in rules if rule.tipo_fuente != ReglaFuenteRubro.FUENTE_MANUAL]
    if not automatic:
        return SOURCE_MANUAL
    expected_source = "AUTO:" + "+".join(sorted({rule.tipo_fuente for rule in automatic}))
    metadata = (line.metadata or {}) if line else {}
    if line and line.fuente_real == expected_source and not metadata.get("sin_datos_fuente"):
        return SOURCE_AUTO_WITH_DATA
    return SOURCE_AUTO_WITHOUT_DATA


@dataclass(frozen=True)
class CatalogCreateResult:
    rubro: RubroPresupuesto


class PresupuestoCatalogoService:
    @transaction.atomic
    def create_category(
        self,
        *,
        user,
        codigo: str,
        nombre: str,
        capa_objetivo: str,
        bucket: str,
    ) -> CategoriaGasto:
        normalized_code = normalize_catalog_code(codigo)
        normalized_name = normalize_display_name(nombre)
        if not normalized_code:
            raise ValueError("Captura un código para la categoría.")
        _validate_max_length(
            normalized_code,
            label="código de la categoría",
            max_length=CategoriaGasto._meta.get_field("codigo").max_length,
        )
        _validate_max_length(
            normalized_name,
            label="nombre de la categoría",
            max_length=CategoriaGasto._meta.get_field("nombre").max_length,
        )
        lock_keys = sorted(
            (
                f"categoria:codigo:{canonical_catalog_value(normalized_code)}",
                f"categoria:nombre:{canonical_catalog_value(normalized_name)}",
            )
        )
        for lock_key in lock_keys:
            _lock_catalog_key(lock_key)
        for category in CategoriaGasto.objects.all().only("id", "codigo", "nombre", "activo"):
            if (
                canonical_catalog_value(category.codigo) == canonical_catalog_value(normalized_code)
                or canonical_catalog_value(category.nombre) == canonical_catalog_value(normalized_name)
            ):
                inactive_help = (
                    " Usa el filtro Categorías: Inactivas para localizarla; no se reactivó ni modificó."
                    if not category.activo
                    else ""
                )
                raise ValueError(
                    f"Ya existe la categoría {category.nombre} ({category.codigo}).{inactive_help}"
                )
        valid_layers = {value for value, _ in CategoriaGasto.CAPA_CHOICES}
        valid_buckets = {value for value, _ in CategoriaGasto.BUCKET_CHOICES}
        if capa_objetivo not in valid_layers or bucket not in valid_buckets:
            raise ValueError("Selecciona una capa y un grupo válidos.")
        if bucket not in CATEGORY_BUCKETS_BY_LAYER[capa_objetivo]:
            layer_label = dict(CategoriaGasto.CAPA_CHOICES)[capa_objetivo]
            bucket_label = dict(CategoriaGasto.BUCKET_CHOICES)[bucket]
            raise ValueError(
                f"El grupo {bucket_label} no corresponde a la capa {layer_label}."
            )
        impacts = {
            "impacta_costo_producto": capa_objetivo == CategoriaGasto.CAPA_FABRICACION,
            "impacta_contribucion_sucursal": capa_objetivo == CategoriaGasto.CAPA_SUCURSAL,
            "impacta_utilidad_empresa": True,
        }
        category = CategoriaGasto.objects.create(
            codigo=normalized_code,
            nombre=normalized_name,
            capa_objetivo=capa_objetivo,
            bucket=bucket,
            **impacts,
            metadata={"source": "presupuesto_catalogos_ui"},
        )
        log_event(
            user,
            "presupuesto_catalogo_categoria_creada",
            "reportes.CategoriaGasto",
            str(category.id),
            {
                "codigo": category.codigo,
                "nombre": category.nombre,
                "capa_objetivo": category.capa_objetivo,
                "bucket": category.bucket,
                **impacts,
            },
        )
        return category

    @transaction.atomic
    def create_rubro(
        self,
        *,
        user,
        area_code: str,
        concepto: str,
        tipo: str,
        year: int,
        version: str,
        codigo_cuenta: str = "",
        sucursal_id: int | None = None,
        categoria_id: int | None = None,
        fuente_mode: str = SOURCE_MANUAL,
    ) -> CatalogCreateResult:
        areas = ensure_master_budget_areas()
        normalized_area = normalize_area_code(area_code)
        area = areas.get(normalized_area)
        if area is None:
            raise ValueError("Selecciona un área de presupuesto válida.")
        normalized_concept = normalize_display_name(concepto)
        normalized_account = normalize_account_code(codigo_cuenta)
        _validate_max_length(
            normalized_concept,
            label="concepto del rubro",
            max_length=RubroPresupuesto._meta.get_field("concepto").max_length,
        )
        _validate_max_length(
            normalized_account,
            label="código de cuenta",
            max_length=RubroPresupuesto._meta.get_field("codigo_cuenta").max_length,
        )
        branch = Sucursal.objects.filter(pk=sucursal_id, activa=True).first() if sucursal_id else None
        if sucursal_id and branch is None:
            raise ValueError("Selecciona una sucursal activa.")
        category = CategoriaGasto.objects.filter(pk=categoria_id, activo=True).first() if categoria_id else None
        if categoria_id and category is None:
            raise ValueError("Selecciona una categoría activa.")
        if fuente_mode not in {SOURCE_MANUAL, SOURCE_UNCONFIGURED}:
            raise ValueError("La fuente inicial debe ser Manual o Sin configurar.")

        duplicate_key = _rubro_duplicate_key(
            area_id=area.id,
            concepto=normalized_concept,
            codigo_cuenta=normalized_account,
            sucursal_id=branch.id if branch else None,
        )
        _lock_catalog_key(f"rubro:{duplicate_key}")
        candidates = RubroPresupuesto.objects.filter(area=area, sucursal=branch).only(
            "id", "concepto", "codigo_cuenta", "activo"
        )
        for existing in candidates:
            existing_key = _rubro_duplicate_key(
                area_id=area.id,
                concepto=existing.concepto,
                codigo_cuenta=existing.codigo_cuenta,
                sucursal_id=branch.id if branch else None,
            )
            if existing_key == duplicate_key:
                state = "activo" if existing.activo else "inactivo"
                inactive_help = (
                    " Usa el filtro Estado: Inactivos para localizarlo; no se reactivó ni modificó."
                    if not existing.activo
                    else ""
                )
                raise ValueError(
                    f"Ya existe el rubro {existing.concepto} en esta área, cuenta y sucursal "
                    f"({state}, #{existing.id}).{inactive_help}"
                )

        metadata = {"source": "presupuesto_catalogos_ui"}
        if category:
            metadata["catalog_category_id"] = category.id
            metadata["catalog_category_name"] = category.nombre
        rubro = RubroPresupuesto.objects.create(
            area=area,
            concepto=normalized_concept,
            codigo_cuenta=normalized_account,
            tipo=normalize_rubro_type(tipo, area.codigo),
            sucursal=branch,
            activo=True,
            metadata=metadata,
        )
        for period in month_periods(year):
            LineaPresupuestoMensual.objects.create(
                rubro=rubro,
                periodo=period,
                version=normalize_version(version),
                monto_presupuesto=0,
                metadata={"source": "presupuesto_catalogos_ui"},
            )
        if fuente_mode == SOURCE_MANUAL:
            ReglaFuenteRubro.objects.create(
                rubro=rubro,
                tipo_fuente=ReglaFuenteRubro.FUENTE_MANUAL,
                origen=ReglaFuenteRubro.ORIGEN_ADMIN,
                notas="Alta desde Catálogos de presupuesto",
            )
        log_event(
            user,
            "presupuesto_catalogo_rubro_creado",
            "reportes.RubroPresupuesto",
            str(rubro.id),
            {
                "area": area.codigo,
                "concepto": rubro.concepto,
                "codigo_cuenta": rubro.codigo_cuenta,
                "sucursal_id": rubro.sucursal_id,
                "categoria_id": category.id if category else None,
                "source_state": fuente_mode,
                "year": year,
                "version": normalize_version(version),
            },
        )
        return CatalogCreateResult(rubro=rubro)

    def list_categories(self, record_status: str = CATEGORY_STATUS_ACTIVE):
        record_status = (
            record_status
            if record_status in dict(CATEGORY_STATUS_CHOICES)
            else CATEGORY_STATUS_ACTIVE
        )
        qs = CategoriaGasto.objects.order_by("nombre", "codigo")
        if record_status == CATEGORY_STATUS_ACTIVE:
            qs = qs.filter(activo=True)
        elif record_status == CATEGORY_STATUS_INACTIVE:
            qs = qs.filter(activo=False)
        return qs

    def list_rows(
        self,
        *,
        period: date,
        version: str,
        area_code: str = "",
        category_id: int | None = None,
        source_state: str = "",
        query: str = "",
        record_status: str = RECORD_STATUS_ACTIVE,
    ) -> list[dict[str, object]]:
        line_qs = LineaPresupuestoMensual.objects.filter(
            periodo=period, version=normalize_version(version)
        )
        record_status = record_status if record_status in dict(RECORD_STATUS_CHOICES) else RECORD_STATUS_ACTIVE
        qs = RubroPresupuesto.objects.select_related("area", "sucursal").prefetch_related(
            Prefetch(
                "reglas_fuente",
                queryset=ReglaFuenteRubro.objects.select_related("categoria_gasto").order_by("tipo_fuente", "id"),
            ),
            Prefetch("lineas_mensuales", queryset=line_qs, to_attr="catalog_period_lines"),
        )
        if area_code:
            qs = qs.filter(area__codigo=normalize_area_code(area_code))
        if record_status == RECORD_STATUS_ACTIVE:
            qs = qs.filter(activo=True)
        elif record_status == RECORD_STATUS_INACTIVE:
            qs = qs.filter(activo=False)
        # Las categorías inactivas siguen siendo parte de la jerarquía histórica.
        # Solo se excluyen de selectores que crean relaciones nuevas.
        categories = {category.id: category for category in CategoriaGasto.objects.all()}
        raw_rows: list[dict[str, object]] = []
        duplicate_counts: dict[str, int] = {}
        duplicate_scope = RubroPresupuesto.objects.all().only(
            "id", "area_id", "concepto", "codigo_cuenta", "sucursal_id"
        )
        for candidate in duplicate_scope:
            key = _rubro_duplicate_key(
                area_id=candidate.area_id,
                concepto=candidate.concepto,
                codigo_cuenta=candidate.codigo_cuenta,
                sucursal_id=candidate.sucursal_id,
            )
            duplicate_counts[key] = duplicate_counts.get(key, 0) + 1
        for rubro in qs.order_by("area__orden", "area__nombre", "concepto", "sucursal__codigo", "id"):
            rules = [rule for rule in rubro.reglas_fuente.all() if rule.activa]
            rule_categories = []
            seen_category_ids = set()
            for rule in rules:
                if rule.categoria_gasto_id and rule.categoria_gasto_id not in seen_category_ids:
                    rule_categories.append(rule.categoria_gasto)
                    seen_category_ids.add(rule.categoria_gasto_id)
            metadata_category = categories.get((rubro.metadata or {}).get("catalog_category_id"))
            if metadata_category and metadata_category.id not in seen_category_ids:
                rule_categories.append(metadata_category)
            category = rule_categories[0] if rule_categories else None
            line = rubro.catalog_period_lines[0] if rubro.catalog_period_lines else None
            state = source_state_for(rubro, line)
            duplicate_key = _rubro_duplicate_key(
                area_id=rubro.area_id,
                concepto=rubro.concepto,
                codigo_cuenta=rubro.codigo_cuenta,
                sucursal_id=rubro.sucursal_id,
            )
            labels = [rule.get_tipo_fuente_display() for rule in rules]
            row = {
                "rubro": rubro,
                "category": category,
                "categories": rule_categories,
                "category_ids": {item.id for item in rule_categories},
                "category_label": " · ".join(item.nombre for item in rule_categories) or "Sin categoría",
                "source_state": state,
                "source_state_label": SOURCE_STATE_LABELS[state],
                "source_labels": labels,
                "source_label": ", ".join(labels) if labels else "Sin regla asignada",
                "duplicate_key": duplicate_key,
                "record_status": RECORD_STATUS_ACTIVE if rubro.activo else RECORD_STATUS_INACTIVE,
                "record_status_label": "Activo" if rubro.activo else "Inactivo",
            }
            raw_rows.append(row)

        query_key = canonical_catalog_value(query)
        rows = []
        for row in raw_rows:
            rubro = row["rubro"]
            row["is_duplicate"] = duplicate_counts[row["duplicate_key"]] > 1
            if category_id and category_id not in row["category_ids"]:
                continue
            if source_state and row["source_state"] != source_state:
                continue
            if query_key:
                haystack = " ".join(
                    (
                        rubro.concepto,
                        rubro.codigo_cuenta,
                        rubro.area.nombre,
                        rubro.sucursal.nombre if rubro.sucursal_id else "",
                        row["category_label"],
                        row["source_label"],
                    )
                )
                if query_key not in canonical_catalog_value(haystack):
                    continue
            rows.append(row)
        return rows
