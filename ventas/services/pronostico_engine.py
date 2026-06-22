from __future__ import annotations

import math
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
import pandas as pd
from django.db.models import Avg, Max, Min, Q, Sum

from core.models import Sucursal
from pos_bridge.models import PointProduct, PointSalesDailyProductFact
from recetas.models import Receta

try:
    from prophet import Prophet

    PROPHET_AVAILABLE = True
except ImportError:  # pragma: no cover - optional production dependency.
    PROPHET_AVAILABLE = False

try:
    from statsmodels.tsa.exponential_smoothing.ets import ETSModel
    from statsmodels.tsa.seasonal import STL
except ImportError:  # pragma: no cover - production dependency is installed in the container.
    ETSModel = None
    STL = None


HISTORY_START = date(2022, 1, 1)
MIN_MODEL_OBSERVATIONS = 30
SPECIAL_RATIO_MIN = 0.94
SPECIAL_RATIO_MAX = 1.15
SPECIAL_CONTEXT_YEAR_WEIGHTS = (
    Decimal("0.55"),
    Decimal("0.30"),
    Decimal("0.15"),
)
FATHER_DAY_NAME = "Día del Padre"
FECHAS_ESPECIALES_FIJAS = {
    (1, 6): "Reyes",
    (2, 14): "San Valentín",
    (4, 30): "Día del Niño",
    (5, 10): "Día de las Madres",
    (10, 31): "Halloween",
    (11, 2): "Día de Muertos",
    (12, 24): "Nochebuena",
    (12, 25): "Navidad",
    (12, 31): "Año Nuevo",
}
FECHAS_ESPECIALES = {**FECHAS_ESPECIALES_FIJAS, (6, 21): FATHER_DAY_NAME}
FESTIVOS_POLLYANAS = pd.DataFrame(
    {
        "holiday": ["dia_madres"] * 3
        + ["dia_nino"] * 3
        + ["dia_padre"] * 3
        + ["navidad"] * 3
        + ["nochebuena"] * 3
        + ["año_nuevo"] * 3
        + ["reyes"] * 3
        + ["san_valentin"] * 3
        + ["halloween"] * 3
        + ["dia_muertos"] * 3,
        "ds": pd.to_datetime(
            [
                "2024-05-12",
                "2025-05-10",
                "2026-05-10",
                "2024-04-30",
                "2025-04-30",
                "2026-04-30",
                "2024-06-16",
                "2025-06-15",
                "2026-06-21",
                "2024-12-25",
                "2025-12-25",
                "2026-12-25",
                "2024-12-24",
                "2025-12-24",
                "2026-12-24",
                "2024-12-31",
                "2025-12-31",
                "2026-12-31",
                "2024-01-06",
                "2025-01-06",
                "2026-01-06",
                "2024-02-14",
                "2025-02-14",
                "2026-02-14",
                "2024-10-31",
                "2025-10-31",
                "2026-10-31",
                "2024-11-02",
                "2025-11-02",
                "2026-11-02",
            ]
        ),
        "lower_window": [0] * 30,
        "upper_window": [1] * 30,
    }
)
ORDEN_CATEGORIAS = [
    "Bollo",
    "Empanadas",
    "Galletas",
    "Cheesecake",
    "Individual",
    "Pastel Grande",
    "Pastel Mediano",
    "Pastel Chico",
    "Pastel Mini",
    "Rebanada",
    "Pay Grande",
    "Pay Mediano",
    "Vasos Preparados Grande",
    "Otros postres",
    "Accesorios de repostería",
    "Alegría",
    "Café",
    "Chico",
    "Clarita",
    "Coca-cola",
    "D-rigaldi",
    "Glow",
    "Grande",
    "Granmark",
    "Hielo y agua mar de cortéz",
    "Industrias lec",
    "Letrero B",
    "Letreros",
    "Media Plancha",
    "Mediano",
    "Pillines",
    "Plásticos",
    "REGALOS",
    "Rosca",
    "San Valentín",
    "TE",
    "Vaso Preparado Mini",
    "Vasos Grande",
    "Vasos Mini",
    "Vela Sparklers",
    "Velas Sparklers",
    "Viva party",
    "Xtudio",
]
CATEGORY_INDEX = {category: index for index, category in enumerate(ORDEN_CATEGORIAS)}
WEEKDAYS_ES = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
MONTHS_ES = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

FORECASTABLE_TERMS = (
    "bollo",
    "empanada",
    "galleta",
    "cheesecake",
    "pastel",
    "pay",
    "vaso",
    "fresas con crema",
)
EXCLUDED_TERMS = (
    "sobre pedido",
    "sp ",
    " sp",
    "topping",
    "vela",
    "velas",
    "caja ",
    "servicio",
    "domicilio",
    "extra ",
    "letrero",
    "empaque",
    "coca-cola",
    "cafe",
    "starbucks",
    "encendedor",
    "tarjeta",
)


def _date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _ceil(value: Decimal | float) -> int:
    numeric = float(value)
    return int(math.ceil(numeric)) if numeric > 0 else 0


def _third_sunday_of_june(year: int) -> date:
    day = date(year, 6, 1)
    sundays_seen = 0
    while True:
        if day.weekday() == 6:
            sundays_seen += 1
            if sundays_seen == 3:
                return day
        day += timedelta(days=1)


def _is_fathers_day(value: date) -> bool:
    return value == _third_sunday_of_june(value.year)


def _special_day_name(value: date) -> str:
    if _is_fathers_day(value):
        return FATHER_DAY_NAME
    return FECHAS_ESPECIALES_FIJAS.get((value.month, value.day), "")


def _same_special_day_in_year(anchor_day: date, year: int) -> date | None:
    special_name = _special_day_name(anchor_day)
    if not special_name:
        return None
    if special_name == FATHER_DAY_NAME:
        return _third_sunday_of_june(year)
    try:
        return date(year, anchor_day.month, anchor_day.day)
    except ValueError:
        return None


def _special_anchor_days(days: list[date]) -> list[date]:
    return [day for day in days if _special_day_name(day)]


def _special_context_anchor(target_day: date, days: list[date]) -> date | None:
    anchors = _special_anchor_days(days)
    if not anchors:
        return None
    anchor = min(anchors, key=lambda value: abs((target_day - value).days))
    if abs((target_day - anchor).days) <= 3:
        return anchor
    return None


def _previous_special_context_day(target_day: date, days: list[date]) -> date | None:
    anchor = _special_context_anchor(target_day, days)
    if not anchor:
        return None
    previous_anchor = _same_special_day_in_year(anchor, anchor.year - 1)
    if not previous_anchor:
        return None
    return previous_anchor + (target_day - anchor)


def _special_context_comparable_days(target_day: date, days: list[date], years_back: int = 3) -> list[date]:
    anchor = _special_context_anchor(target_day, days)
    if not anchor:
        return []
    offset = target_day - anchor
    comparables = []
    for previous_year in range(anchor.year - 1, anchor.year - years_back - 1, -1):
        previous_anchor = _same_special_day_in_year(anchor, previous_year)
        if previous_anchor:
            comparables.append(previous_anchor + offset)
    return comparables


def _event_context_weighted_base(
    history: dict[date, Decimal],
    comparable_days: list[date],
) -> tuple[Decimal, list[dict]]:
    weighted_sum = Decimal("0")
    used_weight = Decimal("0")
    observations = []
    for index, comparable_day in enumerate(comparable_days):
        qty = history.get(comparable_day, Decimal("0"))
        if qty <= 0:
            continue
        weight = SPECIAL_CONTEXT_YEAR_WEIGHTS[min(index, len(SPECIAL_CONTEXT_YEAR_WEIGHTS) - 1)]
        weighted_sum += qty * weight
        used_weight += weight
        observations.append({"fecha": comparable_day, "cantidad": qty, "peso": weight})
    if used_weight <= 0:
        return Decimal("0"), observations
    return weighted_sum / used_weight, observations


def _event_relation_label(target_day: date, anchor_day: date) -> str:
    event_name = _special_day_name(anchor_day)
    offset = (target_day - anchor_day).days
    if offset == 0:
        return event_name
    if offset < 0:
        days = abs(offset)
        return f"{days} día{'s' if days != 1 else ''} antes de {event_name}"
    return f"{offset} día{'s' if offset != 1 else ''} después de {event_name}"


def _special_context_explanations(days: list[date]) -> list[dict]:
    explanations = []
    for target_day in days:
        anchor = _special_context_anchor(target_day, days)
        if not anchor:
            continue
        comparables = _special_context_comparable_days(target_day, days)
        explanations.append(
            {
                "fecha": target_day,
                "fecha_iso": target_day.isoformat(),
                "fecha_label": _date_label(target_day),
                "evento": _special_day_name(anchor),
                "fecha_evento": anchor.isoformat(),
                "fecha_evento_label": _date_label(anchor),
                "relacion_evento": _event_relation_label(target_day, anchor),
                "comparables": [
                    {
                        "fecha": comparable_day,
                        "fecha_iso": comparable_day.isoformat(),
                        "fecha_label": _date_label(comparable_day),
                        "anio": comparable_day.year,
                    }
                    for comparable_day in comparables
                ],
            }
        )
    return explanations


def _is_special_context_day(target_day: date) -> bool:
    return (
        bool(_special_day_name(target_day))
        or bool(_special_day_name(target_day + timedelta(days=1)))
        or bool(_special_day_name(target_day - timedelta(days=1)))
    )


def _calcular_producto_prophet(serie_df: pd.DataFrame, fechas_rango: list[date], festivos_df: pd.DataFrame) -> dict:
    logging.getLogger("prophet").setLevel(logging.WARNING)
    logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

    clean_df = serie_df[["ds", "y"]].copy()
    clean_df["ds"] = pd.to_datetime(clean_df["ds"])
    clean_df["y"] = pd.to_numeric(clean_df["y"], errors="coerce").fillna(0.0).clip(lower=0.0)

    m = Prophet(
        holidays=festivos_df,
        seasonality_mode="multiplicative",
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        interval_width=0.90,
        changepoint_prior_scale=0.05,
    )
    m.add_country_holidays(country_name="MX")
    m.fit(clean_df, iter=300)

    previous_event_days: dict[date, date] = {}
    for target_day in fechas_rango:
        previous_day = _previous_special_context_day(target_day, fechas_rango)
        if previous_day:
            previous_event_days[target_day] = previous_day

    prediction_days = list(dict.fromkeys([*fechas_rango, *previous_event_days.values()]))
    future = pd.DataFrame({"ds": pd.to_datetime(prediction_days)})
    forecast = m.predict(future)
    forecast_by_day = {
        row.ds.date(): {
            "yhat": float(row.yhat),
            "yhat_lower": float(row.yhat_lower),
            "yhat_upper": float(row.yhat_upper),
        }
        for row in forecast.itertuples()
    }
    history_by_day = {
        row.ds.date(): float(row.y)
        for row in clean_df.itertuples()
    }

    recomendado = []
    conservador = []
    agresivo = []
    confianzas = []
    used_hybrid = False
    for target_day in fechas_rango:
        prediction = forecast_by_day.get(target_day, {"yhat": 0.0, "yhat_lower": 0.0, "yhat_upper": 0.0})
        yhat = prediction["yhat"]
        lower = prediction["yhat_lower"]
        upper = prediction["yhat_upper"]

        previous_event_day = previous_event_days.get(target_day)
        event_base = history_by_day.get(previous_event_day, 0.0) if previous_event_day else 0.0
        previous_prediction = forecast_by_day.get(previous_event_day) if previous_event_day else None
        previous_yhat = previous_prediction["yhat"] if previous_prediction else 0.0
        if event_base > 0 and previous_yhat > 0:
            ratio = _clamp(yhat / previous_yhat, 0.85, 1.20)
            yhat = event_base * ratio
            lower = yhat * 0.90
            upper = yhat * 1.12
            used_hybrid = True

        recomendado.append(max(0, math.ceil(float(yhat))))
        conservador.append(max(0, math.ceil(float(lower))))
        agresivo.append(max(0, math.ceil(float(upper))))
        interval_confidence = 1 - ((float(upper) - float(lower)) / ((2 * float(yhat)) + 1))
        confianzas.append(_clamp(interval_confidence, 0.50, 0.95))

    return {
        "recomendado": recomendado,
        "conservador": conservador,
        "agresivo": agresivo,
        "confianza": round(sum(confianzas) / len(confianzas), 3) if confianzas else 0.0,
        "metodo": "prophet+hibrido-fecha-especial" if used_hybrid else "prophet",
    }


def _calcular_serie_ets(serie_df: pd.DataFrame, fechas_rango: list[date]) -> dict:
    if not fechas_rango:
        return {"recomendado": [], "conservador": [], "agresivo": [], "confianza": 0.0, "metodo": "sin-fechas"}

    clean_df = serie_df[["ds", "y"]].copy()
    clean_df["ds"] = pd.to_datetime(clean_df["ds"])
    clean_df["y"] = pd.to_numeric(clean_df["y"], errors="coerce").fillna(0.0).clip(lower=0.0)
    clean_df = clean_df.sort_values("ds")
    if clean_df.empty:
        empty = [0] * len(fechas_rango)
        return {"recomendado": empty, "conservador": empty, "agresivo": empty, "confianza": 0.0, "metodo": "sin-datos"}

    series = pd.Series(clean_df["y"].to_numpy(dtype=float), index=pd.DatetimeIndex(clean_df["ds"]))
    history_end = clean_df["ds"].max().date()
    horizon = max(1, (max(fechas_rango) - history_end).days)
    lower, forecast, upper, confidence, method = _fit_ets(series, horizon)

    recomendado = []
    conservador = []
    agresivo = []
    for day in fechas_rango:
        forecast_index = max(0, (day - history_end).days - 1)
        if forecast_index < len(forecast):
            model_value = float(forecast[forecast_index])
            lower_value = float(lower[forecast_index]) if forecast_index < len(lower) else model_value * 0.90
            upper_value = float(upper[forecast_index]) if forecast_index < len(upper) else model_value * 1.12
        else:
            fallback = float(_simple_average_forecast(series, 1).iloc[0])
            model_value = fallback
            lower_value = fallback * 0.90
            upper_value = fallback * 1.12
        recomendado.append(_ceil(model_value))
        conservador.append(_ceil(lower_value))
        agresivo.append(_ceil(upper_value))

    n_dias_historia = int((clean_df["y"] > 0).sum())
    return {
        "recomendado": recomendado,
        "conservador": conservador,
        "agresivo": agresivo,
        "confianza": min(0.75, 0.50 + n_dias_historia / 500),
        "metodo": method,
    }


def _calcular_serie(serie_df: pd.DataFrame, fechas_rango: list[date]) -> dict:
    if PROPHET_AVAILABLE and len(serie_df) >= 60:
        try:
            return _calcular_producto_prophet(serie_df, fechas_rango, FESTIVOS_POLLYANAS)
        except Exception:
            pass
    return _calcular_serie_ets(serie_df, fechas_rango)


def _clamp(value: float, low: float, high: float) -> float:
    return min(high, max(low, value))


def _money(value: Decimal) -> Decimal:
    return _decimal(value).quantize(Decimal("0.01"))


def _date_label(value: date) -> str:
    return f"{WEEKDAYS_ES[value.weekday()]} {value.day} {MONTHS_ES[value.month - 1]}"


def _period_label(start: date, end: date) -> str:
    return f"{start.day} {MONTHS_ES[start.month - 1].lower()} {start.year} - {end.day} {MONTHS_ES[end.month - 1].lower()} {end.year}"


def _trend_label(factor: float) -> str:
    if factor > 1.10:
        return "sube"
    if factor < 0.90:
        return "baja"
    return "estable"


def _clean_label(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def _category_sort_key(category: str | None) -> tuple[int, int, str]:
    label = _clean_label(category)
    if label in CATEGORY_INDEX:
        return (0, CATEGORY_INDEX[label], label.casefold())
    return (1, len(CATEGORY_INDEX), label.casefold())


def _product_sort_key(product: dict) -> tuple[tuple[int, int, str], int, str]:
    return (_category_sort_key(product.get("categoria")), -product["total_piezas"], product["nombre"])


def categoria_producto(
    *,
    point_category: str | None,
    familia: str | None,
    receta_codigo_point: str | None = None,
    category_by_sku: dict[str, str] | None = None,
) -> str:
    codigo_point = _clean_label(receta_codigo_point)
    if codigo_point and category_by_sku:
        category = _clean_label(category_by_sku.get(codigo_point))
        if category:
            return category

    category = _clean_label(point_category)
    if category:
        return category

    fallback = _clean_label(familia)
    return fallback or "Sin categoría"


def _is_forecastable_product(*, nombre: str, familia: str, receta: Receta | None) -> bool:
    haystack = f"{nombre} {familia} {(receta.categoria if receta else '')}".casefold()
    if any(term in haystack for term in EXCLUDED_TERMS):
        return False
    if receta:
        return receta.tipo == Receta.TIPO_PRODUCTO_FINAL
    return any(term in haystack for term in FORECASTABLE_TERMS)


def _empty_result(start_date: date, end_date: date, branch_count: int = 0) -> dict:
    selected_days = list(_date_range(start_date, end_date))
    return {
        "fechas": [day.isoformat() for day in selected_days],
        "fechas_tabla": [{"iso": day.isoformat(), "label": _date_label(day)} for day in selected_days],
        "resumen": {
            "total_piezas": 0,
            "total_ingreso": Decimal("0.00"),
            "dias": len(selected_days),
            "n_productos": 0,
            "productos": 0,
            "n_sucursales": branch_count,
            "sucursales": branch_count,
            "fechas_especiales": _special_days(selected_days),
            "metodo": "sin-datos",
            "confianza_promedio": 0.0,
            "tendencia_reciente": "",
            "comparable": "",
        },
        "por_categoria": [],
        "por_dia": [],
        "por_sucursal": [],
        "por_producto": [],
        "por_producto_familias": [],
    }


def _special_days(days: list[date]) -> list[dict]:
    return [
        {"fecha": day, "fecha_iso": day.isoformat(), "nombre": _special_day_name(day)}
        for day in days
        if _special_day_name(day)
    ]


def _confidence_from_aic(aic: float | None, n_obs: int) -> float:
    if aic is None or not np.isfinite(aic) or n_obs <= 0:
        return 0.55
    aic_per_obs = abs(float(aic)) / max(n_obs, 1)
    return _clamp(1.0 / (1.0 + (aic_per_obs / 100.0)), 0.35, 0.90)


def _event_confidence(years: int) -> float:
    if years >= 3:
        return 0.92
    if years == 2:
        return 0.82
    if years == 1:
        return 0.70
    return 0.50


def _prediction_interval_columns(frame: pd.DataFrame) -> tuple[np.ndarray | None, np.ndarray | None]:
    lower_candidates = ("mean_ci_lower", "pi_lower", "lower PI", "lower", "mean_se")
    upper_candidates = ("mean_ci_upper", "pi_upper", "upper PI", "upper")
    lower = next((frame[column].to_numpy(dtype=float) for column in lower_candidates if column in frame), None)
    upper = next((frame[column].to_numpy(dtype=float) for column in upper_candidates if column in frame), None)
    if lower is not None and upper is not None:
        return lower, upper
    return None, None


def _fit_ets(series: pd.Series, horizon: int) -> tuple[np.ndarray, np.ndarray, np.ndarray, float, str]:
    if ETSModel is None or horizon <= 0 or series.sum() <= 0:
        forecast = _simple_average_forecast(series, horizon).to_numpy(dtype=float)
        return forecast * 0.90, forecast, forecast * 1.12, 0.45, "promedio-simple"

    clean_series = series.astype(float).clip(lower=0)
    try:
        model = ETSModel(
            clean_series,
            error="add",
            trend="add",
            seasonal="add",
            seasonal_periods=7,
            initialization_method="estimated",
        )
        fit = model.fit(disp=False)
        forecast = np.asarray(fit.forecast(horizon), dtype=float)
        lower = None
        upper = None
        try:
            prediction = fit.get_prediction(start=len(clean_series), end=len(clean_series) + horizon - 1)
            lower, upper = _prediction_interval_columns(prediction.summary_frame(alpha=0.10))
        except Exception:
            lower = upper = None
        if lower is None or upper is None:
            lower = forecast * 0.90
            upper = forecast * 1.12
        confidence = _confidence_from_aic(getattr(fit, "aic", None), len(clean_series))
        return np.clip(lower, 0, None), np.clip(forecast, 0, None), np.clip(upper, 0, None), confidence, "ets-estacional"
    except Exception:
        forecast = _simple_average_forecast(series, horizon).to_numpy(dtype=float)
        confidence = min(0.55, len(series[series > 0]) / 90) if len(series) else 0.0
        return forecast * 0.90, forecast, forecast * 1.12, confidence, "promedio-simple"


def _simple_average_forecast(series: pd.Series, horizon: int) -> pd.Series:
    if horizon <= 0:
        return pd.Series(dtype=float)
    if series.empty:
        return pd.Series([0.0] * horizon)

    clean = series.astype(float).clip(lower=0)
    window = clean.tail(min(84, len(clean)))
    fallback = float(window.mean()) if len(window) else 0.0
    values = []
    last_day = clean.index.max().date()
    for offset in range(1, horizon + 1):
        target_day = last_day + timedelta(days=offset)
        same_weekday = window[window.index.weekday == target_day.weekday()]
        value = float(same_weekday.mean()) if len(same_weekday) else fallback
        values.append(max(value, 0.0))
    return pd.Series(values)


def _series_from_history(history: dict[date, Decimal], *, end_date: date) -> pd.Series:
    positive_dates = [day for day, qty in history.items() if qty > 0]
    start_date = min(positive_dates) if positive_dates else end_date
    index = pd.date_range(start=start_date, end=end_date, freq="D")
    values = [float(history.get(day.date(), Decimal("0"))) for day in index]
    return pd.Series(values, index=index)


def _history_dataframe(series: pd.Series, special_days: set[tuple[int, int]]) -> pd.DataFrame:
    frame = pd.DataFrame({"fecha": series.index, "qty": series.to_numpy(dtype=float)})
    frame["dia_semana"] = frame["fecha"].dt.weekday
    frame["semana_año"] = frame["fecha"].dt.isocalendar().week.astype(int)
    frame["es_fecha_especial"] = frame["fecha"].map(
        lambda value: 1 if _special_day_name(value) or (value.month, value.day) in special_days else 0
    )
    frame["nombre_evento"] = frame["fecha"].map(_special_day_name)
    return frame


def _window_average_from_history(history: dict[date, Decimal], start: date, end: date, days: int = 30) -> Decimal:
    total = sum((qty for day, qty in history.items() if start <= day <= end), Decimal("0"))
    return total / Decimal(days)


def _stl_trend_ratio(
    series: pd.Series,
    history: dict[date, Decimal],
    *,
    trend_start: date,
    history_end: date,
) -> float:
    positive_values = series[series > 0]
    alpha = max(float(positive_values.mean()) * 0.1, 1.0) if not positive_values.empty else 1.0
    recent_fallback = _window_average_from_history(history, trend_start, history_end)
    comparable_start = trend_start - timedelta(days=364)
    comparable_end = history_end - timedelta(days=364)
    comparable_fallback = _window_average_from_history(history, comparable_start, comparable_end)

    if STL is None or len(series) < 60 or series.sum() <= 0:
        ratio = (float(recent_fallback) + alpha) / (float(comparable_fallback) + alpha)
        return _clamp(ratio, SPECIAL_RATIO_MIN, SPECIAL_RATIO_MAX)

    try:
        stl_series = series.astype(float).clip(lower=0).tail(450)
        result = STL(stl_series, period=7, robust=True).fit()
        trend = pd.Series(result.trend, index=stl_series.index).replace([np.inf, -np.inf], np.nan).dropna()
        recent_window = trend.loc[pd.Timestamp(trend_start) : pd.Timestamp(history_end)]
        previous_window = trend.loc[pd.Timestamp(comparable_start) : pd.Timestamp(comparable_end)]
        if recent_window.empty:
            recent_window = trend.tail(30)
        if previous_window.empty and len(trend) >= 395:
            previous_window = trend.iloc[-395:-365]
        trend_recent = float(recent_window.mean()) if not recent_window.empty else float(recent_fallback)
        trend_previous = float(previous_window.mean()) if not previous_window.empty else float(comparable_fallback)
        ratio = (trend_recent + alpha) / (trend_previous + alpha)
    except Exception:
        ratio = (float(recent_fallback) + alpha) / (float(comparable_fallback) + alpha)
    return _clamp(ratio, SPECIAL_RATIO_MIN, SPECIAL_RATIO_MAX)


def _event_history(history: dict[date, Decimal], target_day: date) -> tuple[Decimal, list[Decimal]]:
    values = []
    anchor = Decimal("0")
    for previous_year in (target_day.year - 1, target_day.year - 2, target_day.year - 3):
        try:
            event_day = date(previous_year, target_day.month, target_day.day)
        except ValueError:
            continue
        qty = history.get(event_day, Decimal("0"))
        if qty > 0:
            values.append(qty)
            if anchor <= 0:
                anchor = qty
    return anchor, values


def _weighted_insufficient_forecast(
    *,
    history: dict[date, Decimal],
    target_day: date,
    trend_start: date,
    history_end: date,
) -> Decimal:
    recent_avg = _window_average_from_history(history, trend_start, history_end)
    event_base, _values = _event_history(history, target_day)
    if event_base > 0:
        return (event_base * Decimal("0.65")) + (recent_avg * Decimal("1.5") * Decimal("0.35"))
    return recent_avg * Decimal("1.5")


def _special_day_forecast(
    *,
    series: pd.Series,
    history: dict[date, Decimal],
    target_day: date,
    trend_start: date,
    history_end: date,
    sale_days: int,
    trend_ratio: float | None = None,
) -> tuple[Decimal, int, int, float, float, str]:
    if sale_days < MIN_MODEL_OBSERVATIONS:
        forecast = _weighted_insufficient_forecast(
            history=history,
            target_day=target_day,
            trend_start=trend_start,
            history_end=history_end,
        )
        ratio = (
            trend_ratio
            if trend_ratio is not None
            else _stl_trend_ratio(series, history, trend_start=trend_start, history_end=history_end)
        )
        forecast = forecast * Decimal(str(ratio))
        conservador = _ceil(forecast * Decimal("0.92"))
        agresivo = _ceil(forecast * Decimal("1.10"))
        return forecast, conservador, agresivo, ratio, 0.58, "promedio-ponderado+fecha-especial"

    event_base, event_values = _event_history(history, target_day)
    ratio = (
        trend_ratio
        if trend_ratio is not None
        else _stl_trend_ratio(series, history, trend_start=trend_start, history_end=history_end)
    )
    if event_base <= 0:
        forecast = _weighted_insufficient_forecast(
            history=history,
            target_day=target_day,
            trend_start=trend_start,
            history_end=history_end,
        )
        conservador = _ceil(forecast * Decimal("0.92"))
        agresivo = _ceil(forecast * Decimal("1.10"))
        return forecast, conservador, agresivo, ratio, 0.55, "promedio-ponderado+fecha-especial"

    forecast = event_base * Decimal(str(ratio))
    if len(event_values) >= 2:
        std_events = Decimal(str(float(np.std([float(value) for value in event_values]))))
        conservador = _ceil(forecast - (std_events * Decimal("0.5")))
        agresivo = _ceil(forecast + (std_events * Decimal("0.5")))
    else:
        conservador = _ceil(forecast * Decimal("0.92"))
        agresivo = _ceil(forecast * Decimal("1.10"))
    return (
        forecast,
        conservador,
        agresivo,
        ratio,
        _event_confidence(len(event_values)),
        "regresion-estacional+fecha-especial",
    )


def _apply_special_context_forecast(
    forecast_result: dict,
    *,
    series: pd.Series,
    history: dict[date, Decimal],
    selected_days: list[date],
    trend_start: date,
    history_end: date,
) -> dict:
    if not _special_anchor_days(selected_days):
        return forecast_result

    recomendado = list(forecast_result.get("recomendado", [0] * len(selected_days)))
    conservador = list(forecast_result.get("conservador", [0] * len(selected_days)))
    agresivo = list(forecast_result.get("agresivo", [0] * len(selected_days)))
    trend_ratio: float | None = None
    overrides = 0
    max_observations = 0

    for index, target_day in enumerate(selected_days):
        comparable_days = _special_context_comparable_days(target_day, selected_days)
        if not comparable_days:
            continue
        comparable_qty, observations = _event_context_weighted_base(history, comparable_days)
        if comparable_qty <= 0:
            continue
        if trend_ratio is None:
            trend_ratio = _stl_trend_ratio(series, history, trend_start=trend_start, history_end=history_end)
        forecast_qty = comparable_qty * Decimal(str(trend_ratio))
        if index >= len(recomendado):
            recomendado.extend([0] * (index + 1 - len(recomendado)))
        if index >= len(conservador):
            conservador.extend([0] * (index + 1 - len(conservador)))
        if index >= len(agresivo):
            agresivo.extend([0] * (index + 1 - len(agresivo)))
        recomendado[index] = _ceil(forecast_qty)
        conservador[index] = _ceil(forecast_qty * Decimal("0.92"))
        agresivo[index] = _ceil(forecast_qty * Decimal("1.10"))
        overrides += 1
        max_observations = max(max_observations, len(observations))

    if not overrides:
        return forecast_result

    adjusted = {**forecast_result}
    adjusted["recomendado"] = recomendado
    adjusted["conservador"] = conservador
    adjusted["agresivo"] = agresivo
    adjusted["confianza"] = max(float(forecast_result.get("confianza") or 0.0), _event_confidence(max_observations))
    adjusted["metodo"] = "evento-comparable+fecha-especial"
    return adjusted


def _forecastable_queryset(branch_ids: set[int], skus_incluidos: set[str] | None = None):
    forecastable_filter = Q(receta__tipo=Receta.TIPO_PRODUCTO_FINAL)
    for term in FORECASTABLE_TERMS:
        forecastable_filter |= Q(point_product__name__icontains=term) | Q(point_product__category__icontains=term)

    queryset = (
        PointSalesDailyProductFact.objects.filter(
            point_product_id__isnull=False,
            point_product__precio_temporada=False,
            point_product__active=True,
            branch__erp_branch_id__in=branch_ids,
            branch__erp_branch__activa=True,
        )
        .filter(forecastable_filter)
        .exclude(
            Q(point_product__name__icontains="sobre pedido")
            | Q(receta__nombre__icontains="sobre pedido")
            | Q(point_product__name__icontains="sp ")
            | Q(point_product__name__icontains=" sp")
            | Q(receta__nombre__icontains="sp ")
            | Q(receta__nombre__icontains=" sp")
        )
    )
    if skus_incluidos is not None:
        queryset = queryset.filter(point_product__sku__in=skus_incluidos)
    return queryset


def calcular_pronostico(
    fecha_inicio: date,
    fecha_fin: date,
    sucursal_ids: set[int] | list[int] | None = None,
    skus_incluidos: set[str] | list[str] | None = None,
) -> dict:
    selected_days = list(_date_range(fecha_inicio, fecha_fin))
    if not selected_days:
        return _empty_result(fecha_inicio, fecha_fin)
    selected_skus = {str(value).strip() for value in (skus_incluidos or []) if str(value).strip()}
    sku_filter = selected_skus if skus_incluidos is not None else None

    active_branches = Sucursal.objects.filter(activa=True).order_by("nombre")
    active_branch_ids = set(active_branches.values_list("id", flat=True))
    if sucursal_ids:
        branch_ids = {int(value) for value in sucursal_ids} & active_branch_ids
    else:
        branch_ids = set(active_branch_ids)
    if not branch_ids:
        return _empty_result(fecha_inicio, fecha_fin)

    branch_map = {
        branch.id: branch
        for branch in Sucursal.objects.filter(id__in=branch_ids).only("id", "codigo", "nombre").order_by("nombre")
    }
    base_qs = _forecastable_queryset(branch_ids, sku_filter)
    sale_bounds = base_qs.filter(sale_date__lt=fecha_inicio).aggregate(
        min_date=Min("sale_date"),
        max_date=Max("sale_date"),
    )
    latest_sale_date = sale_bounds.get("max_date")
    if not latest_sale_date:
        return _empty_result(fecha_inicio, fecha_fin, len(branch_ids))

    history_end = min(latest_sale_date, fecha_inicio - timedelta(days=1))
    oldest_sale_date = sale_bounds.get("min_date") or HISTORY_START
    trend_start = history_end - timedelta(days=29)
    trend_comparable_start = trend_start - timedelta(days=364)
    trend_comparable_end = history_end - timedelta(days=364)
    context_start = history_end - timedelta(days=89)
    context_comparable_start = context_start - timedelta(days=364)
    context_comparable_end = history_end - timedelta(days=364)
    recent_pair_rows = list(
        base_qs.filter(sale_date__range=(trend_start, history_end))
        .values("point_product_id", "branch__erp_branch_id")
        .annotate(qty=Sum("total_cantidad"))
        .filter(qty__gt=0)
    )
    recent_pairs = {
        (int(row["point_product_id"]), int(row["branch__erp_branch_id"]))
        for row in recent_pair_rows
        if row.get("point_product_id") and row.get("branch__erp_branch_id")
    }
    if not recent_pairs:
        return _empty_result(fecha_inicio, fecha_fin, len(branch_ids))
    recent_product_ids = {product_id for product_id, _branch_id in recent_pairs}

    history_start = oldest_sale_date
    rows = list(
        base_qs.filter(sale_date__range=(history_start, history_end), point_product_id__in=recent_product_ids)
        .values("point_product_id", "branch__erp_branch_id", "sale_date", "receta_id")
        .annotate(qty=Sum("total_cantidad"), revenue=Sum("total_venta_neta"))
        .order_by("point_product_id", "branch__erp_branch_id", "sale_date")
    )
    if not rows:
        return _empty_result(fecha_inicio, fecha_fin, len(branch_ids))

    product_ids = {int(row["point_product_id"]) for row in rows if row.get("point_product_id")}
    product_map = {
        product.id: product
        for product in PointProduct.objects.filter(id__in=product_ids).only("id", "sku", "name", "category", "precio")
    }
    recipe_by_product: dict[int, int] = {}
    recipe_ids = set()
    for row in rows:
        if row.get("receta_id"):
            product_id = int(row["point_product_id"])
            recipe_id = int(row["receta_id"])
            recipe_by_product.setdefault(product_id, recipe_id)
            recipe_ids.add(recipe_id)
    recipe_map = {
        receta.id: receta
        for receta in Receta.objects.filter(id__in=recipe_ids).only("id", "codigo_point", "nombre", "familia", "categoria", "tipo")
    }
    recipe_codes = {_clean_label(receta.codigo_point) for receta in recipe_map.values() if _clean_label(receta.codigo_point)}
    category_by_sku = {
        _clean_label(row["sku"]): _clean_label(row["category"])
        for row in PointProduct.objects.filter(sku__in=recipe_codes)
        .exclude(category="")
        .values("sku", "category")
    }

    histories: dict[tuple[int, int], dict[date, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
    product_histories: dict[int, dict[date, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
    recent_pair_qty: dict[tuple[int, int], Decimal] = defaultdict(Decimal)
    revenue_by_product_recent: dict[int, dict[str, Decimal]] = defaultdict(lambda: {"qty": Decimal("0"), "revenue": Decimal("0")})
    revenue_by_product_all: dict[int, dict[str, Decimal]] = defaultdict(lambda: {"qty": Decimal("0"), "revenue": Decimal("0")})
    for row in rows:
        product_id = int(row["point_product_id"])
        branch_id = int(row["branch__erp_branch_id"])
        if (product_id, branch_id) not in recent_pairs:
            continue
        sale_date = row["sale_date"]
        qty = _decimal(row.get("qty"))
        revenue = _decimal(row.get("revenue"))
        histories[(product_id, branch_id)][sale_date] += qty
        product_histories[product_id][sale_date] += qty
        revenue_by_product_all[product_id]["qty"] += qty
        revenue_by_product_all[product_id]["revenue"] += revenue
        if trend_start <= sale_date <= history_end:
            recent_pair_qty[(product_id, branch_id)] += qty
            revenue_by_product_recent[product_id]["qty"] += qty
            revenue_by_product_recent[product_id]["revenue"] += revenue

    price_by_product: dict[int, Decimal] = {}
    for product_id in product_ids:
        stats = revenue_by_product_recent[product_id]
        if stats["qty"] <= 0:
            stats = revenue_by_product_all[product_id]
        if stats["qty"] > 0 and stats["revenue"] > 0:
            price_by_product[product_id] = _money(stats["revenue"] / stats["qty"])

    for row in (
        base_qs.filter(point_product_id__in=product_ids, point_product__precio__isnull=False, point_product__precio_activo=True)
        .values("point_product_id")
        .annotate(avg_price=Avg("point_product__precio"))
    ):
        product_id = int(row["point_product_id"])
        if product_id not in price_by_product and row.get("avg_price") is not None:
            price_by_product[product_id] = _money(_decimal(row["avg_price"]))

    forecast_horizon = max(1, (fecha_fin - history_end).days)
    product_totals: dict[int, dict] = {}
    branch_products: dict[int, dict[int, dict]] = defaultdict(dict)
    special_context_explanations = _special_context_explanations(selected_days)
    special_context_by_day = {
        item["fecha_iso"]: item
        for item in special_context_explanations
    }
    day_totals: dict[date, dict] = {
        day: {
            "fecha": day,
            "fecha_iso": day.isoformat(),
            "fecha_label": _date_label(day),
            "es_fecha_especial": bool(_special_day_name(day)),
            "nombre_especial": _special_day_name(day),
            "contexto_evento": special_context_by_day.get(day.isoformat()),
            "total_piezas": 0,
            "total_ingreso": Decimal("0.00"),
            "top_productos": [],
        }
        for day in selected_days
    }
    product_day_rank: dict[date, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    method_counts = defaultdict(int)
    confidence_weighted_sum = 0.0
    special_days_in_range = _special_days(selected_days)
    range_has_special = bool(special_days_in_range)
    product_forecasts: dict[int, dict] = {}
    tareas = []
    for product_id in sorted(recent_product_ids):
        history = product_histories.get(product_id)
        if not history:
            continue
        series = _series_from_history(history, end_date=history_end)
        serie_df = pd.DataFrame({"ds": series.index, "y": series.to_numpy(dtype=float)})
        tareas.append((product_id, serie_df, selected_days))

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(_calcular_serie, serie, fechas): key
            for key, serie, fechas in tareas
        }
        task_series = {key: serie for key, serie, _fechas in tareas}
        for future in as_completed(futures):
            key = futures[future]
            try:
                product_forecasts[key] = future.result()
            except Exception:
                product_forecasts[key] = _calcular_serie_ets(task_series[key], selected_days)

    for product_id in sorted(recent_product_ids):
        product = product_map.get(product_id)
        if not product:
            continue
        receta = recipe_map.get(recipe_by_product.get(product_id))
        name = product.name or (receta.nombre if receta else "Producto")
        family = (receta.familia if receta else "") or product.category or "Sin familia"
        if not _is_forecastable_product(nombre=name, familia=family, receta=receta):
            continue

        history = product_histories.get(product_id)
        if not history:
            continue
        series = _series_from_history(history, end_date=history_end)
        forecast_result = product_forecasts.get(product_id)
        if forecast_result is None:
            serie_df = pd.DataFrame({"ds": series.index, "y": series.to_numpy(dtype=float)})
            forecast_result = _calcular_serie(serie_df, selected_days)
        forecast_result = _apply_special_context_forecast(
            forecast_result,
            series=series,
            history=history,
            selected_days=selected_days,
            trend_start=trend_start,
            history_end=history_end,
        )

        category = categoria_producto(
            point_category=product.category,
            familia=receta.familia if receta else family,
            receta_codigo_point=receta.codigo_point if receta else "",
            category_by_sku=category_by_sku,
        )
        price = price_by_product.get(product_id, Decimal("0.00"))
        method = forecast_result.get("metodo") or "sin-modelo"
        confidence = float(forecast_result.get("confianza") or 0.0)

        day_values = []
        total_recommended = 0
        total_conservative = 0
        total_aggressive = 0
        total_income = Decimal("0.00")
        for index, day in enumerate(selected_days):
            recomendado_values = forecast_result.get("recomendado", [0] * len(selected_days))
            conservador_values = forecast_result.get("conservador", [0] * len(selected_days))
            agresivo_values = forecast_result.get("agresivo", [0] * len(selected_days))
            recomendado = int(recomendado_values[index] or 0) if index < len(recomendado_values) else 0
            conservador = int(conservador_values[index] or 0) if index < len(conservador_values) else 0
            agresivo = int(agresivo_values[index] or 0) if index < len(agresivo_values) else 0
            if recomendado <= 0 and conservador <= 0 and agresivo <= 0:
                continue

            income = _money(Decimal(recomendado) * price)
            total_recommended += recomendado
            total_conservative += conservador
            total_aggressive += agresivo
            total_income += income
            day_values.append(
                {
                    "fecha": day,
                    "fecha_iso": day.isoformat(),
                    "fecha_label": _date_label(day),
                    "conservador": conservador,
                    "recomendado": recomendado,
                    "agresivo": agresivo,
                }
            )
            day_totals[day]["total_piezas"] += recomendado
            day_totals[day]["total_ingreso"] += income
            product_day_rank[day][name] += recomendado

        if total_recommended <= 0:
            continue

        product_payload = {
            "point_product_id": product_id,
            "receta_id": receta.id if receta else None,
            "nombre": name,
            "familia": family,
            "categoria": category,
            "categoria_pronostico": category,
            "precio": price,
            "dias": {
                item["fecha_iso"]: {
                    "conservador": item["conservador"],
                    "recomendado": item["recomendado"],
                    "agresivo": item["agresivo"],
                }
                for item in day_values
            },
            "dias_lista": day_values,
            "por_dia": {item["fecha_iso"]: item["recomendado"] for item in day_values},
            "escenarios": {
                "conservador": total_conservative,
                "recomendado": total_recommended,
                "agresivo": total_aggressive,
            },
            "total_piezas": total_recommended,
            "total_ingreso": _money(total_income),
            "pct_total": 0.0,
            "pct_del_total": Decimal("0.00"),
            "factor_tendencia": 1.0,
            "tendencia": "estable",
            "metodo_usado": method,
            "confianza": round(confidence, 3),
        }
        product_totals[product_id] = product_payload

        branch_weights = {
            branch_id: qty
            for (pair_product_id, branch_id), qty in recent_pair_qty.items()
            if pair_product_id == product_id and branch_id in branch_map and qty > 0
        }
        event_branch_weights_by_day: dict[str, tuple[dict[int, Decimal], Decimal]] = {}
        for item in day_values:
            target_day = item["fecha"]
            previous_event_day = _previous_special_context_day(target_day, selected_days)
            if not previous_event_day:
                continue
            event_weights = {
                branch_id: history.get(previous_event_day, Decimal("0"))
                for (pair_product_id, branch_id), history in histories.items()
                if pair_product_id == product_id and branch_id in branch_map
            }
            event_weights = {branch_id: qty for branch_id, qty in event_weights.items() if qty > 0}
            event_total = sum(event_weights.values(), Decimal("0"))
            if event_total > 0:
                event_branch_weights_by_day[item["fecha_iso"]] = (event_weights, event_total)
        total_weight = sum(branch_weights.values(), Decimal("0"))
        if total_weight > 0:
            for branch_id, weight in branch_weights.items():
                recent_share = float(weight / total_weight)
                branch_day_values = []
                branch_total_recommended = 0
                branch_total_conservative = 0
                branch_total_aggressive = 0
                branch_total_income = Decimal("0.00")
                for item in day_values:
                    event_weights, event_total = event_branch_weights_by_day.get(item["fecha_iso"], ({}, Decimal("0")))
                    if event_total > 0:
                        share = float(event_weights.get(branch_id, Decimal("0")) / event_total)
                    else:
                        share = recent_share
                    recomendado = max(0, int(round(item["recomendado"] * share)))
                    conservador = max(0, int(round(item["conservador"] * share)))
                    agresivo = max(0, int(round(item["agresivo"] * share)))
                    if recomendado <= 0 and conservador <= 0 and agresivo <= 0:
                        continue
                    income = _money(Decimal(recomendado) * price)
                    branch_total_recommended += recomendado
                    branch_total_conservative += conservador
                    branch_total_aggressive += agresivo
                    branch_total_income += income
                    branch_day_values.append(
                        {
                            "fecha": item["fecha"],
                            "fecha_iso": item["fecha_iso"],
                            "fecha_label": item["fecha_label"],
                            "conservador": conservador,
                            "recomendado": recomendado,
                            "agresivo": agresivo,
                        }
                    )
                if branch_total_recommended <= 0:
                    continue
                branch_products[branch_id][product_id] = {
                    **product_payload,
                    "dias": {
                        item["fecha_iso"]: {
                            "conservador": item["conservador"],
                            "recomendado": item["recomendado"],
                            "agresivo": item["agresivo"],
                        }
                        for item in branch_day_values
                    },
                    "dias_lista": branch_day_values,
                    "por_dia": {item["fecha_iso"]: item["recomendado"] for item in branch_day_values},
                    "escenarios": {
                        "conservador": branch_total_conservative,
                        "recomendado": branch_total_recommended,
                        "agresivo": branch_total_aggressive,
                    },
                    "total_piezas": branch_total_recommended,
                    "total_ingreso": _money(branch_total_income),
                }

        method_counts[method] += 1
        confidence_weighted_sum += confidence * total_recommended

    total_pieces = sum(product["total_piezas"] for product in product_totals.values())
    total_income = sum((product["total_ingreso"] for product in product_totals.values()), Decimal("0.00"))
    for product in product_totals.values():
        factor_weight = product.pop("_factor_weight", product.get("factor_tendencia", 1.0))
        confidence_sum = product.pop("_confidence_sum", product.get("confianza", 0.0))
        pairs = product.pop("_pairs", 1) or 1
        method_counts_for_product = product.pop("_method_counts", {})
        weighted_factor = factor_weight / product["total_piezas"] if product["total_piezas"] else 1.0
        product["factor_tendencia"] = round(weighted_factor, 2)
        product["tendencia"] = _trend_label(weighted_factor)
        product["confianza"] = round(confidence_sum / pairs, 3)
        if method_counts_for_product:
            product["metodo_usado"] = max(method_counts_for_product.items(), key=lambda item: item[1])[0]
        product["dias_lista"] = []
        product["por_dia"] = {}
        for day in selected_days:
            day_key = day.isoformat()
            day_data = product["dias"].get(day_key, {"conservador": 0, "recomendado": 0, "agresivo": 0})
            product["dias_lista"].append(
                {
                    "fecha": day,
                    "fecha_iso": day_key,
                    "fecha_label": _date_label(day),
                    "conservador": day_data.get("conservador", 0),
                    "recomendado": day_data.get("recomendado", 0),
                    "agresivo": day_data.get("agresivo", 0),
                }
            )
            product["por_dia"][day_key] = day_data.get("recomendado", 0)
        pct = (Decimal(product["total_piezas"]) / Decimal(total_pieces) * Decimal("100")) if total_pieces else Decimal("0")
        product["pct_del_total"] = pct.quantize(Decimal("0.01"))
        product["pct_total"] = float(product["pct_del_total"])

    categories = _build_categories(list(product_totals.values()))
    por_dia = []
    for day in selected_days:
        row = day_totals[day]
        row["total_ingreso"] = _money(row["total_ingreso"])
        row["top_productos"] = [
            {"nombre": name, "piezas": qty, "total_piezas": qty}
            for name, qty in sorted(product_day_rank[day].items(), key=lambda item: (-item[1], item[0]))[:5]
        ]
        row["top_producto"] = row["top_productos"][0] if row["top_productos"] else None
        por_dia.append(row)

    por_sucursal = []
    for branch_id, products_by_id in branch_products.items():
        products = list(products_by_id.values())
        branch_total_pieces = sum(product["total_piezas"] for product in products)
        branch_total_income = sum((product["total_ingreso"] for product in products), Decimal("0.00"))
        for product in products:
            pct = (Decimal(product["total_piezas"]) / Decimal(branch_total_pieces) * Decimal("100")) if branch_total_pieces else Decimal("0")
            product["pct_del_total"] = pct.quantize(Decimal("0.01"))
            product["pct_total"] = float(product["pct_del_total"])
        por_sucursal.append(
            {
                "sucursal_id": branch_id,
                "sucursal_nombre": branch_map[branch_id].nombre,
                "sucursal": branch_map[branch_id].nombre,
                "codigo": branch_map[branch_id].codigo,
                "total_piezas": branch_total_pieces,
                "total_ingreso": _money(branch_total_income),
                "categorias": _build_categories(products),
                "productos": sorted(
                    products,
                    key=_product_sort_key,
                ),
            }
        )
    por_sucursal.sort(key=lambda item: item["sucursal_nombre"])

    if any("prophet" in method for method in method_counts):
        main_method = "prophet"
    elif any("evento-comparable" in method for method in method_counts):
        main_method = "evento-comparable"
    elif any("regresion-estacional" in method for method in method_counts):
        main_method = "regresion-estacional"
    elif any("ets-estacional" in method for method in method_counts):
        main_method = "ets-estacional"
    else:
        main_method = "promedio-ponderado"
    if special_days_in_range and "fecha-especial" not in main_method:
        main_method = f"{main_method}+fecha-especial"
    confidence_avg = round(confidence_weighted_sum / total_pieces, 3) if total_pieces else 0.0
    fechas_especiales = special_days_in_range
    model_explanation = ""
    if special_context_explanations:
        event_names = sorted({item["evento"] for item in special_context_explanations if item.get("evento")})
        years = sorted(
            {
                comparable["anio"]
                for item in special_context_explanations
                for comparable in item.get("comparables", [])
            },
            reverse=True,
        )
        if event_names and years:
            model_explanation = (
                f"Rango tratado como {', '.join(event_names)}: cada fecha se compara con el mismo lugar del evento "
                f"en {', '.join(str(year) for year in years)} y se ajusta con la tendencia reciente."
            )

    return {
        "fechas": [day.isoformat() for day in selected_days],
        "fechas_tabla": [{"iso": day.isoformat(), "label": _date_label(day)} for day in selected_days],
        "resumen": {
            "total_piezas": total_pieces,
            "total_ingreso": _money(total_income),
            "dias": len(selected_days),
            "n_productos": len(product_totals),
            "productos": len(product_totals),
            "n_sucursales": len(por_sucursal),
            "sucursales": len(por_sucursal),
            "fechas_especiales": fechas_especiales,
            "metodo": main_method,
            "confianza_promedio": confidence_avg,
            "tendencia_reciente": _period_label(context_start, history_end),
            "comparable": _period_label(context_comparable_start, context_comparable_end),
            "trend_30": _period_label(trend_start, history_end),
            "trend_30_comparable": _period_label(trend_comparable_start, trend_comparable_end),
            "comparables_evento": special_context_explanations,
            "explicacion_modelo": model_explanation,
        },
        "por_categoria": categories,
        "por_dia": por_dia,
        "por_sucursal": por_sucursal,
        "por_producto": sorted(
            product_totals.values(),
            key=_product_sort_key,
        ),
        "por_producto_familias": [
            {
                "familia": category["categoria"],
                "total_piezas": category["subtotal_piezas"],
                "total_ingreso": category["subtotal_ingreso"],
                "rows": category["productos"],
            }
            for category in categories
        ],
    }


def _build_categories(products: list[dict]) -> list[dict]:
    categories = []
    category_names = sorted({product.get("categoria") or "Sin categoría" for product in products}, key=_category_sort_key)
    for category in category_names:
        category_products = [product for product in products if product.get("categoria") == category]
        if not category_products:
            continue
        category_products.sort(key=lambda item: (-item["total_piezas"], item["nombre"]))
        subtotal_pieces = sum(product["total_piezas"] for product in category_products)
        subtotal_income = sum((product["total_ingreso"] for product in category_products), Decimal("0.00"))
        categories.append(
            {
                "categoria": category,
                "productos": category_products,
                "subtotal_piezas": subtotal_pieces,
                "subtotal_ingreso": _money(subtotal_income),
            }
        )
    return categories
