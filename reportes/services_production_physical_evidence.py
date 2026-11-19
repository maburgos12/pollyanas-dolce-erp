"""Read-only physical evidence displayed separately from Point balances."""
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation

from django.utils.dateparse import parse_datetime
from django.db.models import F

from core.models import Sucursal, sucursales_operativas_q
from inventario.conteos_access import conteos_visibles, puede_coordinar
from inventario.models_conteos import (
    AccesoConteoSucursal, EventoConteoSucursal, LecturaConteoSucursal, LineaConteoSucursal,
)
from reportes.models import ProductInventoryAuditCase


def _physical_comparison(case):
    readings = [item for item in case.get("physical_evidence", []) if item["quantity"] is not None]
    if not readings:
        return {"label": "Sin conteo", "summary": "sin conteo"}
    if len({item["conteo"].pk for item in readings}) > 1:
        return {"label": "Revisar conteos", "summary": "por revisar"}
    reading = max(readings, key=lambda item: item["round"] or 0)
    try:
        quantity = Decimal(str(reading["quantity"]))
        if not quantity.is_finite() or quantity < 0:
            raise InvalidOperation
    except InvalidOperation:
        return {"label": "Revisar conteo", "summary": "por revisar"}
    closing = case["closing"]
    if closing is None:
        return {"label": "Sin saldo final Point", "summary": "sin saldo final Point"}
    delta = quantity - closing
    provisional = "hora de corte común por confirmar"
    if reading["conteo"].estado == "RECONTEO" and not reading.get("automatic_history"):
        provisional = "se usa la última ronda enviada"
    elif not reading.get("counted_at"):
        provisional = "hora de conteo sin registrar"
    return {
        "label": "Con diferencia" if delta else "Coincide",
        "summary": "con diferencia" if delta else "coinciden",
        "quantity": quantity,
        "closing": closing,
        "delta": delta,
        "unit": reading["unit"],
        "provisional": provisional,
    }


def _automatic_history_flags(counts, events, current_round_rows):
    by_count = {}
    for event in events:
        by_count.setdefault(event.conteo_id, []).append(event)
    rows_by_count = {}
    for row in current_round_rows:
        rows_by_count.setdefault(row['linea__conteo_id'], []).append(row)
    flags = {}
    for pk, count in counts.items():
        if (count.estado != 'RECONTEO' or type(count.ronda) is not int
                or count.ronda <= 1 or count.iniciado_en is not None):
            continue
        history = by_count.get(pk, [])
        sends = [e for e in history if e.action == 'enviar']
        if len(sends) != 1:
            continue
        autos = [e for e in history if e.action == 'conciliacion_nocturna'
                 and e.actor_id is None and isinstance(e.payload, dict)
                 and isinstance(e.payload.get('automatizacion'), dict)
                 and e.payload['automatizacion'].get('accion') == 'RECONTEO']
        if not autos:
            continue
        event = max(autos, key=lambda e: e.pk)
        payload = event.payload
        previous = payload.get('ronda')
        ids = payload['automatizacion'].get('linea_ids')
        if (type(previous) is not int or previous + 1 != count.ronda
                or not isinstance(ids, list) or not ids
                or any(type(i) is not int or i <= 0 for i in ids)
                or len(ids) != len(set(ids)) or sends[0].pk >= event.pk):
            continue
        if any(e.pk > event.pk and e.action in {'iniciar', 'reconteo', 'enviar'} for e in history):
            continue
        sent = sends[0].payload
        if not isinstance(sent, dict):
            continue
        if isinstance(sent.get('snapshot'), list):
            snapshots = sent['snapshot']
            if any(not isinstance(r, dict) for r in snapshots):
                continue
            rounds = {r.get('ronda') for r in snapshots if r.get('cantidad') is not None}
            if rounds != {previous} or any(type(r) is not int for r in rounds):
                continue
        elif isinstance(sent.get('lecturas'), dict):
            if type(sent.get('ronda', 1)) is not int or sent.get('ronda', 1) != previous:
                continue
        else:
            continue
        rows = rows_by_count.get(pk, [])
        if (set(ids) != {r['linea_id'] for r in rows}
                or any(r['ronda'] != count.ronda or r['cantidad'] is not None
                       or r['contado_en'] is not None or str(r['incidencia']).strip() for r in rows)):
            continue
        flags[pk] = {'event_id': event.pk, 'previous_round': previous}
    return flags


def add_physical_evidence(user, cutoff, groups):
    rows = [row for group in groups for row in group["rows"]]
    cases = {case["id"]: case for row in rows for case in row["cases"]}
    if not cases:
        return
    visible = conteos_visibles(user).filter(fecha=cutoff, estado__in=["ENVIADO", "RECONTEO", "ACEPTADO"])
    # Capture-only users must not see prior rounds in a blind recount.
    eligible = Sucursal.objects.filter(sucursales_operativas_q(cutoff))
    if not puede_coordinar(user):
        if not getattr(user, "is_authenticated", False) or not user.is_active:
            return
        reviewed = AccesoConteoSucursal.objects.filter(user_id=user.pk, activo=True, revisar=True)
        eligible = eligible.filter(pk__in=reviewed.values("sucursal_id"))
    eligible_ids = set(eligible.values_list("pk", flat=True))
    visible = visible.filter(sucursal_id__in=eligible_ids)
    pairs = {
        (branch, product): cases[pk]
        for pk, branch, product in ProductInventoryAuditCase.objects.filter(pk__in=cases)
        .values_list("pk", "branch__erp_branch_id", "product_id") if branch in eligible_ids
    }
    if not pairs:
        return
    lines = {
        line.pk: line
        for line in LineaConteoSucursal.objects.filter(
            conteo__in=visible, producto_id__in={product for _, product in pairs},
            conteo__sucursal_id__in={branch for branch, _ in pairs},
        ).select_related("conteo__sucursal").order_by("conteo_id", "id")
        if (line.conteo.sucursal_id, line.producto_id) in pairs
    }
    submissions = defaultdict(list)
    imports = {}
    events = list(EventoConteoSucursal.objects.filter(
        conteo_id__in={line.conteo_id for line in lines.values()},
        action__in=["enviar", "importacion_historica", "conciliacion_nocturna", "reconteo", "iniciar"],
    ).order_by("id"))
    for event in events:
        if event.action == "enviar":
            submissions[event.conteo_id].append(event.payload)
        elif event.action == "importacion_historica":
            imports[event.conteo_id] = event.payload.get("procedencia", {})
    candidates = {line.conteo_id: line.conteo for line in lines.values()
                  if line.conteo.estado == "RECONTEO"}
    current_round_rows = list(LecturaConteoSucursal.objects.filter(
        linea__conteo_id__in=candidates, ronda=F("linea__conteo__ronda"),
    ).values("linea__conteo_id", "linea_id", "ronda", "cantidad", "contado_en", "incidencia")) if candidates else []
    automatic_history = _automatic_history_flags(candidates, events, current_round_rows)
    imported_values = dict(LecturaConteoSucursal.objects.filter(
        linea_id__in=[line.pk for line in lines.values() if line.conteo_id in imports], ronda=1,
    ).values_list("linea_id", "cantidad"))
    for line in lines.values():
        conteo = line.conteo
        case = pairs[conteo.sucursal_id, line.producto_id]
        evidence = case.setdefault("physical_evidence", [])
        if conteo.pk in imports:
            source = imports[conteo.pk]
            match = next((cell for cell in source.get("celdas", [])
                          if cell.get("domain") == "producto" and cell.get("catalogo_id") == line.producto_id), {})
            evidence.append(dict(conteo=conteo, unit=line.unidad, quantity=imported_values.get(line.pk),
                                 historical=True, file=source.get("archivo"), sheet=match.get("hoja"),
                                 cell=match.get("celda"), round=1))
        seen_rounds = set()
        for index, submission in enumerate(submissions[conteo.pk]):
            if "snapshot" in submission:
                item = next((entry for entry in submission["snapshot"] if entry["linea_id"] == line.pk), None)
                round_number = item.get("ronda") if item else None
            else:
                item = submission.get("lecturas", {}).get(str(line.pk))
                round_number = submission.get("ronda", 1 if index == 0 else None)
            if item is not None and (round_number is None or round_number not in seen_rounds):
                seen_rounds.add(round_number)
                stamp = item.get("contado_en")
                evidence.append(dict(conteo=conteo, unit=line.unidad, quantity=item.get("cantidad"),
                                     round=round_number, counted_at=parse_datetime(stamp) if stamp else None,
                                     automatic_history=automatic_history.get(conteo.pk)))

    for case in pairs.values():
        case["physical_comparison"] = _physical_comparison(case)
    for row in rows:
        comparisons = [case["physical_comparison"] for case in row["cases"] if "physical_comparison" in case]
        row["physical_summary"] = dict(Counter(result["summary"] for result in comparisons))
        row["provisional_count"] = sum(bool(result.get("provisional")) for result in comparisons)
