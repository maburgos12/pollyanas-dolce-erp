from collections import OrderedDict

from django.utils import timezone

from core.models import Notificacion
from fallas.models import ReporteFalla


def _principal_ids(report_ids):
    rows = ReporteFalla.objects.filter(pk__in=report_ids).values("id", "duplicado_de_id")
    return {row["id"]: row["duplicado_de_id"] or row["id"] for row in rows}


def _horas_extra_actuales(referencias):
    # El aviso conserva su historia; la petición de actuar depende de RRHH hoy.
    from rrhh.models import HoraExtra
    ids = {int(objeto_id) for objeto_tipo, objeto_id in referencias
           if objeto_tipo == "rrhh.HoraExtra" and len(objeto_id) <= 18 and objeto_id.isascii() and objeto_id.isdigit()}
    return {str(he.pk): he for he in HoraExtra.objects.filter(pk__in=ids).select_related("empleado")}


def agrupar_notificaciones(user):
    rows = list(
        Notificacion.objects.filter(usuario=user)
        .select_related("actor")
        .order_by("-creado_en", "-pk")
    )
    extras = _horas_extra_actuales((row.objeto_tipo, row.objeto_id) for row in rows)
    for row in rows:
        row.grupo_resuelta = False
        if row.objeto_tipo != "rrhh.HoraExtra":
            continue
        extra = extras.get(row.objeto_id)
        if extra:
            row.url = f"/rrhh/horas-extra/?estado={extra.estado}#hora-extra-{extra.pk}"
            if extra.estado != "pendiente":
                row.grupo_resuelta = True
                row.titulo = f"Hora extra · {extra.get_estado_display()}: {extra.empleado.nombre}"
                row.mensaje = f"{extra.fecha:%d/%m/%Y} · {extra.horas} h · solicitud resuelta. No requiere autorización."
        else:
            row.grupo_resuelta = True
            row.titulo = "Hora extra · solicitud no disponible"
            row.mensaje = "El registro original ya no está disponible. Este aviso se conserva como historial."
            row.url = "/rrhh/horas-extra/"
    report_ids = {
        int(row.objeto_id)
        for row in rows
        if row.objeto_tipo == "ReporteFalla" and row.objeto_id.isdigit()
    }
    principales = _principal_ids(report_ids)
    grupos = OrderedDict()
    for row in rows:
        if row.objeto_tipo == "ReporteFalla" and row.objeto_id.isdigit():
            principal_id = principales.get(int(row.objeto_id), int(row.objeto_id))
            key = ("ReporteFalla", str(principal_id))
        else:
            key = ("Notificacion", str(row.pk))
        grupos.setdefault(key, []).append(row)

    resultado = []
    for key, members in grupos.items():
        representative = max(members, key=lambda item: (item.creado_en, item.pk))
        representative.grupo_ids = [item.pk for item in members]
        representative.grupo_total = len(members)
        representative.grupo_leida = all(item.leida or item.grupo_resuelta for item in members)
        if key[0] == "ReporteFalla":
            representative.url = f"/mantenimiento/?open=falla:{key[1]}"
        resultado.append(representative)
    return sorted(
        resultado,
        key=lambda item: (item.grupo_leida, -item.creado_en.timestamp(), -item.pk),
    )


def contar_grupos_pendientes(user):
    rows = list(
        Notificacion.objects.filter(usuario=user, leida=False).values_list(
            "pk", "objeto_tipo", "objeto_id"
        )
    )
    extras = _horas_extra_actuales((tipo, pk) for _, tipo, pk in rows)
    report_ids = {
        int(objeto_id)
        for _, objeto_tipo, objeto_id in rows
        if objeto_tipo == "ReporteFalla" and objeto_id.isdigit()
    }
    principales = _principal_ids(report_ids)
    pendientes = set()
    for notification_id, objeto_tipo, objeto_id in rows:
        if objeto_tipo == "rrhh.HoraExtra":
            extra = extras.get(objeto_id)
            if not extra or extra.estado != "pendiente":
                continue
        if objeto_tipo == "ReporteFalla" and objeto_id.isdigit():
            report_id = int(objeto_id)
            pendientes.add(("ReporteFalla", principales.get(report_id, report_id)))
        else:
            pendientes.add(("Notificacion", notification_id))
    return len(pendientes)


def marcar_grupo_leido(user, notificacion):
    grupo = next(
        (
            row
            for row in agrupar_notificaciones(user)
            if notificacion.pk in row.grupo_ids
        ),
        None,
    )
    if grupo is None:
        return notificacion.url
    Notificacion.objects.filter(
        usuario=user, pk__in=grupo.grupo_ids, leida=False
    ).update(
        leida=True,
        leido_en=timezone.now(),
    )
    return grupo.url
