"""Auditoría aditiva de entregas; nunca corrige hechos operativos."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date

from django.db import IntegrityError, transaction
from django.db.models import Prefetch

from .models import EventoRuta, ParadaEntregaEvidencia, ParadaRuta, PuntoLogistico, RutaEntrega
from .services_entregas import _evento_geocerca_es_confiable, _notificar_revision


EVENTOS_AUDITORIA = {
    EventoRuta.TIPO_LLEGADA_GEOFENCE,
    EventoRuta.TIPO_ENTREGA,
    EventoRuta.TIPO_ENTREGA_EXCEPCIONAL,
    EventoRuta.TIPO_INCONSISTENCIA_ENTREGA,
    EventoRuta.TIPO_RECARGA_CEDIS,
    EventoRuta.TIPO_INCIDENCIA_MANUAL,
}
EVENTOS_CONFIRMACION = {EventoRuta.TIPO_ENTREGA, EventoRuta.TIPO_ENTREGA_EXCEPCIONAL}
ORIGENES_HORAS_NO_GPS = {"point_transfer", "admin_operativo", "sync_operativo"}
CAMPOS_HORARIOS = {"hora_llegada_real", "hora_salida_real"}


def construir_clave_auditoria(*, regla, ruta_id, parada_id, hecho) -> str:
    regla_legible = str(regla or "LEGACY").strip()[:100] or "LEGACY"
    hecho_canonico = str(hecho or "")
    digest = hashlib.sha256(hecho_canonico.encode("utf-8")).hexdigest()
    return f"{regla_legible}:{ruta_id}:{parada_id}:{digest}"


def _normalizar_metadata_legacy(evento: EventoRuta) -> str | None:
    metadata = evento.metadata or {}
    clave_legacy = str(metadata.get("clave") or "").strip()
    if not clave_legacy:
        return None
    partes = clave_legacy.split(":", 3)
    regla = metadata.get("regla") or (partes[0] if partes else "LEGACY")
    ruta_id = metadata.get("ruta_id") or (partes[1] if len(partes) > 1 else evento.ruta_id)
    parada_id = metadata.get("parada_id") or (partes[2] if len(partes) > 2 else evento.parada_id)
    hecho = metadata.get("hecho")
    if hecho is None:
        hecho = partes[3] if len(partes) > 3 else clave_legacy
    return construir_clave_auditoria(
        regla=regla,
        ruta_id=ruta_id,
        parada_id=parada_id,
        hecho=hecho,
    )


@dataclass(frozen=True)
class HallazgoEntrega:
    regla: str
    ruta_id: int
    parada_id: int
    hecho: str
    descripcion: str

    @property
    def clave(self) -> str:
        return construir_clave_auditoria(
            regla=self.regla,
            ruta_id=self.ruta_id,
            parada_id=self.parada_id,
            hecho=self.hecho,
        )


def _rutas_queryset():
    eventos = (
        EventoRuta.objects.filter(tipo__in=EVENTOS_AUDITORIA)
        .select_related("ubicacion__repartidor")
        .order_by("id")
    )
    evidencias = ParadaEntregaEvidencia.objects.only(
        "id", "parada_id", "capturado_por_id", "metadata"
    ).order_by("id")
    paradas = (
        ParadaRuta.objects.select_related("punto", "entrega_confirmada_por")
        .prefetch_related(
            Prefetch("eventos", queryset=eventos, to_attr="eventos_auditoria"),
            Prefetch("evidencias_entrega", queryset=evidencias, to_attr="evidencias_auditoria"),
        )
        .order_by("id")
    )
    return RutaEntrega.objects.select_related("repartidor__user").prefetch_related(
        Prefetch("paradas", queryset=paradas, to_attr="paradas_auditoria")
    )


def _recarga_cedis_valida(parada: ParadaRuta, eventos: list[EventoRuta]) -> bool:
    if parada.punto.tipo != PuntoLogistico.TIPO_CEDIS:
        return False
    return any(
        evento.tipo == EventoRuta.TIPO_RECARGA_CEDIS
        and (evento.metadata or {}).get("tipo") == "recarga_cedis"
        and evento.creado_por_id is not None
        for evento in eventos
    )


def _horas_con_procedencia_no_gps(eventos: list[EventoRuta]) -> bool:
    for evento in eventos:
        metadata = evento.metadata or {}
        campos = metadata.get("campos_derivados")
        if (
            metadata.get("origen") in ORIGENES_HORAS_NO_GPS
            and isinstance(campos, list)
            and bool(CAMPOS_HORARIOS.intersection(campos))
        ):
            return True
    return False


def _actor_confirmacion_inconsistente(
    parada: ParadaRuta,
    confirmaciones: list[EventoRuta],
    evidencias: list[ParadaEntregaEvidencia],
) -> bool:
    if not confirmaciones or not parada.entrega_confirmada_por_id:
        return False
    evento = confirmaciones[0]
    if evento.creado_por_id != parada.entrega_confirmada_por_id:
        return True
    vinculadas = [
        evidencia
        for evidencia in evidencias
        if (evidencia.metadata or {}).get("evento_id") == evento.id
    ]
    return any(evidencia.capturado_por_id != evento.creado_por_id for evidencia in vinculadas)


def _hallazgos_parada(parada: ParadaRuta) -> list[HallazgoEntrega]:
    eventos = parada.eventos_auditoria
    evidencias = parada.evidencias_auditoria
    geocercas = [evento for evento in eventos if evento.tipo == EventoRuta.TIPO_LLEGADA_GEOFENCE]
    geocercas_confiables = [
        evento
        for evento in geocercas
        if evento.ubicacion_id
        and _evento_geocerca_es_confiable(evento=evento, ruta=parada.ruta, parada=parada)
    ]
    geocerca_confiable = bool(geocercas_confiables)
    confirmaciones = [evento for evento in eventos if evento.tipo in EVENTOS_CONFIRMACION]
    recarga_cedis = _recarga_cedis_valida(parada, eventos)
    hallazgos: list[HallazgoEntrega] = []

    def agregar(regla: str, hecho: str, descripcion: str):
        hallazgos.append(HallazgoEntrega(regla, parada.ruta_id, parada.id, hecho, descripcion))

    if (
        parada.entrega_estado == ParadaRuta.ENTREGA_ENTREGADA
        and not geocerca_confiable
        and parada.revision_entrega_estado == ParadaRuta.REVISION_NO_REQUERIDA
    ):
        agregar(
            "ENTREGADA_SIN_GEOFENCE_O_REVISION",
            parada.entrega_estado,
            "Entrega sin geocerca confiable ni revisión.",
        )
    if parada.estado == ParadaRuta.ESTADO_VISITADA and not geocerca_confiable and not recarga_cedis:
        agregar(
            "VISITADA_SIN_GPS_CONFIABLE",
            parada.estado,
            "Visita sin GPS confiable ni contrato válido de recarga CEDIS.",
        )
    if _actor_confirmacion_inconsistente(parada, confirmaciones, evidencias):
        agregar(
            "ENTREGA_ACTOR_INDEBIDO",
            f"actor-{parada.entrega_confirmada_por_id}",
            "Actor almacenado contradice la procedencia inmutable.",
        )
    if (parada.hora_llegada_real or parada.hora_salida_real) and _horas_con_procedencia_no_gps(eventos):
        agregar(
            "HORAS_DERIVADAS_FUENTE_ADMIN_POINT",
            "horas-fuente-no-gps",
            "Horas físicas con procedencia estructurada no GPS.",
        )
    for evento in geocercas:
        if evento not in geocercas_confiables:
            agregar(
                "LLEGADA_GEOFENCE_INVALIDA",
                f"evento-{evento.id}",
                "Evento de geocerca incumple el contrato GPS confiable.",
            )
    if len(confirmaciones) > 1:
        hecho = "eventos-" + "-".join(str(evento.id) for evento in confirmaciones)
        agregar("CONFIRMACION_DUPLICADA_O_INCOMPATIBLE", hecho, "Confirmaciones duplicadas o incompatibles.")
    elif confirmaciones:
        estado_evento = str((confirmaciones[0].metadata or {}).get("entrega_estado") or "")
        if estado_evento and estado_evento != parada.entrega_estado:
            hecho = f"evento-{confirmaciones[0].id}-{estado_evento}-{parada.entrega_estado}"
            agregar("CONFIRMACION_DUPLICADA_O_INCOMPATIBLE", hecho, "Evento contradice el estado de entrega.")
    tiene_alerta_revision = any(
        (
            evento.tipo == EventoRuta.TIPO_ENTREGA_EXCEPCIONAL
            or (
                evento.tipo == EventoRuta.TIPO_INCONSISTENCIA_ENTREGA
                and evento.clave_auditoria
                and (evento.metadata or {}).get("regla") == "REVISION_PENDIENTE_SIN_ALERTA"
            )
        )
        and evento.severidad in {EventoRuta.SEVERIDAD_ALERTA, EventoRuta.SEVERIDAD_CRITICA}
        for evento in eventos
    )
    if parada.revision_entrega_estado == ParadaRuta.REVISION_PENDIENTE and not tiene_alerta_revision:
        agregar(
            "REVISION_PENDIENTE_SIN_ALERTA",
            parada.revision_entrega_causa or "sin-causa",
            "Revisión pendiente sin alerta asociada.",
        )
    return hallazgos


def _registrar_alerta(ruta: RutaEntrega, parada: ParadaRuta, hallazgo: HallazgoEntrega) -> bool:
    if EventoRuta.objects.filter(clave_auditoria=hallazgo.clave).exists():
        return False
    legacies = EventoRuta.objects.filter(
        tipo=EventoRuta.TIPO_INCONSISTENCIA_ENTREGA,
        ruta=ruta,
        parada=parada,
        clave_auditoria__isnull=True,
    ).order_by("id")
    legacy = next(
        (evento for evento in legacies if _normalizar_metadata_legacy(evento) == hallazgo.clave),
        None,
    )
    if legacy:
        try:
            with transaction.atomic():
                legacy.clave_auditoria = hallazgo.clave
                legacy.save(update_fields=["clave_auditoria"])
        except IntegrityError:
            pass
        return False
    try:
        _, creada = EventoRuta.objects.get_or_create(
            clave_auditoria=hallazgo.clave,
            defaults={
                "ruta": ruta,
                "parada": parada,
                "tipo": EventoRuta.TIPO_INCONSISTENCIA_ENTREGA,
                "severidad": EventoRuta.SEVERIDAD_ALERTA,
                "descripcion": hallazgo.descripcion,
                "metadata": {
                    "regla": hallazgo.regla,
                    "ruta_id": ruta.id,
                    "parada_id": parada.id,
                    "hecho": hallazgo.hecho,
                    "clave": hallazgo.clave,
                    "origen": "auditor_entregas_ruta",
                },
            },
        )
        if creada:
            _notificar_revision(parada=parada, actor=None, causa=hallazgo.regla)
        return creada
    except IntegrityError:
        return False


def _resumen_base(dry_run: bool) -> dict:
    return {
        "rutas_revisadas": 0,
        "paradas_revisadas": 0,
        "hallazgos": [],
        "alertas_creadas": 0,
        "dry_run": dry_run,
    }


def _auditar_rutas(rutas, *, dry_run: bool) -> dict:
    resumen = _resumen_base(dry_run)
    for ruta in rutas:
        resumen["rutas_revisadas"] += 1
        for parada in ruta.paradas_auditoria:
            resumen["paradas_revisadas"] += 1
            for hallazgo in _hallazgos_parada(parada):
                resumen["hallazgos"].append(
                    {
                        "regla": hallazgo.regla,
                        "ruta_id": ruta.id,
                        "parada_id": parada.id,
                        "hecho": hallazgo.hecho,
                        "clave": hallazgo.clave,
                    }
                )
                if not dry_run:
                    resumen["alertas_creadas"] += int(_registrar_alerta(ruta, parada, hallazgo))
    return resumen


def auditar_entregas_ruta(
    *, ruta_id: int | None = None, fecha: date | None = None, dry_run: bool = False
) -> dict:
    base = _rutas_queryset().order_by("id")
    if ruta_id is not None:
        base = base.filter(pk=ruta_id)
    if fecha is not None:
        base = base.filter(fecha_ruta=fecha)
    if dry_run:
        return _auditar_rutas(base, dry_run=True)

    total = _resumen_base(False)
    for pk in base.values_list("pk", flat=True):
        with transaction.atomic():
            RutaEntrega.objects.select_for_update().get(pk=pk)
            ruta = _rutas_queryset().get(pk=pk)
            parcial = _auditar_rutas([ruta], dry_run=False)
        for campo in ("rutas_revisadas", "paradas_revisadas", "alertas_creadas"):
            total[campo] += parcial[campo]
        total["hallazgos"].extend(parcial["hallazgos"])
    return total
