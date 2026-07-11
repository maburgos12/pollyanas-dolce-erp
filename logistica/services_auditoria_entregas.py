"""Auditoría conservadora de hechos de entrega.

Este módulo nunca corrige estados operativos: únicamente agrega eventos de
inconsistencia que dejan el caso listo para revisión humana.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from django.db import transaction

from .models import EventoRuta, ParadaRuta, RutaEntrega
from .services_entregas import _actor_puede_confirmar, _evento_geocerca_es_confiable


EVENTOS_CONFIRMACION = {
    EventoRuta.TIPO_ENTREGA,
    EventoRuta.TIPO_ENTREGA_EXCEPCIONAL,
}


@dataclass(frozen=True)
class HallazgoEntrega:
    regla: str
    ruta_id: int
    parada_id: int
    hecho: str
    descripcion: str

    @property
    def clave(self) -> str:
        return f"{self.regla}:{self.ruta_id}:{self.parada_id}:{self.hecho}"


def _texto_fuentes(parada: ParadaRuta, eventos: list[EventoRuta]) -> str:
    fragmentos = [parada.entrega_notas or ""]
    for evento in eventos:
        metadata = evento.metadata or {}
        fragmentos.extend(
            str(metadata.get(campo) or "")
            for campo in ("origen", "fuente", "actor", "origen_servicio")
        )
    return " ".join(fragmentos).lower()


def _hallazgos_parada(parada: ParadaRuta) -> list[HallazgoEntrega]:
    eventos = list(parada.eventos.exclude(tipo=EventoRuta.TIPO_INCONSISTENCIA_ENTREGA).order_by("id"))
    geocercas = [evento for evento in eventos if evento.tipo == EventoRuta.TIPO_LLEGADA_GEOFENCE]
    geocercas_confiables = [
        evento
        for evento in geocercas
        if evento.ubicacion_id
        and _evento_geocerca_es_confiable(evento=evento, ruta=parada.ruta, parada=parada)
    ]
    geocerca_confiable = bool(geocercas_confiables)
    confirmaciones = [evento for evento in eventos if evento.tipo in EVENTOS_CONFIRMACION]
    hallazgos: list[HallazgoEntrega] = []

    def agregar(regla: str, hecho: str, descripcion: str):
        hallazgos.append(
            HallazgoEntrega(
                regla=regla,
                ruta_id=parada.ruta_id,
                parada_id=parada.id,
                hecho=hecho,
                descripcion=descripcion,
            )
        )

    if (
        parada.entrega_estado == ParadaRuta.ENTREGA_ENTREGADA
        and not geocerca_confiable
        and parada.revision_entrega_estado == ParadaRuta.REVISION_NO_REQUERIDA
    ):
        agregar(
            "ENTREGADA_SIN_GEOFENCE_O_REVISION",
            parada.entrega_estado,
            "Entrega marcada como entregada sin geocerca confiable ni revisión administrativa.",
        )

    if parada.estado == ParadaRuta.ESTADO_VISITADA and not geocerca_confiable:
        agregar(
            "VISITADA_SIN_GPS_CONFIABLE",
            parada.estado,
            "Parada marcada como visitada sin evento GPS confiable.",
        )

    actor = parada.entrega_confirmada_por
    if actor and not _actor_puede_confirmar(actor=actor, ruta=parada.ruta):
        agregar(
            "ENTREGA_ACTOR_INDEBIDO",
            f"actor-{actor.id}",
            "La entrega quedó atribuida a un actor sin permiso para confirmarla.",
        )

    fuente = _texto_fuentes(parada, eventos)
    if (parada.hora_llegada_real or parada.hora_salida_real) and any(
        marcador in fuente for marcador in ("point", "admin", "sync")
    ):
        agregar(
            "HORAS_DERIVADAS_FUENTE_ADMIN_POINT",
            "horas-fuente-no-gps",
            "Hay horas físicas derivadas de evidencia administrativa, Point o sincronización.",
        )

    for evento in geocercas:
        if evento not in geocercas_confiables:
            agregar(
                "LLEGADA_GEOFENCE_INVALIDA",
                f"evento-{evento.id}",
                "Existe un evento de llegada a geocerca que no cumple el contrato GPS confiable.",
            )

    if len(confirmaciones) > 1:
        agregar(
            "CONFIRMACION_DUPLICADA_O_INCOMPATIBLE",
            "eventos-" + "-".join(str(evento.id) for evento in confirmaciones),
            "La parada contiene confirmaciones de entrega duplicadas o incompatibles.",
        )
    elif confirmaciones:
        estado_evento = str((confirmaciones[0].metadata or {}).get("entrega_estado") or "")
        if estado_evento and estado_evento != parada.entrega_estado:
            agregar(
                "CONFIRMACION_DUPLICADA_O_INCOMPATIBLE",
                f"evento-{confirmaciones[0].id}-{estado_evento}-{parada.entrega_estado}",
                "El estado confirmado en el evento contradice el estado actual de entrega.",
            )

    tiene_alerta_revision = any(
        (
            evento.tipo == EventoRuta.TIPO_ENTREGA_EXCEPCIONAL
            or (
                evento.tipo == EventoRuta.TIPO_INCONSISTENCIA_ENTREGA
                and (evento.metadata or {}).get("regla") == "REVISION_PENDIENTE_SIN_ALERTA"
            )
        )
        and evento.severidad
        in {EventoRuta.SEVERIDAD_ALERTA, EventoRuta.SEVERIDAD_CRITICA}
        for evento in parada.eventos.all()
    )
    if parada.revision_entrega_estado == ParadaRuta.REVISION_PENDIENTE and not tiene_alerta_revision:
        agregar(
            "REVISION_PENDIENTE_SIN_ALERTA",
            parada.revision_entrega_causa or "sin-causa",
            "La revisión de entrega está pendiente pero no existe una alerta asociada.",
        )

    return hallazgos


def auditar_entregas_ruta(
    *, ruta_id: int | None = None, fecha: date | None = None, dry_run: bool = False
) -> dict:
    rutas = RutaEntrega.objects.all().order_by("id")
    if ruta_id is not None:
        rutas = rutas.filter(pk=ruta_id)
    if fecha is not None:
        rutas = rutas.filter(fecha_ruta=fecha)

    resumen = {
        "rutas_revisadas": 0,
        "paradas_revisadas": 0,
        "hallazgos": [],
        "alertas_creadas": 0,
        "dry_run": dry_run,
    }
    for ruta_pk in rutas.values_list("pk", flat=True):
        with transaction.atomic():
            ruta = RutaEntrega.objects.select_for_update().get(pk=ruta_pk)
            resumen["rutas_revisadas"] += 1
            paradas = (
                ruta.paradas.select_related("ruta__repartidor__user", "entrega_confirmada_por")
                .prefetch_related("eventos", "eventos__ubicacion__repartidor")
                .order_by("id")
            )
            for parada in paradas:
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
                    if dry_run:
                        continue
                    _, creada = EventoRuta.objects.get_or_create(
                        ruta=ruta,
                        parada=parada,
                        tipo=EventoRuta.TIPO_INCONSISTENCIA_ENTREGA,
                        metadata__clave=hallazgo.clave,
                        defaults={
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
                    resumen["alertas_creadas"] += int(creada)
    return resumen
