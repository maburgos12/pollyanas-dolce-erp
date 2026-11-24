"""Bitácora documental de empresa, independiente de importes, cobertura y RRHH."""
import hashlib
import json
from datetime import date

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError, connection, transaction
from rrhh.services_permisos import can_authorize_direccion
from ventas.models import ContextoComercial

FIELDS = ('clave', 'fecha', 'tipo', 'titulo', 'descripcion', 'evidencia')


def contexts_for_range(*, start_date, end_date):
    return [dict(clave=row.clave, fecha=row.fecha.isoformat(), tipo=row.tipo,
                 etiqueta=row.get_tipo_display(), titulo=row.titulo, descripcion=row.descripcion,
                 evidencia=row.evidencia, referencia=row.clave, alcance='empresa_general_informativo')
            for row in ContextoComercial.objects.filter(fecha__range=(start_date, end_date)).order_by('fecha', 'clave')]


def _normalize(entries, actor):
    if not isinstance(entries, list) or not entries:
        raise ValidationError('El archivo debe contener una lista no vacía de contextos.')
    result, keys = [], set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != set(FIELDS):
            raise ValidationError('Cada contexto requiere exactamente: ' + ', '.join(FIELDS))
        row = dict(entry)
        if any(not isinstance(row[key], str) or not row[key].strip() for key in FIELDS if key != 'evidencia'):
            raise ValidationError('Los campos de texto deben ser cadenas no vacías.')
        try:
            parsed = date.fromisoformat(row['fecha'])
            if parsed.isoformat() != row['fecha']:
                raise ValueError
            row['fecha'] = parsed.isoformat()
        except ValueError as exc:
            raise ValidationError('Fecha inválida; use YYYY-MM-DD.') from exc
        candidate = ContextoComercial(**row, autor=actor)
        candidate.full_clean(validate_unique=False)
        if row['clave'] in keys:
            raise ValidationError('Clave duplicada en el lote.')
        keys.add(row['clave'])
        result.append(row)
    return sorted(result, key=lambda row: row['clave'])


@transaction.atomic
def register_contexts(*, entries, actor_id, apply=False, digest=None):
    if apply:
        # One transaction lock also serializes absent keys across different DG actors.
        # Preview never locks rows and remains usable in READ ONLY transactions.
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_advisory_xact_lock(%s)', [5771230241124])
    actors = get_user_model().objects.all()
    if apply:
        actors = actors.select_for_update()
    actor = actors.get(pk=actor_id)
    if not actor.is_active or not can_authorize_direccion(actor):
        raise PermissionDenied('Se requiere Dirección General activa.')
    rows = _normalize(entries, actor)
    existing = {}
    contexts = ContextoComercial.objects.filter(clave__in=[row['clave'] for row in rows])
    if apply:
        contexts = contexts.select_for_update()
    for obj in contexts:
        value = {key: getattr(obj, key) for key in FIELDS}
        value['fecha'] = value['fecha'].isoformat()
        existing[obj.clave] = dict(value=value, autor_id=obj.autor_id, creado_en=obj.creado_en.isoformat())
    for row in rows:
        if row['clave'] in existing and existing[row['clave']]['value'] != row:
            raise ValidationError('La clave ya existe con contenido distinto: ' + row['clave'])
    state = dict(actor_id=actor.pk, entries=rows, existing=existing)
    fresh_digest = hashlib.sha256(json.dumps(state, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    if apply and (not digest or digest != fresh_digest):
        raise ValidationError('Digest ausente o desactualizado; ejecute una nueva vista previa.')
    new_rows = [row for row in rows if row['clave'] not in existing]
    if apply:
        for row in new_rows:
            try:
                ContextoComercial.objects.create(**row, autor=actor)
            except IntegrityError as exc:
                raise ValidationError('El estado cambió concurrentemente; ejecute una nueva vista previa.') from exc
    return dict(digest=fresh_digest, aplicado=apply, nuevas=len(new_rows), existentes=len(existing), entradas=rows)
