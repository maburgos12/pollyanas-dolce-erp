"""Durable preparation and second approval for the private hours bot."""
import base64
import hashlib
import re
import secrets
from datetime import datetime, timedelta

from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from horarios_especiales.models import SolicitudHorarioEspecial, TelegramHoursStory, TelegramHoursUpdate
from horarios_especiales.services.requests import _lock_request_branches
from horarios_especiales.services.story_art import TITLE, ZONE, build_schedule, render_notice
from horarios_especiales.services.story_planner import DIGEST, ROOT, MarketingClient, MarketingError, encode


def approved(request_obj, revision):
    return (
        request_obj.approved_at and not request_obj.cancelled_at
        and request_obj.status in {'APROBADO', 'EJECUTADO', 'FALLIDO'}
        and request_obj.canonical_payload.get('schema_version') == 2
        and request_obj.canonical_payload.get('revision') == revision
        and request_obj.audit_entries.filter(action='APPROVED', payload_json__revision=revision).exists()
    )


def branches(draft):
    return {branch['branch_id'] for branch in draft['canonical_payload']['locations']}


def superseded_refs(draft):
    begin, end = (datetime.fromisoformat(draft[key]) for key in ('valid_from', 'valid_until'))
    wanted = branches(draft)
    references = []
    for other in TelegramHoursStory.objects.filter(
            Q(state__in=('APPROVE', 'SCHEDULED')) | Q(state='NOTICE', remote__has_key='post_ids')
    ).iterator():
        prior = other.draft
        prior_begin, prior_end = (datetime.fromisoformat(prior[key]) for key in ('valid_from', 'valid_until'))
        if begin >= prior_end or prior_begin >= end or wanted.isdisjoint(branches(prior)):
            continue
        if not (begin <= prior_begin and prior_end <= end and wanted.issuperset(branches(prior))):
            raise ValueError('Hay una historia aprobada que cubre otras fechas o sucursales. Prepara un aviso que cubra todo su alcance.')
        references.append({'draft_id': other.remote['draft_id'], 'draft_digest': other.remote['draft_digest']})
    return sorted(references, key=lambda ref: ref['draft_id'])


def stage(source, actor):
    from horarios_especiales.services.telegram_bot import approval_snapshot, preserved_schedule, preview_schedule

    obj = SolicitudHorarioEspecial.objects.select_for_update().get(pk=source.request_id)
    _lock_request_branches(obj)
    revision = obj.canonical_payload.get('revision')
    if not approved(obj, revision) or actor.pk != obj.approved_by_id or actor.pk != obj.requested_by_id:
        raise ValueError('La solicitud de horarios ya no está aprobada para esta cuenta.')
    prior = obj.telegramhoursstory_set.filter(revision=revision).order_by('-art_revision').first()
    if prior and prior.state in {'ASSET', 'DRAFT', 'IMAGE', 'REVIEW', 'APPROVE', 'SCHEDULED'}:
        if prior.state in {'APPROVE', 'SCHEDULED'} or timezone.now() < datetime.fromisoformat(prior.draft['review_expires_at']):
            raise ValueError('Esta revisión ya tiene una historia en preparación, revisión o programada.')
    plan, note = build_schedule(obj.canonical_payload['rules'], timezone.now())
    schedule = preview_schedule(obj.canonical_payload)
    preserved = preserved_schedule(obj.canonical_payload)
    if preserved:
        schedule += '\n\nSe conserva:\n' + '\n'.join(preserved)
    draft = {**plan, 'request_id': obj.pk, 'request_code': obj.request_code, 'erp_revision_digest': revision,
             'art_revision': prior.art_revision + 1 if prior else 1, 'canonical_payload': obj.canonical_payload,
             'hours_approved_at': obj.approved_at.isoformat(), 'hours_approved_by_id': actor.pk,
             'title': TITLE, 'caption': schedule}
    draft['supersedes'] = superseded_refs(draft)
    TelegramHoursStory.objects.create(
        request=obj, source=source, revision=revision, art_revision=draft['art_revision'],
        nonce=secrets.token_hex(16), draft=draft, snapshot=approval_snapshot(obj), state='ASSET',
        remote={'schedule_note': note},
    )


def check_binding(row, source, config, actor, message_id, nonce):
    from horarios_especiales.services.telegram_bot import authorized

    return (authorized(row.payload, config) and source.bot_id == row.bot_id
            and source.erp_user_id == actor.pk and source.chat_id == row.chat_id
            and source.telegram_user_id == row.telegram_user_id and message_id == row.message_id
            and secrets.compare_digest(nonce, row.payload['callback_query']['data'].rsplit(':', 1)[-1]))


@transaction.atomic
def handle_callback(row, config, actor):
    from horarios_especiales.services.telegram_bot import approval_snapshot

    callback = row.payload['callback_query']
    match = re.fullmatch(r'(storydraft|storyapprove):([1-9][0-9]{0,18}):([a-f0-9]{32})', callback['data'])
    if not match or int(match[2]) >= 2**63:
        return 'El botón de historia ya no es válido.'
    if not MarketingClient.configured():
        return 'La conexión con el planificador de historias está desactivada.'
    try:
        if match[1] == 'storydraft':
            source = TelegramHoursUpdate.objects.filter(pk=int(match[2]), state='DONE', request__isnull=False).first()
            if not source or not check_binding(row, source, config, actor, source.preview_message_id, source.nonce):
                raise ValueError('El botón de historia ya no es válido.')
            stage(source, actor)
            return 'Estoy preparando la imagen. Te enviaré el borrador con su propio botón de aprobación.'
        story = TelegramHoursStory.objects.select_for_update(of=('self',)).select_related('source').filter(pk=int(match[2])).first()
        if not story or not check_binding(row, story.source, config, actor, story.preview_message_id, story.nonce):
            raise ValueError('El botón de historia ya no es válido.')
        if story.state in {'APPROVE', 'SCHEDULED'}:
            return 'Esta historia ya tiene una aprobación registrada.'
        now = timezone.now()
        if story.state != 'REVIEW' or now >= datetime.fromisoformat(story.draft['review_expires_at']):
            raise ValueError('La revisión de esta historia venció. Genera un nuevo borrador.')
        obj = SolicitudHorarioEspecial.objects.select_for_update().get(pk=story.request_id)
        _lock_request_branches(obj)
        if not approved(obj, story.revision) or approval_snapshot(obj) != story.snapshot:
            raise ValueError('Hubo un cambio en los horarios desde la vista previa. Genera un nuevo borrador.')
        story.approval_body = encode({
            'request_id': story.request_id, 'erp_revision_digest': story.revision, 'art_revision': story.art_revision,
            'draft_digest': story.remote['draft_digest'], 'bot_id': row.bot_id, 'update_id': row.update_id,
            'message_id': row.message_id, 'telegram_user_id': row.telegram_user_id, 'erp_user_id': actor.pk,
            'chat_id': row.chat_id, 'nonce': story.nonce, 'approved_at': now.isoformat(),
        }).decode()
        story.state = 'APPROVE'
        story.next_attempt_at = now
        story.save(update_fields=['approval_body', 'state', 'next_attempt_at'])
        return 'Aprobación de la imagen registrada. El planificador todavía debe confirmar la programación.'
    except ValueError as exc:
        return str(exc)


def preview_caption(story):
    starts = [datetime.fromisoformat(w['scheduled_at']).astimezone(ZONE) for w in story.draft['schedule_windows']]
    dates = ', '.join(d.strftime('%d/%m') for d in starts)
    replaces = story.draft['supersedes']
    replacement_note = '\nSustituye: ' + ', '.join(
        'solicitud ' + ref['draft_id'].split(':')[1] for ref in replaces
    ) if replaces else ''
    caption = (
        f'Vista previa · {story.draft["request_code"]}\n'
        f'Historias de Facebook e Instagram · 9:00 a. m. (Sinaloa)\n{dates}\n'
        f'{story.remote.get("schedule_note", "")}{replacement_note}\n'
        'El botón aprueba esta imagen y estas fechas.'
    )
    if len(caption) > 1024:
        raise ValueError('El detalle de la segunda aprobación excede el espacio del mensaje.')
    return caption


def validate_marketing_draft(story, reply):
    expected = f'hours:{story.request_id}:{story.revision}:{story.art_revision}'
    if (reply.get('draft_id') != expected or reply.get('erp_revision_digest') != story.revision
            or reply.get('art_revision') != story.art_revision or not re.fullmatch(DIGEST, reply.get('draft_digest', ''))):
        raise MarketingError('invalid_draft_response', retryable=True)


def advance(bot_config, telegram, marketing, *, revocations_only=False):
    from horarios_especiales.services.telegram_bot import actor_for, approval_snapshot, config_for_row

    now = timezone.now()
    for story in TelegramHoursStory.objects.filter(
            source__bot_id=bot_config['bot_id'], next_attempt_at__lte=now,
            state__in=('ASSET', 'DRAFT', 'IMAGE', 'REVIEW', 'APPROVE', 'INVALIDATE', 'SCHEDULED', 'NOTICE'),
    ).select_related('source', 'request').order_by('next_attempt_at', 'pk')[:20]:
        try:
            config = config_for_row(bot_config, story.source)
            actor_for(config)
        except PermissionDenied:
            if story.state != 'INVALIDATE':
                story.state = 'INVALIDATE'
                story.save(update_fields=['state'])
            # Failed or withdrawn users never receive a new Telegram message.
            if story.state == 'INVALIDATE':
                process(story, telegram, marketing, False, now, notify=False)
                return
            continue
        unchanged = approved(story.request, story.revision) and approval_snapshot(story.request) == story.snapshot
        if revocations_only:
            if unchanged and story.state != 'INVALIDATE':
                continue
            process(story, telegram, marketing, False, now, notify=False)
            return
        if story.state == 'SCHEDULED' and unchanged:
            story.next_attempt_at = now + timedelta(minutes=15)
            story.save(update_fields=['next_attempt_at'])
            continue
        process(story, telegram, marketing, unchanged, now)
        return


def process(story, telegram, marketing, unchanged, now, *, notify=True):
    from horarios_especiales.services.telegram_bot import TelegramUnavailable

    try:
        expired = now >= datetime.fromisoformat(story.draft['review_expires_at'])
        if (not unchanged
                or expired and story.state in {'ASSET', 'DRAFT', 'IMAGE', 'REVIEW'}):
            story.state = 'INVALIDATE'
        if story.state == 'APPROVE':
            from horarios_especiales.services.telegram_bot import approval_snapshot

            with transaction.atomic():
                obj = SolicitudHorarioEspecial.objects.select_for_update().get(pk=story.request_id)
                try:
                    _lock_request_branches(obj)
                except ValueError:
                    raise MarketingError('hours_busy', retryable=True) from None
                if not approved(obj, story.revision) or approval_snapshot(obj) != story.snapshot:
                    story.state = 'INVALIDATE'
                else:
                    step(story, telegram, marketing, notify=notify)
        else:
            step(story, telegram, marketing, notify=notify)
        story.error_code = ''
        delay = timedelta(minutes=15) if story.state == 'SCHEDULED' else timedelta(seconds=15)
        if story.state == 'REVIEW':
            story.next_attempt_at = datetime.fromisoformat(story.draft['review_expires_at'])
        else:
            story.next_attempt_at = now + delay
    except MarketingError as exc:
        story.error_code = str(exc)
        story.next_attempt_at = now + timedelta(minutes=1)
        # A missing local receipt does not prove that Marketing rejected an
        # earlier delivery. Preserve the exact approval/revocation until it is
        # acknowledged, even after a terminal auth/configuration error.
        if not exc.retryable and story.state not in {'APPROVE', 'INVALIDATE'}:
            story.remote['notice'] = 'El planificador no confirmó el trámite de la historia. Código: ' + str(exc)
            story.state = 'NOTICE' if notify else 'STOPPED'
    except ValueError as exc:
        story.remote['notice'] = str(exc)[:200]
        story.state = 'NOTICE' if notify else 'STOPPED'
    except TelegramUnavailable:
        # An ambiguous Telegram send is never repeated automatically.
        if story.state != 'SCHEDULED':
            story.state = 'STOPPED'
        story.error_code = 'telegram_delivery_unknown'
    story.save(update_fields=['remote', 'draft', 'preview_message_id', 'state', 'error_code', 'next_attempt_at'])


def step(story, telegram, marketing, *, notify):
    from horarios_especiales.services.telegram_bot import TelegramUnavailable

    if story.state == 'ASSET':
        image = render_notice(story.draft['caption'])
        asset = marketing.request('POST', ROOT + '/assets', encode({
            'image_base64': base64.b64encode(image).decode(), 'duration_seconds': 20,
        }))
        asset_id, image_hash = asset.get('asset_id', ''), asset.get('image_sha256', '')
        if not re.fullmatch(DIGEST, asset_id) or not re.fullmatch(DIGEST, image_hash):
            raise MarketingError('invalid_asset_response', retryable=True)
        story.draft['asset_id'] = asset_id
        story.remote['image_sha256'] = image_hash
        story.state = 'DRAFT'
    elif story.state == 'DRAFT':
        reply = marketing.request('POST', ROOT, encode(story.draft))
        validate_marketing_draft(story, reply)
        if reply.get('status') != 'awaiting_approval':
            raise MarketingError('unexpected_draft_state')
        story.remote.update(draft_id=reply['draft_id'], draft_digest=reply['draft_digest'])
        story.state = 'IMAGE'
    elif story.state == 'IMAGE':
        image = marketing.request('GET', f"{ROOT}/assets/{story.draft['asset_id']}/preview", image=True)
        if hashlib.sha256(image).hexdigest() != story.remote['image_sha256']:
            raise MarketingError('preview_digest_mismatch')
        caption = preview_caption(story)
        story.state = 'STOPPED'  # Durable before sending; Telegram has no idempotency key.
        story.save(update_fields=['state'])
        reply = telegram.call('sendPhoto', chat_id=story.source.chat_id, photo=image, caption=caption, reply_markup={
            'inline_keyboard': [[{'text': 'Aprobar imagen y programación', 'callback_data': f'storyapprove:{story.pk}:{story.nonce}'}]],
        })
        if (not isinstance(reply, dict) or reply.get('chat', {}).get('id') != story.source.chat_id or reply.get('chat', {}).get('type') != 'private'
                or reply.get('from', {}).get('id') != story.source.bot_id or reply.get('from', {}).get('is_bot') is not True
                or type(reply.get('message_id')) is not int or reply['message_id'] <= 0):
            raise TelegramUnavailable('Telegram no confirmó la imagen en el chat privado.')
        story.preview_message_id = reply['message_id']
        story.state = 'REVIEW'
    elif story.state == 'APPROVE':
        # Marketing deduplicates the exact receipt and atomically recovers committed posts.
        reply = marketing.request('POST', f"{ROOT}/{story.remote['draft_id']}/approve", story.approval_body.encode())
        validate_marketing_draft(story, reply)
        if reply.get('draft_digest') != story.remote['draft_digest'] or reply.get('status') != 'scheduled' or not reply.get('post_ids'):
            raise MarketingError('invalid_schedule_receipt', retryable=True)
        story.remote['post_ids'] = reply['post_ids']
        story.remote['notice'] = 'Historia programada para Facebook e Instagram en el periodo que aprobaste.'
        story.state = 'NOTICE'
    elif story.state == 'INVALIDATE':
        target = {
            'request_id': story.request_id, 'erp_revision_digest': story.revision, 'art_revision': story.art_revision,
        }
        reply = marketing.request('POST', f'{ROOT}/invalidate', encode(target))
        revoked = reply.get('invalidated_draft_ids')
        if (any(reply.get(key) != value for key, value in target.items()) or not isinstance(revoked, list)
                or story.remote.get('draft_id') and story.remote['draft_id'] not in revoked):
            raise MarketingError('invalid_revocation_receipt', retryable=True)
        story.state = 'STOPPED'
    elif story.state == 'NOTICE':
        story.state = 'SCHEDULED' if story.remote.get('post_ids') else 'STOPPED'
        story.save(update_fields=['state'])
        if notify:
            telegram.call('sendMessage', chat_id=story.source.chat_id, text=story.remote['notice'], disable_notification=True)
