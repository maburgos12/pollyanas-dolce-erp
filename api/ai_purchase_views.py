"""Confirmación y evidencia de compras: sesión humana, CSRF y alcance fresco."""
import logging

from django.http import FileResponse, Http404
from django.core.files.storage import default_storage
from rest_framework.authentication import SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.exceptions import ValidationError

from mantenimiento.evidence_validation import EvidenceValidationError
from orquestacion.services import agent_purchases as purchases
from orquestacion.services.agent_workflows import WorkflowError

logger = logging.getLogger(__name__)


class AIPurchaseConfirmationView(APIView):
    authentication_classes = [SessionAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request, draft_id):
        try:
            return Response(purchases.confirm(user=request.user,draft_id=draft_id,arguments=request.data))
        except WorkflowError as exc:
            return Response({'code':exc.code,'detail':str(exc)},status=exc.status)
        except ValidationError:
            raise
        except Exception:
            logger.exception('Purchase confirmation failed')
            return Response({'detail':'No se recibió un resultado seguro. Revisa la misma propuesta antes de reintentar.'},status=503)


class AIPurchaseEvidenceView(APIView):
    authentication_classes = [SessionAuthentication]
    permission_classes = [IsAuthenticated]

    def post(self, request, draft_id):
        try:
            if set(request.data)-{'expected_version','payload_hash','files'}:
                raise ValueError
            version=request.data.get('expected_version','')
            if not isinstance(version,str) or not version.isascii() or not version.isdecimal() or len(version)>9:
                raise ValueError
            args={'expected_version':int(version),'payload_hash':request.data.get('payload_hash'),'confirm':True}
            return Response(purchases.attach(user=request.user,draft_id=draft_id,arguments=args,files=request.FILES.getlist('files')))
        except WorkflowError as exc:
            return Response({'code':exc.code,'detail':str(exc)},status=exc.status)
        except (ValueError,EvidenceValidationError):
            return Response({'detail':'Adjunta hasta cinco imágenes JPG, PNG o WebP válidas, máximo 10 MB cada una y 20 MB en total.'},status=400)
        except ValidationError:
            raise
        except Exception:
            logger.exception('Purchase evidence upload failed')
            return Response({'detail':'No se pudieron asociar las imágenes. Revisa la propuesta antes de reintentar.'},status=503)

    def get(self, request, draft_id, evidence_id):
        draft=purchases.ChatToolCall.objects.select_related('conversation').filter(public_id=draft_id,tool_key=purchases.KEY).first()
        if not draft: raise Http404
        actor=purchases.fresh_asset_user(request.user)
        try:
            if draft.conversation.owner_id==request.user.pk:
                dto=purchases.project(draft,actor)
                if dto['status'] not in ('WAITING_INFORMATION','AWAITING_CONFIRMATION','EXECUTED'): raise Http404
            else:
                if draft.metadata_json.get('purchase_status')!='EXECUTED': raise Http404
                row=purchases.SolicitudCompraDepartamental.objects.filter(pk=draft.metadata_json.get('request_id')).first()
                if not actor or not row or not purchases._puede_ver_solicitud(actor,row): raise Http404
                if not purchases.AuditLog.objects.filter(action='AI_PURCHASE_REQUEST_CREATE',object_id=str(row.pk),
                        model='compras.SolicitudCompraDepartamental',payload__draft_id=str(draft.public_id),
                        payload__payload_hash=draft.metadata_json['payload_hash']).exists(): raise Http404
            info=next((f for f in draft.metadata_json.get('files',[]) if f['id']==str(evidence_id)),None)
            if not info: raise Http404
            result=FileResponse(default_storage.open(info['path'],'rb'),content_type=info['mime'],filename=info['nombre'])
            result['Cache-Control']='private, no-store'
            result['X-Content-Type-Options']='nosniff'
            return result
        except (WorkflowError,OSError):
            raise Http404
