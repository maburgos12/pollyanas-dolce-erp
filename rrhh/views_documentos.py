from __future__ import annotations

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import get_object_or_404, redirect, render

from .models import Empleado, EmpleadoDocumento
from .services_vacantes import can_gestionar_vacantes
from .views import _module_tabs


def _can_gestionar_documentos(user) -> bool:
    if not user or not user.is_authenticated:
        return False
    return user.is_superuser or can_gestionar_vacantes(user)


@login_required
def empleado_documentos(request, empleado_pk: int):
    empleado = get_object_or_404(Empleado, pk=empleado_pk)
    if not _can_gestionar_documentos(request.user):
        raise PermissionDenied("Solo Capital Humano puede gestionar documentos.")

    if request.method == "POST":
        tipo = (request.POST.get("tipo") or "").strip()
        valor_texto = (request.POST.get("valor_texto") or "").strip()
        notas = (request.POST.get("notas") or "").strip()
        archivo = request.FILES.get("archivo")

        if not tipo:
            messages.error(request, "Selecciona el tipo de documento.")
            return redirect("rrhh:rrhh_empleado_documentos", empleado_pk=empleado.pk)

        # Cuenta bancaria: guardar los tres campos en Empleado directamente
        if tipo == EmpleadoDocumento.TIPO_BANCO:
            banco = (request.POST.get("banco") or "").strip()
            clabe = (request.POST.get("cuenta_clabe") or "").strip()
            numero = (request.POST.get("numero_cuenta") or "").strip()
            update = {}
            if banco:
                update["banco"] = banco
            if clabe:
                update["cuenta_clabe"] = clabe
            if numero:
                update["numero_cuenta"] = numero
            if update:
                Empleado.objects.filter(pk=empleado.pk).update(**update)
            valor_texto = f"Banco: {banco} | CLABE: {clabe} | Cuenta: {numero}"

        doc = EmpleadoDocumento.objects.create(
            empleado=empleado,
            tipo=tipo,
            archivo=archivo,
            valor_texto=valor_texto.upper() if tipo in EmpleadoDocumento.TIPOS_TEXTO else valor_texto,
            notas=notas,
            subido_por=request.user,
        )
        messages.success(request, f"Documento '{doc.get_tipo_display()}' guardado.")
        return redirect("rrhh:rrhh_empleado_documentos", empleado_pk=empleado.pk)

    documentos = EmpleadoDocumento.objects.filter(empleado=empleado).select_related("subido_por")
    docs_por_tipo = {}
    for doc in documentos:
        docs_por_tipo.setdefault(doc.tipo, []).append(doc)

    return render(
        request,
        "rrhh/empleado_documentos.html",
        {
            "module_tabs": _module_tabs("empleados", request.user),
            "empleado": empleado,
            "documentos": documentos,
            "docs_por_tipo": docs_por_tipo,
            "tipos_con_docs": set(docs_por_tipo.keys()),
            "tipo_choices": EmpleadoDocumento.TIPO_CHOICES,
            "tipos_texto": EmpleadoDocumento.TIPOS_TEXTO,
        },
    )


@login_required
def empleado_documento_eliminar(request, empleado_pk: int, doc_pk: int):
    if request.method != "POST":
        return redirect("rrhh:rrhh_empleado_documentos", empleado_pk=empleado_pk)
    if not _can_gestionar_documentos(request.user):
        raise PermissionDenied()
    doc = get_object_or_404(EmpleadoDocumento, pk=doc_pk, empleado_id=empleado_pk)
    doc.delete()
    messages.success(request, "Documento eliminado.")
    return redirect("rrhh:rrhh_empleado_documentos", empleado_pk=empleado_pk)
