import csv

from django.contrib import messages
from django.http import HttpResponse
from django.core.exceptions import PermissionDenied
from django.shortcuts import render, redirect
from django.core.paginator import Paginator
from django.views.generic import ListView, CreateView, UpdateView, DeleteView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.urls import reverse_lazy
from django.db.models import Count, Q
from django.contrib.auth.decorators import login_required
from core.access import ROLE_ADMIN, ROLE_COMPRAS, can_view_maestros, has_any_role
from recetas.models import Receta, RecetaCodigoPointAlias, normalizar_codigo_point
from recetas.utils.normalizacion import normalizar_nombre

from .models import PointPendingMatch, Proveedor, Insumo, InsumoAlias, UnidadMedida

# ============ PROVEEDORES ============

class ProveedorListView(LoginRequiredMixin, ListView):
    model = Proveedor
    template_name = 'maestros/proveedor_list.html'
    context_object_name = 'proveedores'
    paginate_by = 20
    
    def get_queryset(self):
        queryset = Proveedor.objects.all()
        search = self.request.GET.get('q')
        estado = self.request.GET.get('estado')
        if search:
            queryset = queryset.filter(nombre__icontains=search)
        if estado == "activos":
            queryset = queryset.filter(activo=True)
        elif estado == "inactivos":
            queryset = queryset.filter(activo=False)
        return queryset.order_by('nombre')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        qs = self.get_queryset()
        context['search_query'] = self.request.GET.get('q', '')
        context['estado'] = self.request.GET.get('estado', '')
        context['total_proveedores'] = qs.count()
        context['total_activos'] = qs.filter(activo=True).count()
        return context

class ProveedorCreateView(LoginRequiredMixin, CreateView):
    model = Proveedor
    template_name = 'maestros/proveedor_form.html'
    fields = ['nombre', 'lead_time_dias', 'activo']
    success_url = reverse_lazy('maestros:proveedor_list')

class ProveedorUpdateView(LoginRequiredMixin, UpdateView):
    model = Proveedor
    template_name = 'maestros/proveedor_form.html'
    fields = ['nombre', 'lead_time_dias', 'activo']
    success_url = reverse_lazy('maestros:proveedor_list')

class ProveedorDeleteView(LoginRequiredMixin, DeleteView):
    model = Proveedor
    template_name = 'maestros/proveedor_confirm_delete.html'
    success_url = reverse_lazy('maestros:proveedor_list')

# ============ INSUMOS ============

class InsumoListView(LoginRequiredMixin, ListView):
    model = Insumo
    template_name = 'maestros/insumo_list.html'
    context_object_name = 'insumos'
    paginate_by = 20
    
    def get_queryset(self):
        queryset = Insumo.objects.select_related('unidad_base', 'proveedor_principal')
        search = self.request.GET.get('q')
        estado = self.request.GET.get('estado')
        point_status = self.request.GET.get('point_status')
        if search:
            queryset = queryset.filter(
                Q(nombre__icontains=search)
                | Q(codigo__icontains=search)
                | Q(codigo_point__icontains=search)
                | Q(nombre_point__icontains=search)
            )
        if estado == "activos":
            queryset = queryset.filter(activo=True)
        elif estado == "inactivos":
            queryset = queryset.filter(activo=False)
        if point_status == "pendientes":
            queryset = queryset.filter(activo=True).filter(Q(codigo_point="") | Q(codigo_point__isnull=True))
        elif point_status == "completos":
            queryset = queryset.filter(activo=True).exclude(Q(codigo_point="") | Q(codigo_point__isnull=True))
        return queryset.order_by('nombre')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        qs = self.get_queryset()
        active_qs = Insumo.objects.filter(activo=True)
        pending_point_qs = active_qs.filter(Q(codigo_point="") | Q(codigo_point__isnull=True))
        total_active = active_qs.count()
        total_pending_point = pending_point_qs.count()
        total_complete_point = max(total_active - total_pending_point, 0)
        point_ratio = round((total_complete_point * 100.0 / total_active), 2) if total_active else 100.0

        context['search_query'] = self.request.GET.get('q', '')
        context['estado'] = self.request.GET.get('estado', '')
        context['point_status'] = self.request.GET.get('point_status', '')
        context['total_insumos'] = qs.count()
        context['total_activos'] = qs.filter(activo=True).count()
        context['total_point_pendientes'] = total_pending_point
        context['total_point_completos'] = total_complete_point
        context['point_ratio'] = point_ratio
        return context

class InsumoCreateView(LoginRequiredMixin, CreateView):
    model = Insumo
    template_name = 'maestros/insumo_form.html'
    fields = ['codigo', 'codigo_point', 'nombre', 'nombre_point', 'categoria', 'unidad_base', 'proveedor_principal', 'activo']
    success_url = reverse_lazy('maestros:insumo_list')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['unidades'] = UnidadMedida.objects.all()
        context['proveedores'] = Proveedor.objects.filter(activo=True)
        return context

    def form_valid(self, form):
        response = super().form_valid(form)
        if self.object.activo and not (self.object.codigo_point or "").strip():
            messages.warning(
                self.request,
                "Insumo activo sin Código Point: queda pendiente de homologación para integración.",
            )
        return response

class InsumoUpdateView(LoginRequiredMixin, UpdateView):
    model = Insumo
    template_name = 'maestros/insumo_form.html'
    fields = ['codigo', 'codigo_point', 'nombre', 'nombre_point', 'categoria', 'unidad_base', 'proveedor_principal', 'activo']
    success_url = reverse_lazy('maestros:insumo_list')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['unidades'] = UnidadMedida.objects.all()
        context['proveedores'] = Proveedor.objects.filter(activo=True)
        return context

    def form_valid(self, form):
        response = super().form_valid(form)
        if self.object.activo and not (self.object.codigo_point or "").strip():
            messages.warning(
                self.request,
                "Insumo activo sin Código Point: queda pendiente de homologación para integración.",
            )
        return response

class InsumoDeleteView(LoginRequiredMixin, DeleteView):
    model = Insumo
    template_name = 'maestros/insumo_confirm_delete.html'
    success_url = reverse_lazy('maestros:insumo_list')


@login_required
def insumo_point_mapping_csv(request):
    qs = (
        Insumo.objects.select_related('unidad_base')
        .annotate(alias_count=Count("aliases"))
        .order_by("nombre")
    )
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="insumos_point_mapping.csv"'
    writer = csv.writer(response)
    writer.writerow([
        "insumo_id",
        "codigo_interno",
        "codigo_point",
        "nombre_interno",
        "nombre_point",
        "nombre_normalizado",
        "unidad_base",
        "alias_count",
        "activo",
    ])
    for i in qs:
        writer.writerow([
            i.id,
            i.codigo or "",
            i.codigo_point or "",
            i.nombre or "",
            i.nombre_point or "",
            i.nombre_normalizado or "",
            i.unidad_base.codigo if i.unidad_base else "",
            i.alias_count,
            "1" if i.activo else "0",
        ])
    return response


@login_required
def point_pending_review(request):
    if not can_view_maestros(request.user):
        raise PermissionDenied("No tienes permisos para ver Maestros.")

    can_manage = has_any_role(request.user, ROLE_ADMIN, ROLE_COMPRAS)
    allowed_types = {
        PointPendingMatch.TIPO_PROVEEDOR,
        PointPendingMatch.TIPO_INSUMO,
        PointPendingMatch.TIPO_PRODUCTO,
    }

    if request.method == "POST":
        if not can_manage:
            raise PermissionDenied("No tienes permisos para resolver pendientes Point.")

        action = (request.POST.get("action") or "").strip().lower()
        tipo = (request.POST.get("tipo") or PointPendingMatch.TIPO_INSUMO).strip().upper()
        if tipo not in allowed_types:
            tipo = PointPendingMatch.TIPO_INSUMO

        pending_ids = [pid for pid in request.POST.getlist("pending_ids") if pid.isdigit()]
        selected = PointPendingMatch.objects.filter(id__in=pending_ids, tipo=tipo)
        if not pending_ids:
            messages.error(request, "Selecciona al menos un pendiente.")
            return redirect("maestros:point_pending_review")

        if action == "resolve_insumos":
            insumo_id = (request.POST.get("insumo_id") or "").strip()
            create_aliases = request.POST.get("create_aliases") == "on"
            target = Insumo.objects.filter(pk=insumo_id).first() if insumo_id else None
            if not target:
                messages.error(request, "Selecciona un insumo destino.")
                return redirect("maestros:point_pending_review")

            resolved = 0
            conflicts = 0
            aliases_created = 0
            for p in selected:
                point_code = (p.point_codigo or "").strip()
                if point_code and target.codigo_point and target.codigo_point != point_code:
                    conflicts += 1
                    continue

                changed = []
                if point_code and target.codigo_point != point_code:
                    target.codigo_point = point_code
                    changed.append("codigo_point")
                if target.nombre_point != p.point_nombre:
                    target.nombre_point = p.point_nombre
                    changed.append("nombre_point")
                if changed:
                    target.save(update_fields=changed)

                if create_aliases:
                    alias_norm = normalizar_nombre(p.point_nombre)
                    if alias_norm and alias_norm != target.nombre_normalizado:
                        alias, was_created = InsumoAlias.objects.get_or_create(
                            nombre_normalizado=alias_norm,
                            defaults={"nombre": p.point_nombre[:250], "insumo": target},
                        )
                        if not was_created and alias.insumo_id != target.id:
                            alias.insumo = target
                            alias.save(update_fields=["insumo"])
                        if was_created:
                            aliases_created += 1

                p.delete()
                resolved += 1

            messages.success(
                request,
                f"Pendientes resueltos (insumos): {resolved}. Aliases creados: {aliases_created}.",
            )
            if conflicts:
                messages.warning(
                    request,
                    f"Pendientes con conflicto de código Point (no aplicados): {conflicts}.",
                )

        elif action == "resolve_productos":
            receta_id = (request.POST.get("receta_id") or "").strip()
            create_aliases = request.POST.get("create_aliases") == "on"
            target = Receta.objects.filter(pk=receta_id).first() if receta_id else None
            if not target:
                messages.error(request, "Selecciona una receta destino.")
                return redirect("maestros:point_pending_review")

            resolved = 0
            conflicts = 0
            aliases_created = 0
            for p in selected:
                point_code = (p.point_codigo or "").strip()
                if point_code:
                    point_norm = normalizar_codigo_point(point_code)
                    primary_norm = normalizar_codigo_point(target.codigo_point)
                    if not target.codigo_point:
                        target.codigo_point = point_code[:80]
                        target.save(update_fields=["codigo_point"])
                    elif primary_norm != point_norm:
                        if not point_norm:
                            conflicts += 1
                            continue
                        if not create_aliases:
                            conflicts += 1
                            continue
                        alias, was_created = RecetaCodigoPointAlias.objects.get_or_create(
                            codigo_point_normalizado=point_norm,
                            defaults={
                                "receta": target,
                                "codigo_point": point_code[:80],
                                "nombre_point": (p.point_nombre or "")[:250],
                                "activo": True,
                            },
                        )
                        if not was_created and alias.receta_id != target.id:
                            conflicts += 1
                            continue
                        changed = []
                        if alias.codigo_point != point_code[:80]:
                            alias.codigo_point = point_code[:80]
                            changed.append("codigo_point")
                        if (p.point_nombre or "").strip() and alias.nombre_point != (p.point_nombre or "")[:250]:
                            alias.nombre_point = (p.point_nombre or "")[:250]
                            changed.append("nombre_point")
                        if not alias.activo:
                            alias.activo = True
                            changed.append("activo")
                        if changed:
                            alias.save(update_fields=changed)
                        if was_created:
                            aliases_created += 1

                p.delete()
                resolved += 1

            messages.success(
                request,
                f"Pendientes resueltos (productos): {resolved}. Aliases creados: {aliases_created}.",
            )
            if conflicts:
                messages.warning(request, f"Conflictos de código Point en productos: {conflicts}.")

        elif action == "resolve_proveedores":
            proveedor_id = (request.POST.get("proveedor_id") or "").strip()
            target = Proveedor.objects.filter(pk=proveedor_id).first() if proveedor_id else None
            resolved = 0
            created = 0
            for p in selected:
                if not target:
                    _, was_created = Proveedor.objects.get_or_create(nombre=p.point_nombre[:200], defaults={"activo": True})
                    if was_created:
                        created += 1
                p.delete()
                resolved += 1
            messages.success(
                request,
                f"Pendientes resueltos (proveedores): {resolved}. Proveedores nuevos creados: {created}.",
            )

        elif action == "discard_selected":
            deleted, _ = selected.delete()
            messages.success(request, f"Pendientes descartados: {deleted}.")
        else:
            messages.error(request, "Acción no válida.")

        return redirect("maestros:point_pending_review")

    tipo = (request.GET.get("tipo") or PointPendingMatch.TIPO_INSUMO).strip().upper()
    if tipo not in allowed_types:
        tipo = PointPendingMatch.TIPO_INSUMO
    q = (request.GET.get("q") or "").strip()

    qs = PointPendingMatch.objects.filter(tipo=tipo).order_by("-fuzzy_score", "point_nombre")
    if q:
        qs = qs.filter(
            Q(point_nombre__icontains=q)
            | Q(point_codigo__icontains=q)
            | Q(fuzzy_sugerencia__icontains=q)
        )

    paginator = Paginator(qs, 200)
    page = paginator.get_page(request.GET.get("page"))
    counts = {
        PointPendingMatch.TIPO_INSUMO: PointPendingMatch.objects.filter(tipo=PointPendingMatch.TIPO_INSUMO).count(),
        PointPendingMatch.TIPO_PRODUCTO: PointPendingMatch.objects.filter(tipo=PointPendingMatch.TIPO_PRODUCTO).count(),
        PointPendingMatch.TIPO_PROVEEDOR: PointPendingMatch.objects.filter(tipo=PointPendingMatch.TIPO_PROVEEDOR).count(),
    }

    return render(
        request,
        "maestros/point_pending_review.html",
        {
            "tipo": tipo,
            "q": q,
            "page": page,
            "counts": counts,
            "can_manage": can_manage,
            "insumos": Insumo.objects.filter(activo=True).order_by("nombre")[:1500],
            "recetas": Receta.objects.order_by("nombre")[:1500],
            "proveedores": Proveedor.objects.filter(activo=True).order_by("nombre")[:800],
        },
    )
