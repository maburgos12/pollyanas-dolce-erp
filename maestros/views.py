import csv

from django.contrib import messages
from django.http import HttpResponse
from django.shortcuts import render, redirect
from django.views.generic import ListView, CreateView, UpdateView, DeleteView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.urls import reverse_lazy
from django.db.models import Count, Q
from django.contrib.auth.decorators import login_required
from .models import Proveedor, Insumo, UnidadMedida

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
    fields = ['codigo', 'codigo_point', 'nombre', 'nombre_point', 'unidad_base', 'proveedor_principal', 'activo']
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
    fields = ['codigo', 'codigo_point', 'nombre', 'nombre_point', 'unidad_base', 'proveedor_principal', 'activo']
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
