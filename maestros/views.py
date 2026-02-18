from django.shortcuts import render, redirect
from django.views.generic import ListView, CreateView, UpdateView, DeleteView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.urls import reverse_lazy
from django.db.models import Q
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
        if search:
            queryset = queryset.filter(Q(nombre__icontains=search) | Q(codigo__icontains=search))
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
        context['total_insumos'] = qs.count()
        context['total_activos'] = qs.filter(activo=True).count()
        return context

class InsumoCreateView(LoginRequiredMixin, CreateView):
    model = Insumo
    template_name = 'maestros/insumo_form.html'
    fields = ['codigo', 'nombre', 'unidad_base', 'proveedor_principal', 'activo']
    success_url = reverse_lazy('maestros:insumo_list')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['unidades'] = UnidadMedida.objects.all()
        context['proveedores'] = Proveedor.objects.filter(activo=True)
        return context

class InsumoUpdateView(LoginRequiredMixin, UpdateView):
    model = Insumo
    template_name = 'maestros/insumo_form.html'
    fields = ['codigo', 'nombre', 'unidad_base', 'proveedor_principal', 'activo']
    success_url = reverse_lazy('maestros:insumo_list')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['unidades'] = UnidadMedida.objects.all()
        context['proveedores'] = Proveedor.objects.filter(activo=True)
        return context

class InsumoDeleteView(LoginRequiredMixin, DeleteView):
    model = Insumo
    template_name = 'maestros/insumo_confirm_delete.html'
    success_url = reverse_lazy('maestros:insumo_list')
