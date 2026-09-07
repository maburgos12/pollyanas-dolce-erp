from django import forms
from django.contrib.auth import get_user_model
from django.db.models import Q
from core.models import UserProfile, sucursales_operativas
from .models_conteos import AccesoConteoSucursal


class PrepararConteoForm(forms.Form):
    sucursal = forms.ModelChoiceField(queryset=sucursales_operativas(), label='Sucursal')
    responsable = forms.ModelChoiceField(queryset=get_user_model().objects.filter(is_active=True).order_by('username'), label='Responsable de captura')
    fecha = forms.DateField(label='Fecha programada', widget=forms.DateInput(attrs={'type':'date'}, format='%Y-%m-%d'))
    titulo = forms.CharField(max_length=180, label='Nombre del conteo', initial='Conteo de cierre')
    request_id = forms.UUIDField(widget=forms.HiddenInput)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        branches=sucursales_operativas()
        branch=str(self.data.get('sucursal','')) if self.is_bound else ''
        if branch.isdigit(): branches=branches.filter(pk=branch)
        profiles=UserProfile.objects.filter(sucursal__in=branches).values('user_id')
        grants=AccesoConteoSucursal.objects.filter(sucursal__in=branches,activo=True,capturar=True).values('user_id')
        self.fields['responsable'].queryset=get_user_model().objects.filter(is_active=True).filter(Q(pk__in=profiles)|Q(pk__in=grants)).order_by('username')
