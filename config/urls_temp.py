from django.contrib import admin
from django.urls import path, include
from django.views.generic import TemplateView

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', TemplateView.as_view(template_name='dashboard/index.html'), name='dashboard'),
    path('login/', TemplateView.as_view(template_name='login.html'), name='login'),
    path('logout/', TemplateView.as_view(template_name='logout.html'), name='logout'),
    # ... tus otras rutas
    path('recetas/', include('recetas.urls'), name='recetas_list'),
    path('api/', include('api.urls')),
]
