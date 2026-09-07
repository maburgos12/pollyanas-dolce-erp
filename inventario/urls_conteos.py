from django.urls import path
from . import views_conteos as views

app_name = 'conteos'
urlpatterns = [
    path('', views.lista, name='lista'),
    path('preparar/', views.preparar, name='preparar'),
    path('<int:pk>/', views.detalle, name='detalle'),
    path('<int:pk>/accion/', views.accion, name='accion'),
    path('<int:pk>/exportar/', views.exportar, name='exportar'),
    path('<int:pk>/evidencia/', views.evidencia, name='evidencia'),
    path('<int:pk>/evidencia/<int:event_id>/', views.descarga, name='descarga'),
]
