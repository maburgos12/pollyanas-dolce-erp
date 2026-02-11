from django.urls import path
from .views import MRPExplodeView

urlpatterns = [
    path("mrp/explode/", MRPExplodeView.as_view(), name="api_mrp_explode"),
]
