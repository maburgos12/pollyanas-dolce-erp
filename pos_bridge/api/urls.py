from django.urls import include, path
from rest_framework.routers import DefaultRouter

from pos_bridge.api.views import AgentQueryView, InventoryViewSet, ProductsViewSet, SalesViewSet, SyncJobsViewSet

app_name = "pos_bridge_api"

router = DefaultRouter()
router.register("sales", SalesViewSet, basename="pos_bridge_sales")
router.register("inventory", InventoryViewSet, basename="pos_bridge_inventory")
router.register("products", ProductsViewSet, basename="pos_bridge_products")
router.register("sync-jobs", SyncJobsViewSet, basename="pos_bridge_sync_jobs")

urlpatterns = [
    path("agent/query/", AgentQueryView.as_view(), name="agent-query"),
    path("", include(router.urls)),
]
