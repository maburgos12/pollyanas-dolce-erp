from django.contrib import admin

from .models import ConsejoConsulta


@admin.register(ConsejoConsulta)
class ConsejoConsultaAdmin(admin.ModelAdmin):
    list_display = ("pregunta_corta", "veredicto_ceo", "creado_por", "creado_en")
    list_filter = ("veredicto_ceo",)
    readonly_fields = (
        "pregunta",
        "snapshot_json",
        "respuestas_json",
        "veredicto_ceo",
        "resumen_ejecutivo_ceo",
        "creado_por",
        "creado_en",
    )

    def pregunta_corta(self, obj):
        return obj.pregunta[:80]

    pregunta_corta.short_description = "Pregunta"

    def has_add_permission(self, request):
        return False
