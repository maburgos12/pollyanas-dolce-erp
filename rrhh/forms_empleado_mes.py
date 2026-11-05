"""Formularios del reconocimiento mensual; reglas compartidas en el servicio."""
from django import forms
from .models_empleado_mes import SECCIONES

DATE = {"type": "date"}


class PeriodoForm(forms.Form):
    mes = forms.DateField(label="Mes evaluado", widget=forms.DateInput(format="%Y-%m", attrs={"type": "month"}), input_formats=["%Y-%m", "%Y-%m-%d"])
    fecha_limite = forms.DateField(label="Fecha límite de captura (opcional)", required=False, widget=forms.DateInput(format="%Y-%m-%d", attrs=DATE), help_text="Deja vacío para capturar hasta que RRHH registre al ganador y cierre las evaluaciones.")


class PostulacionForm(forms.Form):
    empleado = forms.ModelChoiceField(queryset=None, label="Colaborador de tu equipo")

    def __init__(self, *args, candidatos, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["empleado"].queryset = candidatos


class VersionForm(forms.Form):
    version = forms.IntegerField(min_value=0, widget=forms.HiddenInput)


class MotivoForm(VersionForm):
    motivo = forms.CharField(label="Motivo", max_length=2000, widget=forms.Textarea(attrs={"rows": 3}))


class PlazoForm(MotivoForm):
    fecha_limite = forms.DateField(label="Nueva fecha límite (opcional)", required=False, widget=forms.DateInput(format="%Y-%m-%d", attrs=DATE), help_text="Deja vacío para retirar el límite y mantener la captura abierta hasta la elección de RRHH.")


class EleccionForm(MotivoForm):
    evaluacion = forms.ModelChoiceField(queryset=None, label="Candidato ganador")

    def __init__(self, *args, evaluaciones, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["evaluacion"].queryset = evaluaciones.filter(estado="enviada", recomienda=True)
        self.fields["evaluacion"].label_from_instance = lambda e: f"{e.nombre_snapshot} · {e.total}/70"
        self.fields["motivo"].label = "Justificación de la elección (incluye desempate o menor puntuación)"


class PremioForm(VersionForm):
    tipo = forms.ChoiceField(label="Premio aprobado", choices=[("", "Selecciona"), ("efectivo", "Efectivo"), ("vales", "Vales de despensa"), ("descanso", "Descanso pagado")])
    detalle = forms.CharField(label="Detalle y referencia de aprobación", max_length=2000, widget=forms.Textarea(attrs={"rows": 3}))
    importe = forms.DecimalField(label="Importe aprobado (efectivo o vales)", required=False, min_value=0.01, max_digits=10, decimal_places=2)


class EntregaForm(VersionForm):
    fecha = forms.DateField(label="Fecha de entrega", widget=forms.DateInput(format="%Y-%m-%d", attrs=DATE))
    responsable = forms.CharField(label="Persona responsable de la entrega", max_length=180)
    comprobante = forms.CharField(label="Referencia de comprobante o acuse", required=False, max_length=2000, widget=forms.Textarea(attrs={"rows": 3}), help_text="Escribe el folio, ubicación del comprobante o acuse de recepción disponible.")


class EvaluacionForm(VersionForm):
    reconocimiento = forms.TypedChoiceField(label="¿Recibió algún reconocimiento especial este mes?", choices=[("", "Selecciona"), ("si", "Sí"), ("no", "No")], coerce=lambda x: x == "si", required=False, empty_value=None)
    reconocimiento_detalle = forms.CharField(label="Reconocimientos y logros del mes", max_length=2000, required=False, widget=forms.Textarea(attrs={"rows": 3}))
    recomienda = forms.TypedChoiceField(label="¿Lo recomiendas como empleado del mes?", choices=[("", "Selecciona"), ("si", "Sí"), ("no", "No")], coerce=lambda x: x == "si", required=False, empty_value=None)
    justificacion = forms.CharField(label="Justificación final", max_length=2000, required=False, widget=forms.Textarea(attrs={"rows": 4}))

    def __init__(self, *args, evaluacion, **kwargs):
        initial = {"version": evaluacion.version, "reconocimiento": "si" if evaluacion.reconocimiento else "no", "reconocimiento_detalle": evaluacion.reconocimiento_detalle, "recomienda": "" if evaluacion.recomienda is None else ("si" if evaluacion.recomienda else "no"), "justificacion": evaluacion.justificacion}
        for _, criterios in SECCIONES:
            for key, _ in criterios:
                initial[f"nota_{key}"] = evaluacion.calificaciones.get(key, "")
                initial[f"comentario_{key}"] = evaluacion.comentarios.get(key, "")
        super().__init__(*args, initial=initial, **kwargs)
        for _, criterios in SECCIONES:
            for key, label in criterios:
                self.fields[f"nota_{key}"] = forms.TypedChoiceField(label=label, choices=[("", "Sin calificar")] + [(str(i), str(i)) for i in range(1, 6)], coerce=int, required=False, empty_value=None)
                self.fields[f"comentario_{key}"] = forms.CharField(label=f"Comentario: {label}", max_length=1000, required=False, widget=forms.Textarea(attrs={"rows": 2}))

    @property
    def secciones(self):
        return [(titulo, [(self[f"nota_{k}"], self[f"comentario_{k}"]) for k, _ in criterios]) for titulo, criterios in SECCIONES]

    def datos(self):
        d = self.cleaned_data
        keys = [key for _, criterios in SECCIONES for key, _ in criterios]
        return dict(calificaciones={k: d[f"nota_{k}"] for k in keys if d[f"nota_{k}"] is not None}, comentarios={k: d[f"comentario_{k}"] for k in keys}, reconocimiento=bool(d["reconocimiento"]), reconocimiento_detalle=d["reconocimiento_detalle"], recomienda=d["recomienda"], justificacion=d["justificacion"], version=d["version"])
