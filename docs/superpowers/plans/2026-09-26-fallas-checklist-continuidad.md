# Fallas continuas desde checklist diario Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convertir las repeticiones diarias de un mismo hallazgo de Higiene en constataciones de una sola falla principal, agrupar sus avisos y ofrecer una consolidación histórica revisable sin perder evidencia.

**Architecture:** `RespuestaHigiene` será la evidencia diaria y admitirá una relación muchos-a-uno con `ReporteFalla`. Un servicio de dominio resolverá coincidencias estructuradas y validará cada decisión dentro de una transacción; la UI solo propondrá opciones. Mantenimiento y Notificaciones proyectarán la falla principal con sus constataciones, mientras que la consolidación histórica tendrá servicios separados de vista previa y aplicación explícita.

**Tech Stack:** Django 5, PostgreSQL 16, Django REST Framework, JavaScript sin framework en App Operativa, templates Django, Service Workers, `TestCase`/`TransactionTestCase`.

---

## Mapa de archivos

### Datos y dominio

- Modify: `operacion/models.py` — convertir el vínculo de higiene en muchos-a-uno y registrar el tipo de constatación.
- Create: `operacion/migrations/0006_higiene_continuidad_falla.py` — migración preservando todos los `reporte_falla_id` existentes.
- Create: `operacion/services_higiene_fallas.py` — identidad, coincidencias, bloqueo transaccional, enlace y avisos relevantes.
- Modify: `operacion/services_higiene.py` — delegar creación/reutilización al nuevo servicio.
- Modify: `operacion/services_fallas.py` — aviso específico para cambio, corrección pendiente o escalamiento.

### Captura diaria

- Modify: `operacion/views.py` — endpoint de coincidencias y respuesta estructurada al guardar.
- Modify: `operacion/urls.py` — ruta de consulta de coincidencias.
- Modify: `templates/operacion/higiene_home.html` — panel de continuidad por punto.
- Modify: `static/operacion/higiene.js` — consulta, elección, validación y payload.
- Modify: `static/operacion/higiene.css` — estados del panel de continuidad.
- Modify: `static/operacion/sw.js` — invalidación obligatoria de caché PWA.
- Modify: `operacion/tests_higiene.py` — contratos de captura, permisos y concurrencia funcional.

### Lectura en Mantenimiento y Notificaciones

- Modify: `mantenimiento/services_history.py` — línea de tiempo de constataciones y métricas de continuidad.
- Modify: `mantenimiento/api_v2.py` — descarga privada de evidencia de las constataciones.
- Modify: `mantenimiento/views.py` — conteo visible en la bandeja clásica.
- Modify: `templates/mantenimiento/dashboard.html` — días/constataciones en la tarjeta principal.
- Modify: `templates/mantenimiento/pwa.html` — línea de tiempo en la app de Mantenimiento.
- Create: `core/notificaciones_bandeja.py` — agrupamiento de avisos por falla principal.
- Modify: `core/views.py` — paginar grupos y leer el grupo completo.
- Modify: `core/navigation.py` — contador por grupos activos no leídos.
- Modify: `core/templates/core/notificaciones.html` — una tarjeta por grupo.
- Modify: `core/tests.py` — bandeja, lectura y contador agrupados.
- Modify: `mantenimiento/tests.py` y `mantenimiento/tests_v2.py` — bandeja y detalle.

### Consolidación histórica

- Create: `operacion/services_higiene_consolidacion.py` — propuesta determinista y aplicación idempotente.
- Create: `operacion/tests_higiene_consolidacion.py` — ciclos, ambiguos, idempotencia y preservación.
- Create: `mantenimiento/views_consolidacion_higiene.py` — vista previa y aplicación seleccionada.
- Modify: `mantenimiento/urls.py` — rutas protegidas.
- Create: `templates/mantenimiento/consolidacion_higiene.html` — tabla revisable.
- Create: `static/css/template_modules/mantenimiento-consolidacion-higiene.css` — presentación de la tabla.
- Create: `mantenimiento/tests_consolidacion_higiene.py` — permisos y contrato async.
- Modify: `docs/ux/action-context-coverage.md` — registrar la nueva acción.

### Caché y validación

- Modify: `static/mantenimiento/sw.js` y `templates/mantenimiento/pwa.html` — versión sincronizada del SW.
- Modify: `mantenimiento/tests.py` — versión y comportamiento del SW.

## Task 1: Preparar el entorno aislado y fijar el contrato de datos

**Files:**
- Modify: `operacion/models.py`
- Create: `operacion/migrations/0006_higiene_continuidad_falla.py`
- Test: `operacion/tests_higiene.py`

- [ ] **Step 1: Crear el worktree registrado y PostgreSQL aislado**

Run:

```bash
bash scripts/task_workspace_start.sh \
  --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 \
  --root /Users/mauricioburgos/Downloads/codex_worktrees \
  --task fallas_checklist_continuidad \
  --branch codex/fallas-checklist-continuidad \
  --owner codex \
  --scope 'operacion fallas mantenimiento core templates static docs/ux; continuidad de fallas desde higiene'
cd /Users/mauricioburgos/Downloads/codex_worktrees/fallas_checklist_continuidad
bash scripts/git_workspace_preflight.sh --write
COMPOSE_PROJECT_NAME=erp_fallas_checklist DB_HOST_PORT=55462 docker compose up -d db
COMPOSE_PROJECT_NAME=erp_fallas_checklist DB_HOST_PORT=55462 docker compose exec -T db pg_isready -U postgres
export APP_ENV=development
export ALLOW_INSECURE_LOCAL_SECRET_KEY=1
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55462/pastelerias_erp
python3 manage.py migrate
python3 manage.py migrate --check
python3 manage.py check
```

Expected: PostgreSQL responde `accepting connections`, migraciones pendientes `0` y `System check identified no issues`.

- [ ] **Step 2: Escribir la prueba fallida de relación muchos-a-uno**

Agregar a `operacion/tests_higiene.py`:

```python
def test_varias_revisiones_pueden_apuntar_a_la_misma_falla(self):
    reporte = ReporteFalla.objects.create(
        sucursal=self.payan,
        categoria=self.categoria_instalacion,
        tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
        area_instalacion="Baños",
        titulo="Sanitario sin funcionar",
        descripcion="No descarga agua.",
        justificacion_sin_foto="Prueba automatizada.",
        reportado_por=self.operadora,
    )
    for fecha in ("2026-09-25", "2026-09-26"):
        registro = RegistroHigiene.objects.create(
            tipo=RegistroHigiene.TIPO_BANOS,
            sucursal=self.payan,
            fecha=fecha,
            clave_instancia=f"clientes-{fecha}",
            plantilla_version="2026.1",
            creado_por=self.operadora,
        )
        RespuestaHigiene.objects.create(
            registro=registro,
            punto_clave="banos_sanitario",
            seccion="Limpieza de baños",
            punto_revision="Sanitario limpio y funcional",
            respuesta=RespuestaHigiene.RESPUESTA_NO_CUMPLE,
            requiere_seguimiento=True,
            tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
            area_instalacion="Baños",
            reporte_falla=reporte,
            continuidad_falla=RespuestaHigiene.CONTINUIDAD_IGUAL,
        )

    self.assertEqual(reporte.constataciones_higiene.count(), 2)
```

- [ ] **Step 3: Ejecutar la prueba y comprobar que falla por el contrato actual**

Run:

```bash
python3 manage.py test operacion.tests_higiene.HigieneDiariaTests.test_varias_revisiones_pueden_apuntar_a_la_misma_falla
```

Expected: FAIL porque `continuidad_falla` no existe o porque el `OneToOneField` impide el segundo vínculo.

- [ ] **Step 4: Cambiar el modelo sin perder claves existentes**

Reemplazar el campo en `operacion/models.py` y agregar las constantes:

```python
class RespuestaHigiene(models.Model):
    CONTINUIDAD_INICIAL = "INICIAL"
    CONTINUIDAD_IGUAL = "IGUAL"
    CONTINUIDAD_CAMBIO = "CAMBIO"
    CONTINUIDAD_CORRECCION = "CORRECCION_PENDIENTE"
    CONTINUIDAD_CHOICES = [
        (CONTINUIDAD_INICIAL, "Detección inicial"),
        (CONTINUIDAD_IGUAL, "Sigue igual"),
        (CONTINUIDAD_CAMBIO, "Cambió o empeoró"),
        (CONTINUIDAD_CORRECCION, "Corrección pendiente de validar"),
    ]

    continuidad_falla = models.CharField(
        max_length=24,
        choices=CONTINUIDAD_CHOICES,
        blank=True,
        default="",
        db_index=True,
    )
    reporte_falla = models.ForeignKey(
        "fallas.ReporteFalla",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="constataciones_higiene",
    )
```

Run:

```bash
python3 manage.py makemigrations operacion --name higiene_continuidad_falla
python3 manage.py sqlmigrate operacion 0006
python3 manage.py migrate
python3 manage.py migrate --check
```

Expected: la migración usa `AlterField` y `AddField`; no elimina ni recrea la columna `reporte_falla_id`.

- [ ] **Step 5: Probar compatibilidad y confirmar**

Run:

```bash
python3 manage.py test operacion.tests_higiene
python3 manage.py check
git add operacion/models.py operacion/migrations/0006_higiene_continuidad_falla.py operacion/tests_higiene.py
git commit -m "feat(operacion): permitir continuidad diaria de una falla"
```

Expected: tests PASS y commit limitado al contrato de datos.

## Task 2: Implementar coincidencia estructurada y enlace transaccional

**Files:**
- Create: `operacion/services_higiene_fallas.py`
- Modify: `operacion/services_higiene.py`
- Modify: `operacion/services_fallas.py`
- Test: `operacion/tests_higiene.py`

- [ ] **Step 1: Escribir pruebas fallidas para misma falla, problema distinto y reporte cerrado**

Agregar a `operacion/tests_higiene.py`:

```python
def _hallazgo_banos(self, decision="AUTO", reporte_id=None, observacion="No descarga agua"):
    row = {
        "key": "banos_sanitario",
        "respuesta": "NO_CUMPLE",
        "observacion": observacion,
        "corregido": False,
        "requiere_seguimiento": True,
        "tipo_objetivo": "INSTALACION",
        "categoria_id": self.categoria_instalacion.id,
        "area_instalacion": "Baños",
        "prioridad": "alta",
        "falla_decision": decision,
    }
    if reporte_id:
        row["reporte_falla_id"] = reporte_id
    return row

def _crear_falla_higiene_abierta(self, sucursal=None, usuario=None):
    sucursal = sucursal or self.payan
    usuario = usuario or self.operadora
    indice = ReporteFalla.objects.count() + 1
    reporte = ReporteFalla.objects.create(
        sucursal=sucursal,
        categoria=self.categoria_instalacion,
        tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
        area_instalacion="Baños",
        titulo="Limpieza de baños · Sanitario limpio y funcional",
        descripcion="No descarga agua.",
        justificacion_sin_foto="Preparación de prueba.",
        prioridad=ReporteFalla.PRIORIDAD_ALTA,
        reportado_por=usuario,
    )
    registro = RegistroHigiene.objects.create(
        tipo=RegistroHigiene.TIPO_BANOS,
        sucursal=sucursal,
        fecha="2026-09-24",
        clave_instancia=f"base-{indice}",
        plantilla_version="2026.1",
        creado_por=usuario,
    )
    RespuestaHigiene.objects.create(
        registro=registro,
        punto_clave="banos_sanitario",
        seccion="Limpieza de baños",
        punto_revision="Sanitario limpio y funcional",
        respuesta=RespuestaHigiene.RESPUESTA_NO_CUMPLE,
        observacion="No descarga agua",
        requiere_seguimiento=True,
        tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
        area_instalacion="Baños",
        reporte_falla=reporte,
        continuidad_falla=RespuestaHigiene.CONTINUIDAD_INICIAL,
    )
    return reporte

def test_sigue_igual_en_otro_dia_reutiliza_la_falla(self):
    self.client.force_login(self.operadora)
    with mock.patch("django.utils.timezone.localdate", return_value=date(2026, 9, 25)):
        inicial = self._guardar(
            tipo="BANOS",
            clave_instancia="clientes-ronda-1",
            respuestas=[self._hallazgo_banos()],
            archivos={"evidencia_banos_sanitario": self._foto("inicial.png")},
        )
    reporte_id = inicial.json()["reporte_falla_ids"][0]
    with mock.patch("django.utils.timezone.localdate", return_value=date(2026, 9, 26)):
        continuidad = self._guardar(
            tipo="BANOS",
            clave_instancia="clientes-ronda-1",
            respuestas=[self._hallazgo_banos("MISMA", reporte_id)],
        )

    self.assertEqual(continuidad.status_code, 201)
    self.assertEqual(ReporteFalla.objects.count(), 1)
    self.assertEqual(ReporteFalla.objects.get().constataciones_higiene.count(), 2)

def test_problema_distinto_crea_otro_reporte_aunque_coincida_el_punto(self):
    principal = self._crear_falla_higiene_abierta()
    self.client.force_login(self.operadora)
    response = self._guardar(
        tipo="BANOS",
        clave_instancia="clientes-ronda-2",
        respuestas=[self._hallazgo_banos("DISTINTA", principal.id, "La tapa está rota")],
        archivos={"evidencia_banos_sanitario": self._foto("distinta.png")},
    )
    self.assertEqual(response.status_code, 201)
    self.assertEqual(ReporteFalla.objects.count(), 2)

def test_reporte_cerrado_no_acepta_continuidad_y_se_convierte_en_reincidencia(self):
    principal = self._crear_falla_higiene_abierta()
    principal.estatus = ReporteFalla.ESTATUS_CERRADO
    principal.fecha_cierre = timezone.now()
    principal.save(update_fields=["estatus", "fecha_cierre"])
    self.client.force_login(self.operadora)
    response = self._guardar(
        tipo="BANOS",
        clave_instancia="clientes-ronda-3",
        respuestas=[self._hallazgo_banos("MISMA", principal.id)],
        archivos={"evidencia_banos_sanitario": self._foto("reincidencia.png")},
    )
    self.assertEqual(response.status_code, 201)
    self.assertEqual(ReporteFalla.objects.count(), 2)
```

Agregar imports reales: `from datetime import date`, `from unittest import mock` y `from django.utils import timezone`.

- [ ] **Step 2: Ejecutar las pruebas y verificar los fallos**

Run:

```bash
python3 manage.py test \
  operacion.tests_higiene.HigieneDiariaTests.test_sigue_igual_en_otro_dia_reutiliza_la_falla \
  operacion.tests_higiene.HigieneDiariaTests.test_problema_distinto_crea_otro_reporte_aunque_coincida_el_punto \
  operacion.tests_higiene.HigieneDiariaTests.test_reporte_cerrado_no_acepta_continuidad_y_se_convierte_en_reincidencia
```

Expected: FAIL porque todas las detecciones todavía crean un reporte.

- [ ] **Step 3: Crear el servicio de identidad y coincidencias**

Crear `operacion/services_higiene_fallas.py` con esta interfaz:

```python
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from django.core.exceptions import ValidationError
from django.db import connection, transaction

from activos.models import Activo
from fallas.models import BitacoraFalla, CategoriaFalla, ReporteFalla


ESTATUS_ACTIVOS = (
    ReporteFalla.ESTATUS_ABIERTO,
    ReporteFalla.ESTATUS_REVISION,
    ReporteFalla.ESTATUS_PROCESO,
)


class FallaHigieneConflict(Exception):
    def __init__(self, candidatos):
        super().__init__("Ya existe una falla activa para este punto.")
        self.candidatos = candidatos


@dataclass(frozen=True)
class IdentidadFallaHigiene:
    sucursal_id: int
    tipo_checklist: str
    punto_clave: str
    tipo_objetivo: str
    categoria_id: int
    activo_id: int | None
    area_instalacion: str

    @property
    def lock_key(self) -> int:
        raw = "|".join(
            str(value)
            for value in (
                self.sucursal_id,
                self.tipo_checklist,
                self.punto_clave,
                self.tipo_objetivo,
                self.categoria_id,
                self.activo_id or 0,
                self.area_instalacion.casefold().strip(),
            )
        )
        return int.from_bytes(hashlib.blake2b(raw.encode(), digest_size=8).digest(), "big", signed=True)


def fallas_coincidentes(identidad: IdentidadFallaHigiene):
    queryset = ReporteFalla.objects.filter(
        sucursal_id=identidad.sucursal_id,
        categoria_id=identidad.categoria_id,
        tipo_objetivo=identidad.tipo_objetivo,
        estatus__in=ESTATUS_ACTIVOS,
        duplicado_de__isnull=True,
        constataciones_higiene__registro__tipo=identidad.tipo_checklist,
        constataciones_higiene__punto_clave=identidad.punto_clave,
    )
    if identidad.tipo_objetivo == ReporteFalla.OBJETIVO_EQUIPO:
        queryset = queryset.filter(activo_relacionado_id=identidad.activo_id)
    else:
        queryset = queryset.filter(
            activo_relacionado__isnull=True,
            area_instalacion__iexact=identidad.area_instalacion.strip(),
        )
    return queryset.distinct().order_by("fecha_reporte", "id")


def bloquear_identidad(identidad: IdentidadFallaHigiene) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", [identidad.lock_key])


def identidad_desde_consulta(*, sucursal, params):
    tipo = str(params.get("tipo") or "").strip()
    punto_clave = str(params.get("punto_clave") or "").strip()
    tipo_objetivo = str(params.get("tipo_objetivo") or "").strip().upper()
    categoria = CategoriaFalla.objects.filter(
        pk=params.get("categoria_id"),
        activo=True,
    ).first()
    if not tipo or not punto_clave or categoria is None:
        raise ValidationError("Completa el punto y la categoría antes de buscar coincidencias.")
    activo = None
    area = str(params.get("area_instalacion") or "").strip()
    if tipo_objetivo == ReporteFalla.OBJETIVO_EQUIPO:
        activo = Activo.objects.filter(
            pk=params.get("activo_id"),
            sucursal=sucursal,
            activo=True,
        ).first()
        if activo is None or categoria.tipo != CategoriaFalla.TIPO_EQUIPO:
            raise ValidationError("Selecciona un equipo y categoría válidos para tu sucursal.")
        area = ""
    elif tipo_objetivo == ReporteFalla.OBJETIVO_INSTALACION:
        if not area or categoria.tipo != CategoriaFalla.TIPO_INSTALACION:
            raise ValidationError("Selecciona un área y categoría válidas de instalación.")
    else:
        raise ValidationError("Clasifica el hallazgo como equipo o instalación.")
    return IdentidadFallaHigiene(
        sucursal_id=sucursal.pk,
        tipo_checklist=tipo,
        punto_clave=punto_clave,
        tipo_objetivo=tipo_objetivo,
        categoria_id=categoria.pk,
        activo_id=activo.pk if activo else None,
        area_instalacion=area,
    )


def registrar_constatacion(*, respuesta, reporte, decision, usuario):
    respuesta.reporte_falla = reporte
    respuesta.continuidad_falla = decision
    respuesta.requiere_seguimiento = True
    respuesta.save(update_fields=["reporte_falla", "continuidad_falla", "requiere_seguimiento"])
    etiquetas = {
        respuesta.CONTINUIDAD_IGUAL: "La sucursal confirmó que la falla sigue igual.",
        respuesta.CONTINUIDAD_CAMBIO: "La sucursal indicó que la falla cambió o empeoró.",
        respuesta.CONTINUIDAD_CORRECCION: "La sucursal solicitó validar una corrección.",
    }
    BitacoraFalla.objects.create(
        reporte=reporte,
        usuario=usuario,
        comentario=f"{etiquetas[decision]} Checklist #{respuesta.registro_id}, punto {respuesta.punto_revision}.",
    )
```

- [ ] **Step 4: Integrar el servicio en `guardar_registro_higiene`**

En `operacion/services_higiene.py`, construir `IdentidadFallaHigiene` con los datos ya normalizados y aplicar esta decisión dentro del bucle transaccional:

```python
decision = item["falla_decision"]
identidad = IdentidadFallaHigiene(
    sucursal_id=sucursal.id,
    tipo_checklist=tipo,
    punto_clave=item["clave"],
    tipo_objetivo=item["tipo_objetivo"],
    categoria_id=item["categoria"].id,
    activo_id=item["activo"].id if item["activo"] else None,
    area_instalacion=item["area_instalacion"],
)
bloquear_identidad(identidad)
coincidentes = list(fallas_coincidentes(identidad))
reporte = None

if decision in {"MISMA", "CAMBIO", "CORRECCION_PENDIENTE"}:
    reporte = fallas_coincidentes(identidad).filter(pk=item["reporte_falla_id"]).first()
    if reporte is not None:
        continuidad = {
            "MISMA": RespuestaHigiene.CONTINUIDAD_IGUAL,
            "CAMBIO": RespuestaHigiene.CONTINUIDAD_CAMBIO,
            "CORRECCION_PENDIENTE": RespuestaHigiene.CONTINUIDAD_CORRECCION,
        }[decision]
        registrar_constatacion(
            respuesta=respuesta,
            reporte=reporte,
            decision=continuidad,
            usuario=user,
        )
    else:
        decision = "AUTO"

if reporte is None:
    if decision == "AUTO" and coincidentes:
        raise FallaHigieneConflict(coincidentes)
    reporte = crear_reporte_falla(
        sucursal=sucursal,
        usuario=user,
        categoria=item["categoria"],
        tipo_objetivo=item["tipo_objetivo"],
        activo_relacionado=item["activo"],
        area_instalacion=item["area_instalacion"],
        titulo=f"{plantilla['titulo']} · {item['punto']['etiqueta']}",
        descripcion=f"Hallazgo detectado en higiene diaria ({item['punto']['seccion']}): {item['observacion']}",
        prioridad=item["prioridad"],
        evidencia=respuesta.evidencia.name if respuesta.evidencia else None,
        comentario_bitacora=f"Reporte creado automáticamente desde Higiene diaria, registro #{registro.pk}.",
    )
    registrar_constatacion(
        respuesta=respuesta,
        reporte=reporte,
        decision=RespuestaHigiene.CONTINUIDAD_INICIAL,
        usuario=user,
    )
```

Normalizar `falla_decision` con valor predeterminado `AUTO`, validar `reporte_falla_id` entero para decisiones de continuidad y exigir evidencia para `AUTO`, `DISTINTA`, `CAMBIO` y `CORRECCION_PENDIENTE`. `MISMA` admite foto opcional.

En `operacion/views.py`, capturar `FallaHigieneConflict` antes de `ValidationError` y devolver un `409` sin confirmar la transacción:

```python
except FallaHigieneConflict as exc:
    return JsonResponse(
        {
            "error": str(exc),
            "existing_reports": [
                {"id": row.pk, "titulo": row.titulo, "estatus": row.get_estatus_display()}
                for row in exc.candidatos
            ],
        },
        status=409,
    )
```

- [ ] **Step 5: Añadir avisos relevantes sin avisar por `MISMA`**

En `operacion/services_fallas.py`, agregar:

```python
def notificar_evento_higiene(reporte, respuesta, actor) -> None:
    if respuesta.continuidad_falla == respuesta.CONTINUIDAD_IGUAL:
        dias = (respuesta.registro.fecha - reporte.fecha_reporte.date()).days
        if dias not in {3, 6}:
            return
        titulo = f"Falla sin resolver por {dias} días en {reporte.sucursal.nombre}"
    elif respuesta.continuidad_falla == respuesta.CONTINUIDAD_CAMBIO:
        titulo = f"Falla cambió o empeoró en {reporte.sucursal.nombre}"
    elif respuesta.continuidad_falla == respuesta.CONTINUIDAD_CORRECCION:
        titulo = f"Validar corrección en {reporte.sucursal.nombre}"
    else:
        return
    crear_notificaciones(
        _usuarios_mantenimiento(),
        titulo=titulo,
        mensaje=f"{reporte.titulo} · {respuesta.observacion}",
        url=f"/mantenimiento/?open=falla:{reporte.pk}",
        actor=actor,
        objeto_tipo="ReporteFalla",
        objeto_id=reporte.pk,
    )
```

Invocarlo con `transaction.on_commit` únicamente después de guardar la constatación.

- [ ] **Step 6: Probar concurrencia real sobre PostgreSQL**

Agregar a `operacion/tests_higiene.py` una clase `TransactionTestCase` con dos usuarios de la misma sucursal y esta prueba de carrera inicial:

```python
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from django.db import close_old_connections
from django.test import Client, TransactionTestCase


class HigieneConcurrencyTests(TransactionTestCase):
    reset_sequences = True

    def setUp(self):
        users = get_user_model()
        self.sucursal = Sucursal.objects.create(codigo="HIG-CONC", nombre="Higiene concurrente")
        self.categoria = CategoriaFalla.objects.create(
            nombre="Plomería concurrente",
            tipo=CategoriaFalla.TIPO_INSTALACION,
        )
        self.usuarios = []
        for indice in (1, 2):
            user = users.objects.create_user(username=f"higiene.concurrente.{indice}")
            UserProfile.objects.create(user=user, sucursal=self.sucursal)
            UserModuleAccess.objects.create(user=user, module="fallas", access=ACCESS_MANAGE)
            self.usuarios.append(user)

    def _post(self, *, user, instancia, decision="AUTO", reporte_id=None, foto=False):
        close_old_connections()
        client = Client()
        client.force_login(user)
        respuesta = {
            "key": "banos_sanitario",
            "respuesta": "NO_CUMPLE",
            "observacion": "No descarga agua",
            "corregido": False,
            "requiere_seguimiento": True,
            "tipo_objetivo": "INSTALACION",
            "categoria_id": self.categoria.id,
            "area_instalacion": "Baños",
            "prioridad": "alta",
            "falla_decision": decision,
        }
        if reporte_id:
            respuesta["reporte_falla_id"] = reporte_id
        data = {
            "tipo": "BANOS",
            "clave_instancia": instancia,
            "hora": "09:30",
            "respuestas": json.dumps([respuesta]),
        }
        if foto:
            data["evidencia_banos_sanitario"] = SimpleUploadedFile(
                f"{instancia}.png",
                PNG_1PX,
                content_type="image/png",
            )
        response = client.post(
            reverse("operacion:higiene_guardar"),
            data,
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        close_old_connections()
        return response.status_code

    def _crear_principal_con_respuesta(self):
        reporte = ReporteFalla.objects.create(
            sucursal=self.sucursal,
            categoria=self.categoria,
            tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
            area_instalacion="Baños",
            titulo="Limpieza de baños · Sanitario limpio y funcional",
            descripcion="No descarga agua.",
            justificacion_sin_foto="Preparación concurrente.",
            prioridad=ReporteFalla.PRIORIDAD_ALTA,
            reportado_por=self.usuarios[0],
        )
        registro = RegistroHigiene.objects.create(
            tipo=RegistroHigiene.TIPO_BANOS,
            sucursal=self.sucursal,
            fecha="2026-09-24",
            clave_instancia="base-concurrente",
            plantilla_version="2026.1",
            creado_por=self.usuarios[0],
        )
        RespuestaHigiene.objects.create(
            registro=registro,
            punto_clave="banos_sanitario",
            seccion="Limpieza de baños",
            punto_revision="Sanitario limpio y funcional",
            respuesta=RespuestaHigiene.RESPUESTA_NO_CUMPLE,
            observacion="No descarga agua",
            requiere_seguimiento=True,
            tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
            area_instalacion="Baños",
            reporte_falla=reporte,
            continuidad_falla=RespuestaHigiene.CONTINUIDAD_INICIAL,
        )
        return reporte

    def test_dos_detecciones_iniciales_simultaneas_no_crean_dos_principales(self):
        barrier = Barrier(2)

        def enviar(indice):
            barrier.wait()
            return self._post(
                user=self.usuarios[indice],
                instancia=f"clientes-ronda-{indice + 1}",
                foto=True,
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            statuses = sorted(pool.map(enviar, (0, 1)))

        self.assertEqual(statuses, [201, 409])
        self.assertEqual(ReporteFalla.objects.count(), 1)

    def test_dos_confirmaciones_simultaneas_se_enlazan_al_mismo_principal(self):
        principal = self._crear_principal_con_respuesta()
        barrier = Barrier(2)

        def confirmar(indice):
            barrier.wait()
            return self._post(
                user=self.usuarios[indice],
                instancia=f"personal-ronda-{indice + 1}",
                decision="MISMA",
                reporte_id=principal.id,
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            statuses = sorted(pool.map(confirmar, (0, 1)))

        self.assertEqual(statuses, [201, 201])
        self.assertEqual(principal.constataciones_higiene.count(), 3)
```

No usar mocks de bloqueo; la prueba debe ejecutar `pg_advisory_xact_lock` real.

- [ ] **Step 7: Ejecutar pruebas y confirmar**

Run:

```bash
python3 manage.py test operacion.tests_higiene fallas.tests_duplicados
python3 manage.py check
git add operacion/services_higiene_fallas.py operacion/services_higiene.py operacion/services_fallas.py operacion/tests_higiene.py
git commit -m "feat(operacion): reutilizar fallas activas desde higiene"
```

Expected: una continuidad no crea reporte ni aviso nuevo; cambio/corrección sí genera aviso; cerrada genera reincidencia.

## Task 3: Incorporar la decisión asistida en el checklist

**Files:**
- Modify: `operacion/views.py`
- Modify: `operacion/urls.py`
- Modify: `templates/operacion/higiene_home.html`
- Modify: `static/operacion/higiene.js`
- Modify: `static/operacion/higiene.css`
- Modify: `static/operacion/sw.js`
- Test: `operacion/tests_higiene.py`

- [ ] **Step 1: Escribir pruebas fallidas del endpoint de coincidencias**

Agregar:

```python
def test_coincidencias_higiene_solo_devuelve_fallas_de_la_sucursal(self):
    propia = self._crear_falla_higiene_abierta()
    ajena = self._crear_falla_higiene_abierta(sucursal=self.leyva, usuario=self.otra_operadora)
    self.client.force_login(self.operadora)
    response = self.client.get(
        reverse("operacion:higiene_fallas_coincidentes"),
        {
            "tipo": "BANOS",
            "punto_clave": "banos_sanitario",
            "tipo_objetivo": "INSTALACION",
            "categoria_id": self.categoria_instalacion.id,
            "area_instalacion": "Baños",
        },
    )
    self.assertEqual(response.status_code, 200)
    self.assertEqual([row["id"] for row in response.json()["results"]], [propia.id])
    self.assertNotEqual(propia.id, ajena.id)

def test_coincidencias_higiene_rechaza_usuario_sin_sucursal(self):
    self.client.force_login(self.supervisora)
    response = self.client.get(reverse("operacion:higiene_fallas_coincidentes"))
    self.assertEqual(response.status_code, 403)
```

- [ ] **Step 2: Crear la ruta y vista de solo lectura**

En `operacion/urls.py`:

```python
path(
    "higiene/fallas-coincidentes/",
    views.higiene_fallas_coincidentes,
    name="higiene_fallas_coincidentes",
),
```

En `operacion/views.py`:

```python
@login_required
@require_GET
def higiene_fallas_coincidentes(request):
    sucursal = sucursal_higiene_usuario(request.user)
    if sucursal is None:
        return JsonResponse({"error": "Tu sesión no tiene una sucursal operativa."}, status=403)
    try:
        identidad = identidad_desde_consulta(sucursal=sucursal, params=request.GET)
    except ValidationError as exc:
        return JsonResponse({"error": exc.messages[0]}, status=400)
    results = [
        {
            "id": row.pk,
            "titulo": row.titulo,
            "estatus": row.get_estatus_display(),
            "fecha_reporte": timezone.localtime(row.fecha_reporte).isoformat(),
        "ultima_confirmacion": (
            row.constataciones_higiene.order_by("-registro__fecha", "-id")
            .values_list("registro__fecha", flat=True)
            .first()
            .isoformat()
        ),
        }
        for row in fallas_coincidentes(identidad)
    ]
    return JsonResponse({"results": results})
```

`identidad_desde_consulta` vivirá en `services_higiene_fallas.py` y validará tipo, punto, categoría, objetivo, equipo/área y pertenencia del activo a la sucursal.

- [ ] **Step 3: Añadir el panel accesible por punto**

Dentro de `data-failure-fields` en `templates/operacion/higiene_home.html` agregar:

```html
<section class="failure-match" data-failure-match hidden aria-live="polite">
  <p data-failure-match-title></p>
  <div data-failure-match-options></div>
  <fieldset data-failure-decision hidden>
    <legend>¿Qué ocurre hoy?</legend>
    <label><input type="radio" data-failure-action value="MISMA"> Sigue siendo la misma</label>
    <label><input type="radio" data-failure-action value="CAMBIO"> Empeoró o cambió</label>
    <label><input type="radio" data-failure-action value="DISTINTA"> Es otro problema</label>
    <label><input type="radio" data-failure-action value="CORRECCION_PENDIENTE"> Ya quedó corregido; solicitar validación</label>
  </fieldset>
</section>
```

Crear `static/css/template_modules/mantenimiento-consolidacion-higiene.css` con tabla desplazable, encabezado fijo y foco visible:

```css
.higiene-consolidacion-table { overflow-x: auto; max-height: 70vh; }
.higiene-consolidacion-table table { width: 100%; border-collapse: collapse; }
.higiene-consolidacion-table th { position: sticky; top: 0; z-index: 1; background: #8b2252; color: #fffaf5; }
.higiene-consolidacion-table th,
.higiene-consolidacion-table td { padding: 0.75rem; border-bottom: 1px solid #eadde3; text-align: left; vertical-align: top; }
.higiene-consolidacion-table input:focus-visible,
.higiene-consolidacion-table button:focus-visible { outline: 3px solid #c9a84c; outline-offset: 3px; }
```

Envolver cada tabla con `<div class="higiene-consolidacion-table">` y cargar la hoja desde el bloque CSS del template.

Mantener el botón de guardar como único control mutante; este panel solo prepara la decisión.

- [ ] **Step 4: Consultar coincidencias y serializar la decisión**

Agregar a `static/operacion/higiene.js`:

```javascript
async function loadFailureMatches(point) {
  const form = point.closest("form");
  const params = new URLSearchParams({
    tipo: form.querySelector('[name="tipo"]').value,
    punto_clave: point.dataset.key,
    tipo_objetivo: point.querySelector("[data-target-type]").value,
    categoria_id: point.querySelector("[data-category]").value,
    activo_id: point.querySelector("[data-asset]").value,
    area_instalacion: point.querySelector("[data-area]").value.trim()
  });
  const response = await fetch("/app/higiene/fallas-coincidentes/?" + params.toString(), {
    credentials: "same-origin"
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "No fue posible buscar fallas activas.");
  const panel = point.querySelector("[data-failure-match]");
  const options = point.querySelector("[data-failure-match-options]");
  const decision = point.querySelector("[data-failure-decision]");
  options.replaceChildren();
  payload.results.forEach(function (item) {
    const label = document.createElement("label");
    const radio = document.createElement("input");
    radio.type = "radio";
    radio.name = "existing_failure_" + point.dataset.key;
    radio.value = String(item.id);
    label.append(radio, document.createTextNode(" #" + item.id + " · " + item.titulo + " · " + item.estatus));
    options.append(label);
  });
  panel.hidden = false;
  decision.hidden = payload.results.length === 0;
  panel.dataset.loaded = "true";
  panel.dataset.hasMatches = payload.results.length ? "true" : "false";
}

function failureDecision(point) {
  const panel = point.querySelector("[data-failure-match]");
  const selectedReport = point.querySelector('[name="existing_failure_' + point.dataset.key + '"]:checked');
  const selectedAction = point.querySelector("[data-failure-action]:checked");
  if (!panel || panel.dataset.hasMatches !== "true") {
    return { falla_decision: "AUTO" };
  }
  return {
    falla_decision: selectedAction ? selectedAction.value : "",
    reporte_falla_id: selectedReport ? selectedReport.value : ""
  };
}
```

En `buildAnswers`, fusionar `failureDecision(point)` cuando `requiere_seguimiento` sea verdadero. La validación de completitud deberá exigir reporte y acción si hay coincidencias, comentario y foto para `CAMBIO`/`CORRECCION_PENDIENTE`, y foto para `DISTINTA`; `MISMA` no exige una foto nueva.

- [ ] **Step 5: Manejar carreras sin perder el formulario**

Antes del error genérico del submit, manejar `409`, ejecutar nuevamente `loadFailureMatches` para cada punto de seguimiento, regresar al primer punto coincidente y mostrar:

```javascript
if (response.status === 409) {
  const pending = points.filter(function (point) {
    return selectedValue(point, "[data-resolution]") === "SEGUIMIENTO";
  });
  await Promise.all(pending.map(loadFailureMatches));
  const target = pending[0];
  if (target) {
    const section = target.closest("[data-review-section]");
    showSection(form, Number(section.dataset.sectionIndex), true);
    target.scrollIntoView({ block: "center", behavior: reduceMotion ? "auto" : "smooth" });
  }
  showToast("Apareció una falla activa mientras llenabas el checklist. Confirma si sigue siendo la misma.", "warning");
  button.disabled = false;
  button.innerHTML = original;
  return;
}
```

No reconstruir `FormData` hasta el siguiente envío; los inputs y archivos permanecen en el formulario.

- [ ] **Step 6: Ajustar estilos y caché PWA**

Agregar a `static/operacion/higiene.css`:

```css
.failure-match {
  margin-top: 1rem;
  padding: 1rem;
  border: 1px solid rgba(139, 34, 82, 0.25);
  border-radius: 14px;
  background: #fffaf5;
}
.failure-match label {
  display: flex;
  gap: 0.65rem;
  align-items: flex-start;
  padding: 0.65rem;
}
.failure-match input:focus-visible {
  outline: 3px solid #c9a84c;
  outline-offset: 3px;
}
[data-failure-decision] {
  margin-top: 0.8rem;
  border: 0;
  padding: 0;
}
```

Cambiar:

```javascript
const CACHE_NAME = "pollyanas-app-operativa-pwa-v46-higiene-falla-continuidad";
```

Actualizar el query de `higiene.js` y `higiene.css` a `20260926-higiene-continuidad-v1`.

- [ ] **Step 7: Probar, confirmar y revisar el contrato de acción**

Run:

```bash
python3 manage.py test operacion.tests_higiene
python3 manage.py check
git add operacion/views.py operacion/urls.py templates/operacion/higiene_home.html static/operacion/higiene.js static/operacion/higiene.css static/operacion/sw.js operacion/tests_higiene.py
git commit -m "feat(operacion): confirmar continuidad desde checklist"
```

Expected: endpoint aislado por sucursal, reintento sin pérdida y SW actualizado.

## Task 4: Mostrar continuidad y agrupar notificaciones

**Files:**
- Modify: `mantenimiento/services_history.py`
- Modify: `mantenimiento/api_v2.py`
- Modify: `mantenimiento/views.py`
- Modify: `templates/mantenimiento/dashboard.html`
- Modify: `templates/mantenimiento/pwa.html`
- Create: `core/notificaciones_bandeja.py`
- Modify: `core/views.py`
- Modify: `core/navigation.py`
- Modify: `core/templates/core/notificaciones.html`
- Test: `core/tests.py`
- Test: `mantenimiento/tests.py`
- Test: `mantenimiento/tests_v2.py`

- [ ] **Step 1: Escribir pruebas fallidas de detalle y agrupamiento**

En `MaintenanceDetailV2Tests` de `mantenimiento/tests_v2.py`, importar `RegistroHigiene` y `RespuestaHigiene` y agregar esta prueba completa:

```python
def test_falla_detail_incluye_constataciones_diarias(self):
    for indice, fecha in enumerate(("2026-09-25", "2026-09-26"), start=1):
        registro = RegistroHigiene.objects.create(
            tipo=RegistroHigiene.TIPO_LIMPIEZA,
            sucursal=self.branch,
            fecha=fecha,
            clave_instancia=f"diaria-{indice}",
            plantilla_version="2026.1",
            creado_por=self.reporter,
        )
        RespuestaHigiene.objects.create(
            registro=registro,
            punto_clave="produccion_equipos_limpios",
            seccion="Producción",
            punto_revision="Equipos limpios",
            respuesta=RespuestaHigiene.RESPUESTA_NO_CUMPLE,
            observacion="El horno sigue sin encender",
            requiere_seguimiento=True,
            tipo_objetivo=ReporteFalla.OBJETIVO_EQUIPO,
            activo_relacionado=self.report.activo_relacionado,
            reporte_falla=self.report,
            continuidad_falla=(
                RespuestaHigiene.CONTINUIDAD_INICIAL
                if indice == 1
                else RespuestaHigiene.CONTINUIDAD_IGUAL
            ),
        )

    self.client.force_login(self.user)
    detail = self.client.get(f"/api/mantenimiento/v2/items/falla/{self.report.pk}/")
    self.assertEqual(detail.status_code, 200)
    self.assertEqual(detail.json()["continuidad"]["total"], 2)
    self.assertEqual(detail.json()["continuidad"]["primera_fecha"], "2026-09-25")
    self.assertEqual(detail.json()["continuidad"]["ultima_fecha"], "2026-09-26")
    self.assertEqual(len(detail.json()["constataciones_higiene"]), 2)
```

En `core/tests.py` agregar:

```python
def _falla_notificable(self, titulo):
    sucursal, _ = Sucursal.objects.get_or_create(
        codigo="NOTIF-FALLA",
        defaults={"nombre": "Las Glorias", "activa": True},
    )
    categoria, _ = CategoriaFalla.objects.get_or_create(
        nombre="Instalaciones notificación",
        defaults={"tipo": CategoriaFalla.TIPO_INSTALACION},
    )
    return ReporteFalla.objects.create(
        sucursal=sucursal,
        categoria=categoria,
        tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
        area_instalacion="Baños",
        titulo=titulo,
        descripcion="No descarga agua.",
        justificacion_sin_foto="Prueba automatizada.",
        reportado_por=self.actor,
    )

def _grupo_falla_notificable(self):
    principal = self._falla_notificable("Sanitario")
    repetida = self._falla_notificable("Sanitario otra vez")
    repetida.duplicado_de = principal
    repetida.save(update_fields=["duplicado_de"])
    notificaciones = [
        Notificacion.objects.create(
            usuario=self.user,
            titulo="Nueva falla en Las Glorias",
            objeto_tipo="ReporteFalla",
            objeto_id=str(reporte.pk),
            url="/mantenimiento/",
        )
        for reporte in (principal, repetida)
    ]
    return principal, notificaciones

def test_notificaciones_de_reportes_ligados_cuentan_como_un_grupo(self):
    principal = self._falla_notificable("Sanitario")
    repetida = self._falla_notificable("Sanitario otra vez")
    repetida.duplicado_de = principal
    repetida.save(update_fields=["duplicado_de"])
    for reporte in (principal, repetida):
        Notificacion.objects.create(
            usuario=self.user,
            titulo="Nueva falla en Las Glorias",
            objeto_tipo="ReporteFalla",
            objeto_id=str(reporte.pk),
            url="/mantenimiento/",
        )

    response = self.client.get("/notificaciones/")
    self.assertEqual(response.context["pendientes_count"], 1)
    self.assertContains(response, "2 avisos agrupados")

def test_abrir_grupo_marca_todos_sus_avisos_como_leidos(self):
    principal, notificaciones = self._grupo_falla_notificable()
    response = self.client.post(f"/notificaciones/{notificaciones[0].pk}/leer/")
    self.assertRedirects(response, f"/mantenimiento/?open=falla:{principal.pk}", fetch_redirect_response=False)
    self.assertFalse(Notificacion.objects.filter(pk__in=[row.pk for row in notificaciones], leida=False).exists())
```

Agregar a los imports de `core/tests.py` `CategoriaFalla` y `ReporteFalla` desde `fallas.models` si aún no están presentes.

- [ ] **Step 2: Proyectar constataciones en el detalle de Mantenimiento**

En `mantenimiento/services_history.py`, consultar las respuestas autorizadas del principal y sus duplicados:

```python
from operacion.models import RespuestaHigiene

constataciones = list(
    RespuestaHigiene.objects.filter(
        Q(reporte_falla=report) | Q(reporte_falla__duplicado_de=report)
    )
    .select_related("registro", "registro__creado_por", "reporte_falla")
    .order_by("registro__fecha", "registro__hora", "id")
)
fechas = [row.registro.fecha for row in constataciones]
continuidad = {
    "total": len(constataciones),
    "primera_fecha": fechas[0].isoformat() if fechas else None,
    "ultima_fecha": fechas[-1].isoformat() if fechas else None,
}
constataciones_payload = [
    {
        "id": row.pk,
        "fecha": row.registro.fecha.isoformat(),
        "hora": row.registro.hora.isoformat() if row.registro.hora else None,
        "persona": _person(row.registro.creado_por),
        "punto": row.punto_revision,
        "observacion": row.observacion,
        "tipo": row.continuidad_falla,
        "tipo_etiqueta": row.get_continuidad_falla_display() if row.continuidad_falla else "",
        "evidencia": _evidence_payload("higiene_constatacion", row.pk, row.evidencia),
        "reporte_origen_id": row.reporte_falla_id,
    }
    for row in constataciones
]
```

Agregar `continuidad` y `constataciones_higiene` al payload de `item_detail`; en `_branch_falla_item` y `inbox_rows` incluir el total y la última fecha sin consultas N+1 mediante anotaciones o una precarga separada por IDs.

Agregar este agregador a `mantenimiento/services_history.py` y usarlo después de cargar los IDs principales:

```python
from django.db.models import Case, Count, DateField, F, IntegerField, Max, Min, When


def continuidad_por_principal(report_ids):
    rows = (
        RespuestaHigiene.objects.filter(
            Q(reporte_falla_id__in=report_ids)
            | Q(reporte_falla__duplicado_de_id__in=report_ids)
        )
        .annotate(
            principal_id=Case(
                When(
                    reporte_falla__duplicado_de_id__isnull=False,
                    then=F("reporte_falla__duplicado_de_id"),
                ),
                default=F("reporte_falla_id"),
                output_field=IntegerField(),
            )
        )
        .values("principal_id")
        .annotate(
            constataciones_total=Count("id"),
            primera_constatacion=Min("registro__fecha", output_field=DateField()),
            ultima_constatacion=Max("registro__fecha", output_field=DateField()),
        )
    )
    return {row["principal_id"]: row for row in rows}
```

En `inbox_rows`, añadir `.filter(duplicado_de__isnull=True)` al queryset de fallas y fusionar las métricas del diccionario anterior en cada payload. La bandeja V2 y la clásica deben compartir la regla de una fila por principal.

En `mantenimiento/api_v2.py`, autorizar el nuevo tipo de evidencia únicamente cuando la falla relacionada está dentro del alcance del usuario:

```python
from operacion.models import RespuestaHigiene

if kind == "higiene_constatacion":
    evidencia = RespuestaHigiene.objects.filter(
        pk=pk,
        reporte_falla_id__in=authorized_fallas(user).values("pk"),
    ).only("evidencia").first()
    return (evidencia.evidencia, "") if evidencia else (None, "")
```

- [ ] **Step 3: Renderizar métricas y línea de tiempo**

En `templates/mantenimiento/dashboard.html` mostrar, cuando exista continuidad:

```html
{% if item.constataciones_total %}
  <span class="mant-badge is-warning">
    {{ item.constataciones_total }} revisiones · última {{ item.ultima_constatacion|date:"d/m/Y" }}
  </span>
{% endif %}
```

En `openItemDetail` de `templates/mantenimiento/pwa.html`, preparar y renderizar una sección separada:

```javascript
const hygieneTimeline = data.constataciones_higiene || [];
const hygieneSection = hygieneTimeline.length
  ? `<h2>Revisiones diarias</h2><ol class="timeline">${hygieneTimeline.map(row => `
      <li>
        <strong>${esc(row.tipo_etiqueta || "Revisión diaria")}</strong>
        <p class="muted">${esc(row.fecha || "Sin fecha")} · ${esc(row.persona?.nombre || "Sistema")}</p>
        <p>${esc(row.punto || "Punto sin nombre")}: ${esc(row.observacion || "Sin observación")}</p>
        ${evidenceGallery(row.evidencia ? [row.evidencia] : [])}
      </li>`).join("")}</ol>`
  : `<div class="empty">Sin revisiones diarias relacionadas.</div>`;
```

Insertar `${hygieneSection}` antes del encabezado `Seguimiento`, manteniendo separados los eventos operativos de Mantenimiento.

- [ ] **Step 4: Crear el agrupador de notificaciones**

Crear `core/notificaciones_bandeja.py`:

```python
from collections import OrderedDict

from django.utils import timezone

from core.models import Notificacion
from fallas.models import ReporteFalla


def _principal_ids(report_ids):
    rows = ReporteFalla.objects.filter(pk__in=report_ids).values("id", "duplicado_de_id")
    return {row["id"]: row["duplicado_de_id"] or row["id"] for row in rows}


def agrupar_notificaciones(user):
    rows = list(Notificacion.objects.filter(usuario=user).select_related("actor"))
    report_ids = {
        int(row.objeto_id)
        for row in rows
        if row.objeto_tipo == "ReporteFalla" and row.objeto_id.isdigit()
    }
    principales = _principal_ids(report_ids)
    grupos = OrderedDict()
    for row in rows:
        if row.objeto_tipo == "ReporteFalla" and row.objeto_id.isdigit():
            principal_id = principales.get(int(row.objeto_id), int(row.objeto_id))
            key = ("ReporteFalla", str(principal_id))
        else:
            key = ("Notificacion", str(row.pk))
        grupos.setdefault(key, []).append(row)

    resultado = []
    for key, members in grupos.items():
        representative = max(members, key=lambda item: (item.creado_en, item.pk))
        representative.grupo_ids = [item.pk for item in members]
        representative.grupo_total = len(members)
        representative.grupo_leida = all(item.leida for item in members)
        if key[0] == "ReporteFalla":
            representative.url = f"/mantenimiento/?open=falla:{key[1]}"
        resultado.append(representative)
    return sorted(resultado, key=lambda item: (item.grupo_leida, -item.creado_en.timestamp(), -item.pk))


def contar_grupos_pendientes(user):
    return sum(not row.grupo_leida for row in agrupar_notificaciones(user))


def marcar_grupo_leido(user, notificacion):
    grupo = next(
        (row for row in agrupar_notificaciones(user) if notificacion.pk in row.grupo_ids),
        None,
    )
    if grupo is None:
        return notificacion.url
    Notificacion.objects.filter(usuario=user, pk__in=grupo.grupo_ids, leida=False).update(
        leida=True,
        leido_en=timezone.now(),
    )
    return grupo.url
```

- [ ] **Step 5: Consumir grupos en vista, contador y template**

En `core/views.py`, obtener todos los grupos, filtrar `pendientes` por `not grupo_leida`, `leidas` por `grupo_leida`, paginar la lista y calcular ambos conteos con la misma proyección. En `notificacion_leer_view`, usar `marcar_grupo_leido`.

En `core/navigation.py` reemplazar el `count()` crudo por:

```python
from core.notificaciones_bandeja import contar_grupos_pendientes

notificaciones_pendientes = contar_grupos_pendientes(user)
```

En `core/templates/core/notificaciones.html` usar `notificacion.grupo_leida` y mostrar:

```html
{% if notificacion.grupo_total > 1 %}
  <span class="notif-badge">{{ notificacion.grupo_total }} avisos agrupados</span>
{% endif %}
```

- [ ] **Step 6: Ejecutar pruebas y confirmar**

Run:

```bash
python3 manage.py test core.tests.NotificacionesTests mantenimiento.tests mantenimiento.tests_v2
python3 manage.py check
git add mantenimiento/services_history.py mantenimiento/api_v2.py mantenimiento/views.py templates/mantenimiento/dashboard.html templates/mantenimiento/pwa.html core/notificaciones_bandeja.py core/views.py core/navigation.py core/templates/core/notificaciones.html core/tests.py mantenimiento/tests.py mantenimiento/tests_v2.py
git commit -m "feat(mantenimiento): mostrar continuidad y agrupar avisos"
```

Expected: una tarjeta por principal, lectura grupal, contador grupal y detalle cronológico sin N+1.

## Task 5: Construir la propuesta histórica sin escrituras

**Files:**
- Create: `operacion/services_higiene_consolidacion.py`
- Create: `operacion/tests_higiene_consolidacion.py`

- [ ] **Step 1: Escribir pruebas fallidas de agrupación conservadora**

Crear `operacion/tests_higiene_consolidacion.py` con esta preparación completa antes de las pruebas:

```python
from datetime import date, datetime

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from fallas.models import CategoriaFalla, ReporteFalla
from operacion.models import RegistroHigiene, RespuestaHigiene
from operacion.services_higiene_consolidacion import (
    aplicar_consolidacion_higiene,
    proponer_consolidacion_higiene,
)


class ConsolidacionHigieneTests(TestCase):
    def setUp(self):
        users = get_user_model()
        self.dg = users.objects.create_user(username="dg.consolidacion", is_superuser=True)
        self.operadora = users.objects.create_user(username="higiene.consolidacion")
        self.sucursal = Sucursal.objects.create(
            codigo="HIG-CONS",
            nombre="Sucursal consolidación",
            activa=True,
        )
        self.categoria = CategoriaFalla.objects.create(
            nombre="Plomería consolidación",
            tipo=CategoriaFalla.TIPO_INSTALACION,
        )

    def _crear_reporte_respuesta(self, *, fecha, observacion, indice):
        reporte = ReporteFalla.objects.create(
            sucursal=self.sucursal,
            categoria=self.categoria,
            tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
            area_instalacion="Baños",
            titulo="Limpieza de baños · Sanitario limpio y funcional",
            descripcion=f"Hallazgo de higiene: {observacion}",
            justificacion_sin_foto="Prueba automatizada.",
            reportado_por=self.operadora,
            fecha_reporte=timezone.make_aware(datetime.combine(fecha, datetime.min.time())),
        )
        registro = RegistroHigiene.objects.create(
            tipo=RegistroHigiene.TIPO_BANOS,
            sucursal=self.sucursal,
            fecha=fecha,
            clave_instancia=f"clientes-ronda-{indice}",
            plantilla_version="2026.1",
            creado_por=self.operadora,
        )
        RespuestaHigiene.objects.create(
            registro=registro,
            punto_clave="banos_sanitario",
            seccion="Limpieza de baños",
            punto_revision="Sanitario limpio y funcional",
            respuesta=RespuestaHigiene.RESPUESTA_NO_CUMPLE,
            observacion=observacion,
            evidencia=SimpleUploadedFile(
                f"evidencia-{indice}.png",
                b"\x89PNG\r\n\x1a\nprueba",
                content_type="image/png",
            ),
            requiere_seguimiento=True,
            tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
            area_instalacion="Baños",
            reporte_falla=reporte,
            continuidad_falla=RespuestaHigiene.CONTINUIDAD_INICIAL,
        )
        return reporte

    def crear_repeticiones(self, *, observaciones):
        principal = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 25),
            observacion=observaciones[0],
            indice=1,
        )
        repetido = self._crear_reporte_respuesta(
            fecha=date(2026, 9, 26),
            observacion=observaciones[1],
            indice=2,
        )
        return principal, repetido

    def crear_repeticiones_con_cierre_intermedio(self):
        principal, posterior = self.crear_repeticiones(
            observaciones=("No descarga agua", "No descarga agua"),
        )
        principal.estatus = ReporteFalla.ESTATUS_CERRADO
        principal.fecha_cierre = timezone.make_aware(datetime(2026, 9, 25, 18, 0))
        principal.save(update_fields=["estatus", "fecha_cierre"])
        return principal, posterior
```

Debajo, agregar las pruebas:

```python
def test_preview_agrupa_identidad_y_observacion_exactas_sin_escribir(self):
    principal, repetido = self.crear_repeticiones(
        observaciones=("No descarga agua", "No descarga agua"),
    )
    propuestas = proponer_consolidacion_higiene()
    self.assertEqual(propuestas.exactas[0].principal_id, principal.id)
    self.assertEqual(propuestas.exactas[0].repetido_id, repetido.id)
    repetido.refresh_from_db()
    self.assertIsNone(repetido.duplicado_de_id)

def test_preview_deja_observaciones_distintas_como_ambiguas(self):
    principal, repetido = self.crear_repeticiones(
        observaciones=("No descarga agua", "La tapa está rota"),
    )
    propuestas = proponer_consolidacion_higiene()
    self.assertEqual(propuestas.ambiguas[0].principal_id, principal.id)
    self.assertEqual(propuestas.ambiguas[0].repetido_id, repetido.id)

def test_reaparicion_despues_del_cierre_inicia_otro_ciclo(self):
    principal, posterior = self.crear_repeticiones_con_cierre_intermedio()
    propuestas = proponer_consolidacion_higiene()
    self.assertNotIn(
        (principal.id, posterior.id),
        {(row.principal_id, row.repetido_id) for row in propuestas.todas},
    )
```

- [ ] **Step 2: Implementar normalización, identidad y ciclos**

Crear `operacion/services_higiene_consolidacion.py` con dataclasses inmutables:

```python
from dataclasses import dataclass
import re
import unicodedata

from django.db.models import Q

from operacion.models import RespuestaHigiene


@dataclass(frozen=True)
class PropuestaConsolidacion:
    principal_id: int
    repetido_id: int
    respuesta_principal_id: int
    respuesta_repetida_id: int
    sucursal: str
    punto: str
    fecha_principal: str
    fecha_repetida: str
    estatus_principal: str
    estatus_repetido: str
    observacion_principal: str
    observacion_repetida: str
    exacta: bool
    motivo: str


@dataclass(frozen=True)
class ResultadoPreview:
    exactas: tuple[PropuestaConsolidacion, ...]
    ambiguas: tuple[PropuestaConsolidacion, ...]

    @property
    def todas(self):
        return self.exactas + self.ambiguas


def normalizar_texto(value):
    folded = unicodedata.normalize("NFKD", value or "")
    ascii_text = "".join(char for char in folded if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", ascii_text.casefold()).strip()


def clave_identidad(respuesta):
    reporte = respuesta.reporte_falla
    return (
        respuesta.registro.sucursal_id,
        respuesta.registro.tipo,
        respuesta.punto_clave,
        respuesta.tipo_objetivo,
        reporte.categoria_id,
        respuesta.activo_relacionado_id or 0,
        normalizar_texto(respuesta.area_instalacion),
    )


def fecha_fin(reporte):
    return reporte.fecha_cierre or reporte.fecha_resolucion


def proponer_consolidacion_higiene():
    respuestas_consultadas = list(
        RespuestaHigiene.objects.filter(
            reporte_falla__isnull=False,
            reporte_falla__duplicado_de__isnull=True,
        )
        .select_related("registro", "registro__sucursal", "reporte_falla", "reporte_falla__categoria")
        .order_by("reporte_falla__fecha_reporte", "reporte_falla_id", "id")
    )
    por_reporte = {}
    for respuesta in respuestas_consultadas:
        por_reporte.setdefault(respuesta.reporte_falla_id, respuesta)
    respuestas = list(por_reporte.values())
    grupos = {}
    for respuesta in respuestas:
        grupos.setdefault(clave_identidad(respuesta), []).append(respuesta)

    exactas = []
    ambiguas = []
    for rows in grupos.values():
        principal = rows[0]
        for candidata in rows[1:]:
            cierre = fecha_fin(principal.reporte_falla)
            if cierre and cierre < candidata.reporte_falla.fecha_reporte:
                principal = candidata
                continue
            exacta = normalizar_texto(principal.observacion) == normalizar_texto(candidata.observacion)
            propuesta = PropuestaConsolidacion(
                principal_id=principal.reporte_falla_id,
                repetido_id=candidata.reporte_falla_id,
                respuesta_principal_id=principal.id,
                respuesta_repetida_id=candidata.id,
                sucursal=principal.registro.sucursal.nombre,
                punto=principal.punto_revision,
                fecha_principal=principal.reporte_falla.fecha_reporte.date().isoformat(),
                fecha_repetida=candidata.reporte_falla.fecha_reporte.date().isoformat(),
                estatus_principal=principal.reporte_falla.get_estatus_display(),
                estatus_repetido=candidata.reporte_falla.get_estatus_display(),
                observacion_principal=principal.observacion,
                observacion_repetida=candidata.observacion,
                exacta=exacta,
                motivo="Identidad y observación exactas" if exacta else "Identidad exacta; observación diferente",
            )
            (exactas if exacta else ambiguas).append(propuesta)
    return ResultadoPreview(tuple(exactas), tuple(ambiguas))
```

- [ ] **Step 3: Probar que el preview es estrictamente read-only**

Run:

```bash
python3 manage.py test operacion.tests_higiene_consolidacion --keepdb
```

Expected: PASS; conteos de `ReporteFalla`, `BitacoraFalla`, `Notificacion` y vínculos no cambian tras dos previews.

- [ ] **Step 4: Confirmar el preview como unidad independiente**

Run:

```bash
git add operacion/services_higiene_consolidacion.py operacion/tests_higiene_consolidacion.py
git commit -m "feat(operacion): previsualizar repeticiones históricas"
```

Expected: commit sin vistas ni mutaciones.

## Task 6: Añadir revisión y aplicación histórica explícita

**Files:**
- Modify: `operacion/services_higiene_consolidacion.py`
- Modify: `operacion/tests_higiene_consolidacion.py`
- Create: `mantenimiento/views_consolidacion_higiene.py`
- Modify: `mantenimiento/urls.py`
- Create: `templates/mantenimiento/consolidacion_higiene.html`
- Create: `static/css/template_modules/mantenimiento-consolidacion-higiene.css`
- Create: `mantenimiento/tests_consolidacion_higiene.py`
- Modify: `docs/ux/action-context-coverage.md`

- [ ] **Step 1: Escribir pruebas fallidas de aplicación idempotente y preservación**

Agregar:

```python
def test_aplicar_par_conserva_campos_y_es_idempotente(self):
    principal, repetido = self.crear_repeticiones(
        observaciones=("No descarga agua", "No descarga agua"),
    )
    evidencia = repetido.constataciones_higiene.get().evidencia.name
    snapshot = {
        "titulo": repetido.titulo,
        "descripcion": repetido.descripcion,
        "estatus": repetido.estatus,
        "reportado_por_id": repetido.reportado_por_id,
    }
    pair = (principal.id, repetido.id)
    primero = aplicar_consolidacion_higiene([pair], actor=self.dg)
    segundo = aplicar_consolidacion_higiene([pair], actor=self.dg)
    repetido.refresh_from_db()
    self.assertEqual(repetido.duplicado_de_id, principal.id)
    self.assertEqual(primero.aplicados, 1)
    self.assertEqual(segundo.aplicados, 0)
    self.assertEqual(segundo.omitidos, 1)
    self.assertEqual(repetido.constataciones_higiene.get().evidencia.name, evidencia)
    self.assertEqual(
        {key: getattr(repetido, key) for key in snapshot},
        snapshot,
    )
```

- [ ] **Step 2: Implementar aplicación validada contra un preview fresco**

Agregar al servicio:

```python
from dataclasses import dataclass

from django.db import transaction

from core.duplicados import enlazar_duplicado
from core.models import AuditLog
from fallas.models import BitacoraFalla, ReporteFalla


@dataclass(frozen=True)
class ResultadoAplicacion:
    aplicados: int
    omitidos: int


@transaction.atomic
def aplicar_consolidacion_higiene(pares, *, actor):
    permitidos = {
        (row.principal_id, row.repetido_id)
        for row in proponer_consolidacion_higiene().exactas
    }
    aplicados = 0
    omitidos = 0
    for principal_id, repetido_id in pares:
        if (principal_id, repetido_id) not in permitidos:
            omitidos += 1
            continue
        principal = ReporteFalla.objects.select_for_update().get(pk=principal_id)
        repetido = ReporteFalla.objects.select_for_update().get(pk=repetido_id)
        if repetido.duplicado_de_id == principal.id:
            omitidos += 1
            continue
        destino = enlazar_duplicado(repetido, principal, estatus_cerrados=())
        BitacoraFalla.objects.create(
            reporte=repetido,
            usuario=actor,
            comentario=f"Consolidación histórica: mismo ciclo que la falla #{destino.pk}.",
        )
        BitacoraFalla.objects.create(
            reporte=destino,
            usuario=actor,
            comentario=f"Consolidación histórica: se vinculó la falla #{repetido.pk} sin eliminar evidencia.",
        )
        AuditLog.objects.create(
            user=actor,
            action="CONSOLIDATE",
            model="fallas.ReporteFalla",
            object_id=str(repetido.pk),
            payload={"principal_id": destino.pk, "origen": "higiene_preview_exacto"},
        )
        aplicados += 1
    return ResultadoAplicacion(aplicados=aplicados, omitidos=omitidos)
```

- [ ] **Step 3: Crear vistas protegidas con respuesta JSON/HTML compartida**

En `mantenimiento/views_consolidacion_higiene.py`:

```python
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.views.decorators.http import require_http_methods

from core.access import is_admin_or_dg
from operacion.services_higiene_consolidacion import (
    aplicar_consolidacion_higiene,
    proponer_consolidacion_higiene,
)


@login_required
@require_http_methods(["GET", "POST"])
def consolidacion_higiene(request):
    if not is_admin_or_dg(request.user):
        raise PermissionDenied
    preview = proponer_consolidacion_higiene()
    if request.method == "GET":
        return render(
            request,
            "mantenimiento/consolidacion_higiene.html",
            {"exactas": preview.exactas, "ambiguas": preview.ambiguas},
        )
    pares = []
    for value in request.POST.getlist("pares"):
        principal, repetido = value.split(":", 1)
        pares.append((int(principal), int(repetido)))
    resultado = aplicar_consolidacion_higiene(pares, actor=request.user)
    payload = {
        "ok": True,
        "toast": {
            "type": "success",
            "message": f"{resultado.aplicados} grupos aplicados; {resultado.omitidos} omitidos.",
        },
        "redirect": request.path,
    }
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return JsonResponse(payload)
    return redirect(request.path)
```

Agregar rutas GET/POST en `mantenimiento/urls.py` importando este módulo.

- [ ] **Step 4: Crear la tabla de revisión**

Crear `templates/mantenimiento/consolidacion_higiene.html` con checkboxes únicamente en exactas y una sección separada no seleccionable para ambiguas. El núcleo completo de ambas tablas y la acción será:

```html
<form method="post" data-async-action>
  {% csrf_token %}
  <table>
    <thead><tr><th>Aplicar</th><th>Principal</th><th>Repetido</th><th>Sucursal</th><th>Punto</th><th>Fechas</th><th>Estados</th><th>Observaciones</th><th>Motivo</th></tr></thead>
    <tbody>
      {% for row in exactas %}
      <tr>
        <td><input type="checkbox" name="pares" value="{{ row.principal_id }}:{{ row.repetido_id }}" aria-label="Unir falla {{ row.repetido_id }} con {{ row.principal_id }}"></td>
        <td>#{{ row.principal_id }}</td><td>#{{ row.repetido_id }}</td><td>{{ row.sucursal }}</td><td>{{ row.punto }}</td>
        <td>{{ row.fecha_principal }} → {{ row.fecha_repetida }}</td><td>{{ row.estatus_principal }} / {{ row.estatus_repetido }}</td>
        <td>{{ row.observacion_principal }} / {{ row.observacion_repetida }}</td><td>{{ row.motivo }}</td>
      </tr>
      {% empty %}<tr><td colspan="9">No hay coincidencias exactas.</td></tr>{% endfor %}
    </tbody>
  </table>
  <button type="submit">Aplicar seleccionadas</button>
</form>

<section aria-labelledby="ambiguas-title">
  <h2 id="ambiguas-title">Revisión manual; no se aplicarán</h2>
  <table>
    <thead><tr><th>Principal</th><th>Posible repetido</th><th>Sucursal</th><th>Punto</th><th>Observaciones</th><th>Motivo</th></tr></thead>
    <tbody>
      {% for row in ambiguas %}
      <tr><td>#{{ row.principal_id }}</td><td>#{{ row.repetido_id }}</td><td>{{ row.sucursal }}</td><td>{{ row.punto }}</td><td>{{ row.observacion_principal }} / {{ row.observacion_repetida }}</td><td>{{ row.motivo }}</td></tr>
      {% empty %}<tr><td colspan="6">No hay coincidencias ambiguas.</td></tr>{% endfor %}
    </tbody>
  </table>
</section>
```

- [ ] **Step 5: Probar permisos y contrato progresivo**

Crear `mantenimiento/tests_consolidacion_higiene.py` con la preparación explícita:

```python
from datetime import date, datetime

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.access import ACCESS_MANAGE
from core.models import Sucursal, UserModuleAccess
from fallas.models import CategoriaFalla, ReporteFalla
from operacion.models import RegistroHigiene, RespuestaHigiene


class ConsolidacionHigieneViewTests(TestCase):
    def setUp(self):
        users = get_user_model()
        self.dg = users.objects.create_superuser(username="dg.preview", password="test")
        self.mantenimiento = users.objects.create_user(username="mant.preview", password="test")
        self.operadora = users.objects.create_user(username="operadora.preview", password="test")
        UserModuleAccess.objects.create(
            user=self.mantenimiento,
            module="mantenimiento",
            access=ACCESS_MANAGE,
        )
        self.sucursal = Sucursal.objects.create(
            codigo="PREVIEW-HIG",
            nombre="Sucursal preview",
            activa=True,
        )
        self.categoria = CategoriaFalla.objects.create(
            nombre="Plomería preview",
            tipo=CategoriaFalla.TIPO_INSTALACION,
        )
        self.principal = self._crear_reporte(fecha=date(2026, 9, 25), indice=1)
        self.repetido = self._crear_reporte(fecha=date(2026, 9, 26), indice=2)
        self.url = reverse("mantenimiento:consolidacion-higiene")

    def _crear_reporte(self, *, fecha, indice):
        reporte = ReporteFalla.objects.create(
            sucursal=self.sucursal,
            categoria=self.categoria,
            tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
            area_instalacion="Baños",
            titulo="Limpieza de baños · Sanitario limpio y funcional",
            descripcion="No descarga agua.",
            justificacion_sin_foto="Prueba automatizada.",
            reportado_por=self.operadora,
            fecha_reporte=timezone.make_aware(datetime.combine(fecha, datetime.min.time())),
        )
        registro = RegistroHigiene.objects.create(
            tipo=RegistroHigiene.TIPO_BANOS,
            sucursal=self.sucursal,
            fecha=fecha,
            clave_instancia=f"clientes-ronda-{indice}",
            plantilla_version="2026.1",
            creado_por=self.operadora,
        )
        RespuestaHigiene.objects.create(
            registro=registro,
            punto_clave="banos_sanitario",
            seccion="Limpieza de baños",
            punto_revision="Sanitario limpio y funcional",
            respuesta=RespuestaHigiene.RESPUESTA_NO_CUMPLE,
            observacion="No descarga agua",
            requiere_seguimiento=True,
            tipo_objetivo=ReporteFalla.OBJETIVO_INSTALACION,
            area_instalacion="Baños",
            reporte_falla=reporte,
            continuidad_falla=RespuestaHigiene.CONTINUIDAD_INICIAL,
        )
        return reporte
```

Debajo, agregar las pruebas:

```python
def test_solo_dg_puede_ver_y_aplicar(self):
    self.client.force_login(self.mantenimiento)
    self.assertEqual(self.client.get(self.url).status_code, 403)
    self.client.force_login(self.dg)
    self.assertEqual(self.client.get(self.url).status_code, 200)

def test_post_async_devuelve_toast_y_redirect_estable(self):
    self.client.force_login(self.dg)
    response = self.client.post(
        self.url,
        {"pares": [f"{self.principal.id}:{self.repetido.id}"]},
        HTTP_X_REQUESTED_WITH="XMLHttpRequest",
    )
    self.assertEqual(response.status_code, 200)
    self.assertEqual(response.json()["toast"]["type"], "success")
    self.assertEqual(response.json()["redirect"], self.url)
```

- [ ] **Step 6: Registrar cobertura UX y confirmar**

Agregar una fila a `docs/ux/action-context-coverage.md` indicando preview GET de solo lectura, POST async, toast, bloqueo del submitter, reintento y pruebas.

Run:

```bash
python3 manage.py test operacion.tests_higiene_consolidacion mantenimiento.tests_consolidacion_higiene
python3 manage.py check
git add operacion/services_higiene_consolidacion.py operacion/tests_higiene_consolidacion.py mantenimiento/views_consolidacion_higiene.py mantenimiento/urls.py templates/mantenimiento/consolidacion_higiene.html static/css/template_modules/mantenimiento-consolidacion-higiene.css mantenimiento/tests_consolidacion_higiene.py docs/ux/action-context-coverage.md
git commit -m "feat(mantenimiento): revisar consolidación histórica de fallas"
```

Expected: preview sin escritura, POST solo DG/admin, aplicación exacta e idempotente.

## Task 7: Cerrar caché, regresiones y validación real

**Files:**
- Modify: `static/mantenimiento/sw.js`
- Modify: `templates/mantenimiento/pwa.html`
- Modify: `mantenimiento/tests.py`
- Verify: all files in branch

- [ ] **Step 1: Actualizar la versión del SW de Mantenimiento**

En `static/mantenimiento/sw.js` usar exactamente:

```javascript
const CACHE_VERSION = "20260926-higiene-continuidad-v1";
```

La llamada de registro en `templates/mantenimiento/pwa.html` debe quedar:

```javascript
navigator.serviceWorker.register(
  "/mantenimiento/sw.js?v=20260926-higiene-continuidad-v1",
  { scope: "/mantenimiento/" }
);
```

Actualizar `mantenimiento/tests.py` para exigir igualdad entre ambas versiones.

- [ ] **Step 2: Ejecutar la suite focalizada completa**

Run:

```bash
python3 manage.py migrate --check
python3 manage.py check
python3 manage.py test \
  operacion.tests_higiene \
  operacion.tests_higiene_consolidacion \
  fallas.tests_duplicados \
  mantenimiento.tests \
  mantenimiento.tests_v2 \
  mantenimiento.tests_consolidacion_higiene \
  core.tests.NotificacionesTests
```

Expected: 0 errores, 0 migraciones pendientes y todas las pruebas PASS.

- [ ] **Step 3: Validar en navegador local con usuarios reales de prueba**

Comprobar en escritorio y viewport móvil:

1. Primera detección con foto crea un folio.
2. Al día siguiente aparece el folio activo y `Sigue siendo la misma` guarda sin foto.
3. `Empeoró o cambió` exige foto y comentario.
4. `Es otro problema` crea otro folio.
5. `Ya quedó corregido` solicita validación sin cerrar.
6. Mantenimiento muestra primera/última fecha y línea de tiempo.
7. Notificaciones muestra una tarjeta agrupada y la lectura limpia el grupo.
8. La consolidación histórica muestra exactas/ambiguas separadas y no escribe al abrir.
9. Consola sin errores; XHR relevantes 200/201 o 409 controlado; SW activo con las versiones nuevas.

Expected: el flujo visible coincide con los nueve puntos y conserva captura tras un error.

- [ ] **Step 4: Revisar diff, confirmar el cierre de implementación y abrir PR borrador**

Run:

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git diff origin/main..HEAD --check
git log --oneline --decorate -8
git worktree list
git worktree prune --dry-run
```

Confirmar que no existan capturas, logs, `outputs/`, `.DS_Store`, `.playwright-mcp/` ni cambios ajenos. Crear un PR borrador con resumen funcional, migración, archivos principales, pruebas y validación de navegador.

- [ ] **Step 5: Mergear, desplegar y validar captura futura en producción**

Después de aprobación del PR:

```bash
ssh -i /Users/mauricioburgos/.ssh/agente_dg_ops root@68.183.165.47
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

Verificar `python manage.py migrate --check` dentro del contenedor, la pantalla autenticada de Higiene, Mantenimiento, Notificaciones y las versiones de ambos SW. No ejecutar todavía la aplicación histórica.

- [ ] **Step 6: Obtener autorización separada antes de consolidar producción**

Abrir la vista previa autenticada, exportar o registrar los conteos de exactas y ambiguas, revisar una muestra por sucursal y presentar a Mauricio:

- cantidad de principales propuestos;
- cantidad de repetidos exactos;
- cantidad de ambiguos sin cambio;
- folios y sucursales afectados;
- confirmación de que no se alterarán estados ni evidencias.

Expected: detenerse en la vista previa. La selección y aplicación en producción requiere una autorización explícita posterior.

- [ ] **Step 7: Cerrar el worktree solo después de validación productiva**

Cuando el cambio futuro esté mergeado, desplegado y validado:

```bash
bash scripts/task_workspace_audit.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1
bash scripts/task_workspace_close.sh \
  --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 \
  --task fallas_checklist_continuidad \
  --state merged
```

Expected: worktree y ramas exactas eliminados por el flujo registrado; `git fetch --prune` completado.
