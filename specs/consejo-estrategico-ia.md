# Spec: Consejo Estratégico de IA (comité multi-rol)

## Origen y relación con el resto del proyecto

Última pieza genuinamente faltante del proyecto "Consejo Estratégico de IA".
Ya existen y están deployadas: rentabilidad por producto
([rentabilidad-producto.md](rentabilidad-producto.md)), mano de obra
automática ([mano-obra-produccion-automatica.md](mano-obra-produccion-automatica.md)),
rentabilidad por sucursal (`rentabilidad/`), simulador de expansión
(`/reportes/expansion/simulador/`), dashboard ejecutivo (`/dashboard/`),
estudio de viabilidad de nueva ubicación (`reportes/services_market_study.py`).

Esta rebanada es la capa que faltaba: un panel de 9 roles (CEO, CFO, COO,
CMO, Comercial, RH, CTO, Innovación, Riesgos) que responde una pregunta
estratégica libre en un solo request, cada rol desde su lente, con el CEO
dando la conclusión ejecutiva final.

## Decisiones ya tomadas (no reabrir)

- **No se toca `orquestacion/`.** Su esquema (`AgentDefinition`,
  `OrchestrationRule`) está diseñado para agentes operativos con
  herramientas/handoffs/aprobación humana (ver `orquestacion/catalog.py`:
  Director Operativo, Agente de Demanda y Ventas, Agente de Producción,
  Agente de Compras — pipeline de ejecución). Un rol estratégico que solo
  opina sobre una pregunta no necesita tools ni handoffs; forzarlo ahí
  significaría rellenar campos que no aplican.
- **App nueva y ligera: `consejo_ia/`.** Sin infraestructura nueva de
  agentes/tools — reutiliza el patrón ya probado en producción de
  `rentabilidad/agente_rentabilidad.py`: `OpenAI(api_key=settings.OPENAI_API_KEY)`,
  `model="gpt-4o-mini"`, `temperature=0.3`, prompt de sistema en español con
  reglas de negocio, JSON forzado por instrucción en el prompt (no
  structured outputs de la API), `json.loads` con try/except y fallback si
  sale inválido. Mismo patrón que `reportes/services_market_study.py`.
- **MVP de datos: snapshot general de la empresa**, el mismo para
  cualquier pregunta libre (no formularios estructurados por caso de uso
  todavía — reporte diario, nueva sucursal con renta/inversión, línea
  retail, meta de utilidad del socio quedan fuera de esta rebanada). El
  snapshot se arma con lo que YA existe:
  - Dashboard ejecutivo: `reportes.dashboard_full_dataset.get_materialized_dashboard_full_payload(months_window=...)`.
    Puede regresar `None` si la vista materializada no está poblada — en
    ese caso el snapshot declara explícitamente "dashboard ejecutivo no
    disponible", no lo omite en silencio.
  - Rentabilidad por sucursal: `rentabilidad.models.SucursalRentabilidad`
    del periodo más reciente por sucursal (ventas, costos, margen, ROI,
    payback, `estado` — mismo campo que usa `agente_rentabilidad.py`).
  - Rentabilidad por producto: ranking ya construido en
    `monitor_margenes_precio_sugerido` (top/bottom productos por
    `contribucion_total`, honrando la misma etiqueta de fuente
    FAB_COMPLETO/MP_FALLBACK/REVENTA_HISTORICO/SIN_COSTO).
- **Regla de no inventar datos** (explícita en el documento original): cada
  rol debe separar hechos (del snapshot) de supuestos, y declarar qué dato
  le falta para responder con más precisión en vez de simularlo.

## Objetivo

Un Director General escribe una pregunta estratégica libre (ej. "¿Me
conviene abrir una sucursal en Los Mochis?") y el sistema responde en
formato de comité: 8 roles analizan desde su perspectiva + el CEO da la
conclusión ejecutiva (Aprobar / Rechazar / Posponer / Probar piloto / Pedir
más datos), todo en un solo request, usando datos reales del ERP.

## Requisitos exactos

### Modelo (`consejo_ia/models.py`)
- `ConsejoConsulta`: `pregunta` (TextField), `snapshot_json` (JSONField, el
  contexto real armado, para poder auditar después qué datos vio la IA),
  `respuestas_json` (JSONField, dict por código de rol con
  `{analisis, supuestos, datos_faltantes}`), `veredicto_ceo` (choices:
  APROBAR/RECHAZAR/POSPONER/PILOTO/PEDIR_DATOS), `resumen_ejecutivo_ceo`
  (TextField), `creado_por` (FK user), `creado_en`.
  Guardar histórico es requisito explícito del documento original ("guardar
  los análisis históricos para comparar si sus recomendaciones funcionaron").

### Servicio (`consejo_ia/services.py`)
- `construir_snapshot(months_window=6) -> dict`: arma el bloque de contexto
  real (dashboard + sucursales + ranking de producto), cada sección
  etiquetada como disponible/no disponible.
- `ROLES`: lista de 8 roles (CFO, COO, CMO, Comercial, RH, CTO, Innovación,
  Riesgos) con su prompt de sistema (qué debe analizar cada uno, según la
  sección 4 del documento original — CFO: inversión/ROI/flujo; COO:
  capacidad/logística/personal; CMO: marca/mercado/promociones; Comercial:
  ventas/ticket/tráfico; RH: personal/sueldos/rotación; CTO: si el ERP
  soporta el proyecto; Innovación: ventaja competitiva/nueva unidad de
  negocio; Riesgos: amenazas/dependencias/sensibilidad).
- `analizar_pregunta(pregunta: str, *, usuario) -> ConsejoConsulta`:
  1. Arma el snapshot.
  2. Por cada uno de los 8 roles, una llamada a OpenAI (system prompt del
     rol + snapshot + pregunta) → JSON `{analisis, supuestos, datos_faltantes}`.
     Igual que `agente_rentabilidad.py`: si el JSON sale inválido o hay
     excepción, fallback declarando el error, no tumbar toda la consulta.
  3. Una llamada final del CEO (system prompt CEO + snapshot + pregunta +
     las 8 respuestas) → JSON `{veredicto, resumen_ejecutivo, conclusion}`.
  4. Persiste todo en `ConsejoConsulta` y lo retorna.
  - **Nota de rendimiento (ceiling conocido):** 9 llamadas secuenciales a
    OpenAI por pregunta — latencia esperada ~20-40s. Aceptable para MVP
    (uso ocasional del DG, no un endpoint de alto tráfico). Si se vuelve
    molesto, la mejora es paralelizar las 8 llamadas de rol con hilos antes
    de la síntesis del CEO, no reescribir el mecanismo.

### Vista y RBAC (`consejo_ia/views.py`, `core/access.py`)
- Nueva función `can_view_consejo_ia(user)` en `core/access.py`, mismo
  patrón que `can_view_rentabilidad` — restringido a Dirección/superuser
  (datos financieros y estratégicos cross-departamento).
- Vista `consejo_ia_home`: formulario con textarea para la pregunta libre +
  botón "Consultar al Consejo". Al enviar, corre `analizar_pregunta` y
  renderiza las 9 secciones (8 roles + conclusión CEO) más un historial de
  consultas previas (`ConsejoConsulta` del usuario o de todos, a decidir en
  implementación).
- URL: `/consejo-ia/`.

### Template
- Antes de crear el template, seguir `hallmark_ui_rules` (memoria del
  proyecto): reutilizar `.card`, `.btn`, `.badge`, paleta vino/dorado,
  Playfair Display para títulos, Nunito para body — no inventar clases
  nuevas si ya existe un equivalente.

## Edge cases

- Snapshot con el dashboard ejecutivo no disponible (`None`): declarar la
  sección como "no disponible" en vez de omitirla u inventar cifras.
- Sucursal sin `SucursalRentabilidad` del mes más reciente: excluirla del
  snapshot con nota, no rellenar con ceros que parezcan datos reales.
- Falla de OpenAI (timeout, rate limit, JSON inválido) en el rol de un solo
  miembro del comité: ese rol se marca con error, el resto del comité y la
  conclusión del CEO igual se generan (no tumbar toda la consulta por un
  solo rol fallido).
- Pregunta vacía o demasiado corta: validar en el formulario antes de
  gastar 9 llamadas a OpenAI.
- Costo: cada consulta cuesta 9 llamadas a gpt-4o-mini — aceptable dado el
  uso esperado (consultas puntuales de Dirección, no un chat de alto
  volumen).

## Fuera de alcance (explícito)

- Formularios estructurados por caso de uso (nueva sucursal con
  renta/inversión, línea retail, meta de utilidad del socio) — quedan para
  specs futuros si se necesitan, una vez validado el MVP de snapshot
  general.
- Integración con `orquestacion/` (AgentDefinition/OrchestrationRule) — se
  reevaluará solo si en el futuro se necesita que el comité dispare
  acciones reales en el ERP (hoy es puro análisis textual).
- Reporte diario automático — spec separado, no se resuelve aquí aunque
  comparta el mismo snapshot de dashboard ejecutivo.

## Definición de "hecho"

- [ ] App `consejo_ia/` con modelo `ConsejoConsulta`, migración incluida.
- [ ] `construir_snapshot()` arma el contexto real desde dashboard
      ejecutivo + rentabilidad por sucursal + ranking de producto, con
      cada sección honestamente etiquetada disponible/no disponible.
- [ ] `analizar_pregunta()` genera las 8 respuestas de rol + conclusión del
      CEO, persiste en `ConsejoConsulta`, y sobrevive a que un solo rol
      falle sin tumbar la consulta completa.
- [ ] Vista `/consejo-ia/` con RBAC `can_view_consejo_ia` (Dirección/superuser),
      formulario de pregunta libre, render de las 9 secciones, historial de
      consultas previas.
- [ ] Tests: snapshot con dashboard disponible/no disponible, rol individual
      que falla, veredicto del CEO persistido correctamente, RBAC bloquea a
      usuarios sin permiso.
- [ ] `python manage.py check` y `migrate --check` en 0.
- [ ] Validado en navegador real (o `RequestFactory` si el preview local no
      alcanza, como en la rebanada anterior): la pregunta se envía, las 9
      secciones se renderizan, el historial persiste entre consultas.
