# Maya: precio vivo y horario efectivo

**Goal:** conectar evidencia comercial de lectura verificada, sin activar el piloto ni operaciones comerciales.

**Architecture:** reutilizar cliente/bloqueo Point para precio global; modelos aprobados de horarios especiales y fuentes oficiales para horarios. Maya valida identidad, vigencia y procedencia antes de responder. Sin tablas, dependencias o configuración productiva nuevas.

**Tech Stack:** Django/DRF, PostgreSQL 16 aislado (55594), FastAPI/httpx y pytest existentes.

## Etapas aprobadas

1. ERP: acción GET autenticada `products/sale-price/?product_code=...`. Mapa activo unívoco al PK Point; detalle vivo concordante; `Precio_default` positivo/finito; MXN por política comercial. Timestamp de esa lectura, no de inventario. Errores o contradicciones no usan precio replicado.
2. Revisiones separadas de especificación y calidad; pruebas del coordinador. Maya consume el DTO real con PK/importe en cadenas. El precio es global: no pedir sucursal para cotizar. Mantener presupuesto de cuatro lecturas y veinte segundos, identidad producto/formato/tamaño y cero escrituras.
3. Horario: una excepción aprobada de sucursal/fecha sustituye únicamente ese día. Horario habitual desde la página oficial; Google mediante cliente existente solo con credenciales y ubicación correctas. Conflictos o cobertura especial desconocida no permiten anunciar apertura/cierre efectivo. No confundir Crucero histórico con Bamoa ni un Maps place_id con Business Profile location.
4. Revisión de especificación y calidad por etapa y transversal. Ejecutar regresiones completas, documentar límites y dejar cambios locales listos para revisión; despliegue y configuración requieren aprobación independiente.

## Pruebas

RED/GREEN por etapa: autenticación, identidad ambigua/inactiva, importes inválidos, bloqueo/red fallida, DTO real, precio sin sucursal, memoria conversacional y cero operaciones comerciales. Horarios: patrones oficiales, domingo, AM/PM, excepción por fecha, borradores/cancelados/conflictos, fuentes ausentes, Bamoa/Crucero y ausencia de credenciales Google.

ERP: `APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55594/pastelerias_erp` con Python del entorno ERP: `manage.py check`, `manage.py migrate --check`, tests afectados y regresiones de API/Point.

Maya: `python3 -m pytest -q` en `maya-internal-conversation-loops`.

## Base comprobada

ERP creado desde main actualizado y cuatro commits propios recuperados sin modificar el worktree anterior. PostgreSQL 16 aislado migrado: check sin problemas, 49 pruebas aprobadas. Maya: 1,005 aprobadas y 7 omitidas. Piloto, pagos, reservas, WhatsApp y configuración productiva sin cambios.

Mauricio confirmó precio de catálogo uniforme. No hay acceso Google ni mapeo Bamoa Business Profile comprobados: no afirmar que Google ya está conectado.

## Contrato de horario de lectura

GET autenticado `/api/integraciones/horarios-especiales/effective/?branch_name=Bamoa&target_date=2026-09-30`. Solo identidad de sucursal, fecha, zona, ventanas y procedencia; no comandos ni detalles administrativos. `regular.status=VERIFIED` acredita horario habitual oficial para el día solicitado. `effective.status=VERIFIED` exige excepción válida para esa fecha; `REGULAR_ONLY` o `UNKNOWN` no prueban apertura/cierre ahora.

Resolver operativo ERP existente y verificar vínculo oficial unívoco por nombre/código. Bamoa actual puede conservar código ERP CRUCERO, pero el público es BAMOA: jamás seleccionar perfil histórico por ese código. Las solicitudes APROBADO/EJECUTADO requieren approved_at y detalle válido/no cancelado. Padre FALLIDO aprobado o detalles contradictorios dan UNKNOWN. Ignorar borradores/cancelados. No inferir excepciones desde ausencia de registros.

La lectura Google real puede permanecer pendiente si faltan OAuth/mapeo válido. No inventar conectividad ni cobertura especial. El horario habitual continúa disponible con su procedencia, sin prometer horario efectivo.

La URL oficial sin www redirige a www. Usar directamente `https://www.pollyanasdolce.com/api/branches/` como URL fija de lectura, sin seguir redirecciones arbitrarias ni URLs del contenido. Comprobado con lectura pública; el endpoint solo admite GET, no HEAD.

Corregir la raíz compartida `BranchService.get_operational_status`: estado desconocido `is_open=None`, no `False`; apertura inclusiva y cierre exclusivo, fecha consciente de zona. Sus consumidores legacy y captura deben manejar None sin declarar cerrado, sin promover pagos y sin calcular próxima apertura con evidencia semanal insuficiente. Herramienta y validador semántico conservan contacto local y anexan evidencia ERP verificada.

## Etapa precio ERP

Commit `8ba641f3`: endpoint y once pruebas focales. Implementador: 63 pruebas (incluye cliente HTTP); coordinador: once pruebas independientes aprobadas. Especificación PASS. Calidad encontró JSON malformado en login compartido que produce AttributeError: reparar su frontera de datos y repetir revisión antes de cerrar. La reducción de timeout mediante DEADLINE es cooperativa; no garantiza una duración exacta frente a un servidor que transmite lentamente. El consumidor conserva su plazo de turno existente.

Maya `b5c8d2e` + `c68c454`: lectura y render de precio global, memoria y presupuesto conservados, importe exacto representable en centavos sin redondear evidencia. Especificación PASS y calidad final PASS; coordinador suite fresca 1,049 aprobadas y 7 omitidas.

ERP reparación de frontera `aed8e89e`: cuatro objetos JSON de autenticación y listas/objetos anidados de cuentas/sucursales. RED 39 subcasos, GREEN 98 pruebas en PostgreSQL 16 limpio. Primer combinado con keepdb falló por fixtures persistentes: `ConversionTransfersTests(SimpleTestCase)` permite base de datos sin limpieza transaccional; no se modificó esa infraestructura ajena. Django recreó/destruyó exclusivamente la base test_pastelerias_erp aislada en 55594, sin tocar la base local principal ni producción. No reutilizar keepdb al incluir ese módulo. Revisiones finales de la reparación en curso.

Reparación ERP aprobada por especificación y calidad; coordinador repitió las 98 pruebas en base fresca, resultado OK. Revisión independiente del cliente: 17 pruebas sin red ni base.

El consumidor de horario no debe deducir permiso de pickup, existencia, precio ni prometer pagos a partir de apertura. Si hay varias ventanas confirmadas, calcular el cierre de la ventana actual; no anunciar el último cierre del día durante una pausa. Fuera de ventanas actuales, no inferir próxima apertura de otro día con calendario semanal sin excepciones verificadas. Información de contacto local permanece separada del estado de horario.

## Etapa horario ERP y verificación transversal

`4521d52d` añade el GET de horario efectivo reutilizando solicitudes/detalles existentes y el resolver operativo. `c9260fbb` rechaza estados de detalle desconocidos y claves JSON duplicadas de la página oficial. Especificación y calidad finales PASS; 24 pruebas focales aprobadas. La consulta pública real también verificó que el parser interpreta los nueve horarios oficiales en sus siete días (63 ventanas diarias), sin alimentar tablas ni modificar perfiles externos.

Coordinador: 122 pruebas combinadas API/Point/horarios aprobadas en PostgreSQL 16 aislado, base de prueba recreada exclusivamente en 55594 y destruida al terminar. `manage.py check` sin problemas y `migrate --check` sin pendientes. Los errores HTTP esperados pertenecen a pruebas negativas; no se enviaron mensajes ni se ejecutaron operaciones comerciales.

Maya `efdb8ce` reemplaza el cálculo local de apertura por evidencia ERP validada y conserva contacto por separado. Estado desconocido para horario habitual sin excepción confirmada; cierre exclusivo, pausas y próxima apertura solo del día confirmado. El validador compartido se aplica tanto en el servicio de sucursales como al recibir resultados de herramientas. La revisión independiente detectó un DTO contradictorio (horario habitual VERIFIED sin código público), corregido en `57cd1e1` con RED/GREEN. Especificación final PASS (27 pruebas independientes); calidad comprobó 336 pruebas afectadas sin hallazgos. La suite fresca final del coordinador aprobó 1,076 pruebas, con siete omitidas y cinco advertencias existentes.

La ejecución queda local. Google real necesita OAuth y ubicación Business Profile correcta; la configuración Bamoa, activación del piloto, despliegue y validación real en 6264 siguen pendientes de autorización/ejecución separadas. No se modificaron pagos, reservas ni flags de producción.

Cierre transversal independiente: PASS sobre ERP `c9260fbb` y Maya `57cd1e1`, incluida la revisión de los tres documentos de evidencia. Sin hallazgos pendientes para preparación local. Entrega conservando ambos worktrees; sin merge, push ni despliegue.
