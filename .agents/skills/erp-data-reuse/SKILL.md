---
name: erp-data-reuse
description: Descubre fuentes existentes, alias, identidades y consumidores antes de diseñar o integrar un módulo o proyecto en el ERP de Pollyana's Dolce.
---

# Reutilización de datos del ERP

Usa esta habilidad antes de proponer modelos, tablas, campos, importadores, catálogos o formularios de captura para un módulo nuevo o una ampliación que consuma datos compartidos. Produce una ficha de fuentes que permita decidir qué reutilizar.

1. Define el concepto de negocio, su unidad de análisis y los datos que el módulo necesita. Distingue entidad maestra, transacción, evento, histórico, snapshot y dato calculado.
2. Ejecuta `python3 manage.py inventario_fuentes_datos --term <concepto>` con una conexión PostgreSQL válida; repite `--term` con sinónimos operativos. Busca en el grafo de código los modelos, rutas, servicios y consumidores candidatos. Revisa migraciones y contratos existentes cuando correspondan.
3. Para los candidatos relevantes, verifica en la base autorizada mediante consultas acotadas y de solo lectura si hay registros, claves, alias, relaciones y cobertura temporal. Registra ambiente, fecha, consultas, conteos y límites. Evita extraer valores personales o barrer tablas ajenas al dominio.
4. Identifica la fuente que crea y actualiza cada dato, el identificador estable, reglas de unicidad y otros módulos que lo usan. Señala superposiciones que podrían duplicar información o cálculos.
5. Clasifica cada posible equivalencia como confirmada, candidata, distinta o no resuelta. Un nombre similar solo crea una candidatura. No declares iguales dos registros si faltan claves, ámbito, vigencia o evidencia de negocio; conserva los casos ambiguos para revisión.
6. Entrega la ficha usando `docs/data-reuse/ficha-fuentes-template.md`: necesidad; fuentes y modelos candidatos; evidencia de registros; alias y equivalencias; fuente oficial; consumidores; decisión de reutilizar, extender o crear; y dudas que afectan la decisión. Revisa las reglas nuevas de equivalencia con Mauricio antes de aplicarlas a datos o flujos operativos.

El inventario automatizado informa metadatos y candidatos léxicos. No detecta por sí mismo equivalencias semánticas ni autoriza fusiones. Mantén el ERP en PostgreSQL y enlaza integraciones externas mediante identificadores explícitos. Sigue `AGENTS.md` para aislamiento, permisos y validación de cualquier cambio posterior.
