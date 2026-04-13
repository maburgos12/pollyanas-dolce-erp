Este directorio queda reservado para prompts downstream sobre datos ya persistidos en el ERP.

Regla del módulo:
- No se usan prompts ni IA para navegar Point.
- La navegación y extracción son determinísticas con Playwright + selectores versionados.

Contenido recomendado del modulo:
- `dg_executive_response_policy.md`: politica de redaccion y criterio para preguntas ejecutivas.
- `dg_executive_response_loop.md`: loop minimo de razonamiento para DG.
- `examples/`: casos reales buenos vs malos para convertir errores en aprendizaje durable.

Convencion sugerida:
- Reglas permanentes que aplican siempre -> `memory.md`
- Formato, criterio y secuencia de respuesta -> `pos_bridge/prompts/`
- Casos reales corregidos -> `pos_bridge/prompts/examples/`
