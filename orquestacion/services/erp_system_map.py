"""Developer-only structural map. This module neither reads rows nor grants access."""
import hashlib
import inspect
import json
from pathlib import Path
import subprocess

from django.apps import apps
from django.conf import settings
from django.urls import URLResolver, get_resolver

from core.management.commands.inventario_fuentes_datos import _describe_model


CODE_SUFFIXES = {'.py', '.js', '.jsx', '.ts', '.tsx', '.html', '.css', '.scss', '.sql', '.sh'}
EXCLUDED_DIRS = {'storage', 'media', 'private_media', 'output', 'outputs', 'node_modules', '.venv', '.git', 'backups'}


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def _source_commit(root):
    result = subprocess.run(['git', '-C', str(root), 'rev-parse', 'HEAD'], capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else None


def _safe_source(source, root):
    try:
        relative = source.absolute().relative_to(root)
        if EXCLUDED_DIRS.intersection(relative.parts):
            return None
        if any((root / Path(*relative.parts[:index])).is_symlink() for index in range(1, len(relative.parts) + 1)):
            return None
        resolved = source.resolve()
        if EXCLUDED_DIRS.intersection(resolved.relative_to(root).parts) or not resolved.is_file():
            return None
        return resolved
    except (OSError, ValueError):
        return None


def _source_inventory(root):
    result = subprocess.run(['git', '-C', str(root), 'ls-files', '-z'], capture_output=True, check=True)
    files, excluded, missing = [], [], []
    for name in sorted(result.stdout.decode().split('\0')):
        relative = Path(name)
        if not name or relative.suffix not in CODE_SUFFIXES:
            continue
        if EXCLUDED_DIRS.intersection(relative.parts):
            excluded.append(name)
            continue
        source = _safe_source(root / relative, root)
        # References and inventory share the same private-file boundary.
        if source is None:
            missing.append(name)
            continue
        files.append({'path': name, 'sha256': hashlib.sha256(source.read_bytes()).hexdigest()})
    return {'files': files, 'excluded': excluded, 'missing': missing}


def _source_ref(obj, root):
    obj = inspect.unwrap(obj)
    symbol = f"{getattr(obj, '__module__', '')}.{getattr(obj, '__qualname__', getattr(obj, '__name__', ''))}".strip('.')
    try:
        filename = inspect.getsourcefile(obj)
        source = _safe_source(Path(filename), root) if filename else None
        relative = source.relative_to(root).as_posix() if source else None
    except (TypeError, ValueError):
        return {'symbol': symbol, 'path': None, 'sha256': None, 'external_or_unresolved': True}
    if source is None or not source.is_file():
        return {'symbol': symbol, 'path': relative, 'sha256': None, 'external_or_unresolved': True}
    return {'symbol': symbol, 'path': relative, 'sha256': hashlib.sha256(source.read_bytes()).hexdigest(), 'external_or_unresolved': False}


def _schema(value):
    # Defaults/examples are values, not structure; never export them from a schema.
    if isinstance(value, dict):
        return {key: _schema(item) for key, item in value.items() if key not in {'default', 'example', 'examples'}}
    if isinstance(value, list):
        return [_schema(item) for item in value]
    return value


def _model(model, root):
    result = _describe_model(model, [], 0, set(), True)
    for key in ('tabla_existe', 'coincidencias_lexicas', 'puntaje_busqueda'):
        result.pop(key)
    for rule in result['identificadores_y_unicidad']:
        if 'condicion' in rule:
            rule['conditional'] = bool(rule.pop('condicion'))
    result['source'] = _source_ref(model, root)
    result['many_to_many'] = [
        {'field': field.name, 'target': field.related_model._meta.label,
         'through': field.remote_field.through._meta.label,
         'through_table': field.remote_field.through._meta.db_table,
         'through_auto_created': bool(field.remote_field.through._meta.auto_created),
         'through_fields': list(field.remote_field.through_fields or []),
         'through_relations': [{'field': item.name, 'target': item.related_model._meta.label}
                               for item in field.remote_field.through._meta.concrete_fields
                               if item.is_relation and item.related_model]}
        for field in model._meta.many_to_many
    ]
    return result


def _routes(patterns, root, prefix='', namespaces=(), urlconf_source=None):
    rows = []
    for route in patterns:
        pattern = prefix + str(route.pattern)
        if isinstance(route, URLResolver):
            module = route.urlconf_module
            resolver_source = _source_ref(module, root) if inspect.ismodule(module) else urlconf_source
            rows.extend(_routes(route.url_patterns, root, pattern,
                                namespaces + ((route.namespace,) if route.namespace else ()), resolver_source))
        else:
            rows.append({'pattern': pattern, 'name': ':'.join((*namespaces, route.name)) if route.name else None,
                         'callback': _source_ref(getattr(route.callback, 'view_class', route.callback), root),
                         'urlconf_source': urlconf_source, 'verification': 'declared_resolved'})
    return rows


def build_system_map():
    """Build reproducible metadata from loaded Django declarations and tracked code bytes."""
    from api import ai_gateway_business_reads as business, ai_gateway_services as gateway
    from api.ai_gateway_assets import ASSET_READERS

    root = Path(settings.BASE_DIR).resolve()
    inventory = _source_inventory(root)
    models = [_model(model, root) for model in sorted(apps.get_models(), key=lambda model: model._meta.label)]
    tools, references = [], []
    for key, tool in sorted(gateway.TOOLS.items()):
        serializer = business.SERIALIZERS.get(key) or (ASSET_READERS[key][0] if key in ASSET_READERS else None)
        row = {'key': key, 'name': tool.name, 'data_domain': tool.data_domain,
               'operation_type': tool.operation_type, 'branch_scoped': tool.branch_scoped,
               'requires_approval': tool.requires_approval,
               'handler': _source_ref(tool.handler, root), 'access_check': _source_ref(tool.access_check, root),
               'execute_handler': _source_ref(tool.execute_handler, root) if tool.execute_handler else None,
               'registry_source': _source_ref(gateway, root),
               'schema_source': _source_ref(serializer or gateway, root),
               'argument_schema': _schema(serializer.argument_schema() if serializer else tool.argument_schema),
               'registry_argument_schema': _schema(tool.argument_schema), 'result_contract': _schema(tool.result_contract),
               'runtime_projection': 'business_read_bridge' if key in business.SERIALIZERS else 'asset_read_shadow' if key in ASSET_READERS else 'legacy_gateway_only',
               'authorization': 'not_evaluated; fresh server gate and actor scope required'}
        if key in business.SERIALIZERS:
            row['runtime_bridge'] = _source_ref(business.invoke, root)
            row['unit_of_analysis'] = business.UNITS[key]
            row['declared_relationships'] = business.RELATIONSHIPS[key]
        tools.append(row)
        for kind in ('handler', 'access_check', 'execute_handler', 'schema_source', 'runtime_bridge'):
            if row.get(kind):
                references.append({'from': key, 'to': row[kind]['symbol'], 'kind': kind, 'verification': 'declared_reference'})
    resolver = get_resolver()
    urlconf = resolver.urlconf_module
    result = {
        'schema_version': 1, 'scope': 'developer_structural_metadata',
        'source_commit': _source_commit(root), 'source_tree_hash': _digest(inventory['files']),
        'operational_rows_read': False, 'data_is_authority': False,
        'apps': [{'label': app.label, 'name': app.name, 'source': _source_ref(app.__class__, root),
                  'models': [row['modelo'] for row in models if row['modelo'].split('.')[0] == app.label],
                  'runtime_readiness': 'not_inferred_from_models_or_routes'}
                 for app in sorted(apps.get_app_configs(), key=lambda app: app.label)],
        'models': models,
        'routes': sorted(_routes(resolver.url_patterns, root, urlconf_source=_source_ref(urlconf, root) if inspect.ismodule(urlconf) else None),
                         key=lambda row: (row['pattern'], row['name'] or '', row['callback']['symbol'])),
        'tools': tools,
        'runtime_discovery': {'key': business.DISCOVERY_KEY, 'handler': _source_ref(business.invoke, root),
                              'schema_source': _source_ref(business.DiscoveryArguments, root),
                              'argument_schema': _schema(business.DiscoveryArguments.argument_schema())},
        'call_relationships': {'declared': references, 'static': [], 'inferred': [], 'unresolved': [], 'static_scan': 'not_performed'},
        'coverage': {'tracked_code_files': len(inventory['files']), 'inventory': inventory,
                     'modules': sorted({row['path'].split('/')[0] for row in inventory['files']}),
                     'semantic_completeness_proven': False,
                     'note': 'Django declarations and explicit registry references only; file counts do not prove functional coverage.'},
    }
    # Git HEAD is context only: committing generated output does not make unchanged code stale.
    result['content_hash'] = _digest({key: value for key, value in result.items() if key != 'source_commit'})
    return result
