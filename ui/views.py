from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse
from . import services
from .module_config import MODULE_CONFIG
import json
import os

def index(request):
    try:
        stats = services.get_db_stats()
        audit_log = services.get_audit_log()
        table_counts = services.get_table_counts()
        context = {
            'stats': stats,
            'audit_log': audit_log,
            'table_counts': table_counts,
            'page': 'dashboard',
            'modules': MODULE_CONFIG.keys()
        }
    except Exception as e:
        context = {
            'error': str(e), 
            'page': 'dashboard', 
            'modules': MODULE_CONFIG.keys()
        }
    return render(request, 'index.html', context)

def tables(request):
    try:
        table_list = services.get_tables()
        context = {
            'tables': table_list, 
            'page': 'tables',
            'modules': MODULE_CONFIG.keys()
        }
    except Exception as e:
        context = {
            'error': str(e), 
            'page': 'tables',
            'modules': MODULE_CONFIG.keys()
        }
    return render(request, 'tables.html', context)

def table_detail(request, table_name):
    try:
        data, columns = services.get_table_preview(table_name)
        schema = services.get_table_schema(table_name)
        context = {
            'table_name': table_name,
            'columns': columns,
            'data': data,
            'schema': schema,
            'page': 'tables',
            'modules': MODULE_CONFIG.keys()
        }
    except Exception as e:
        context = {
            'error': str(e), 
            'page': 'tables',
            'modules': MODULE_CONFIG.keys()
        }
    return render(request, 'table_detail.html', context)

def query(request):
    context = {
        'page': 'query',
        'modules': MODULE_CONFIG.keys()
    }
    if request.method == 'POST':
        sql = request.POST.get('sql')
        if sql:
            result = services.execute_custom_sql(sql)
            context['result'] = result
            context['sql'] = sql
    return render(request, 'query.html', context)

def module_view(request, module_name):
    config = MODULE_CONFIG.get(module_name)
    if not config:
        return redirect('index')
    
    # Fetch dynamic data
    try:
        available_tables = services.get_tables()
        schemas = services.get_all_table_schemas()
    except:
        available_tables = []
        schemas = {}

    # Deep copy config
    view_config = json.loads(json.dumps(config))
    
    # List of argument names that represent columns (for auto-dropdown conversion)
    column_args = [
        'col', 'column', 'x', 'y', 'cat', 'num', 'lat_col', 'lon_col', 
        'target_col', 'addr_col', 'id_col', 'group_cols', 'index', 'values', 'stack'
    ]

    for cmd_name, cmd_data in view_config.get('commands', {}).items():
        for arg in cmd_data.get('args', []):
            # Inject Tables
            if arg['name'] in ['table_name', 'table', 'target_db', 'source_db', 'table_a', 'table_b'] and arg.get('type') == 'pos':
                arg['type'] = 'select'
                arg['options'] = available_tables
                if available_tables:
                    arg['default'] = available_tables[0]
                arg['is_table_selector'] = True # Mark for JS
            
            # Inject Columns (Mark them for JS population)
            if arg['name'] in column_args and arg.get('type') == 'pos':
                arg['type'] = 'select'
                arg['options'] = [] # Will be filled by JS
                arg['is_column_selector'] = True # Mark for JS

    result = None
    active_tab = list(view_config['commands'].keys())[0]

    if request.method == 'POST':
        command_name = request.POST.get('command_name')
        active_tab = command_name
        
        args = {}
        cmd_config = view_config['commands'].get(command_name, {})
        
        for arg in cmd_config.get('args', []):
            val = request.POST.get(arg['name'])
            if arg['type'] == 'flag':
                if arg['name'] in request.POST:
                    args[arg['name']] = True
            elif val:
                args[arg['name']] = val
        
        result = services.run_cli_command(module_name, command_name, args)

    context = {
        'page': module_name,
        'module_name': module_name,
        'config': view_config,
        'modules': MODULE_CONFIG.keys(),
        'result': result,
        'active_tab': active_tab,
        'schemas_json': json.dumps(schemas) # Pass full schema map to frontend
    }
    return render(request, 'module.html', context)

def file_browser_api(request):
    path = request.GET.get('path', '.')
    
    if path == '.':
        base_path = os.getcwd()
    else:
        base_path = path
        
    if not os.path.exists(base_path):
        return JsonResponse({'error': 'Path not found'}, status=404)
        
    if not os.path.isdir(base_path):
        return JsonResponse({'error': 'Not a directory'}, status=400)
        
    try:
        items = []
        if os.path.dirname(base_path) != base_path:
            items.append({
                'name': '..',
                'path': os.path.dirname(base_path),
                'type': 'dir'
            })
            
        with os.scandir(base_path) as it:
            for entry in it:
                items.append({
                    'name': entry.name,
                    'path': entry.path,
                    'type': 'dir' if entry.is_dir() else 'file'
                })
        
        items.sort(key=lambda x: (x['type'] != 'dir', x['name'].lower()))
        
        return JsonResponse({
            'current_path': base_path,
            'items': items
        })
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)