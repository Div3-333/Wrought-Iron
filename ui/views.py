from django.shortcuts import render, redirect
from django.http import HttpResponse
from . import services
from .module_config import MODULE_CONFIG
import json

def index(request):
    try:
        stats = services.get_db_stats()
        audit_log = services.get_audit_log()
        context = {
            'stats': stats,
            'audit_log': audit_log,
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

from django.http import JsonResponse
import os

def file_browser_api(request):
    path = request.GET.get('path', '.')
    
    # Security: Prevent breaking out of project root if desired, 
    # but for a local tool, we usually want full system access. 
    # We'll resolve relative to CWD for initial '.' but allow absolute paths.
    
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
        # Add parent directory option if not at root
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
        
        # Sort: directories first, then files
        items.sort(key=lambda x: (x['type'] != 'dir', x['name'].lower()))
        
        return JsonResponse({
            'current_path': base_path,
            'items': items
        })
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

def module_view(request, module_name):
    config = MODULE_CONFIG.get(module_name)
    if not config:
        return redirect('index')
    
    result = None
    active_tab = list(config['commands'].keys())[0]

    if request.method == 'POST':
        command_name = request.POST.get('command_name')
        active_tab = command_name # Keep tab open
        
        # Collect args
        args = {}
        cmd_config = config['commands'].get(command_name, {})
        for arg in cmd_config.get('args', []):
            val = request.POST.get(arg['name'])
            if arg['type'] == 'checkbox':
                # Checkbox presence means true
                if arg['name'] in request.POST:
                    args[arg['name']] = True
            elif val:
                args[arg['name']] = val
        
        result = services.run_cli_command(module_name, command_name, args)

    context = {
        'page': module_name,
        'module_name': module_name,
        'config': config,
        'modules': MODULE_CONFIG.keys(),
        'result': result,
        'active_tab': active_tab
    }
    return render(request, 'module.html', context)