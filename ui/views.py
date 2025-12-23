from django.shortcuts import render, redirect
from django.http import HttpResponse
from . import services
import json

def index(request):
    try:
        stats = services.get_db_stats()
        audit_log = services.get_audit_log()
        context = {
            'stats': stats,
            'audit_log': audit_log,
            'page': 'dashboard'
        }
    except Exception as e:
        context = {'error': str(e), 'page': 'dashboard'}
    return render(request, 'index.html', context)

def tables(request):
    try:
        table_list = services.get_tables()
        context = {'tables': table_list, 'page': 'tables'}
    except Exception as e:
        context = {'error': str(e), 'page': 'tables'}
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
            'page': 'tables'
        }
    except Exception as e:
        context = {'error': str(e), 'page': 'tables'}
    return render(request, 'table_detail.html', context)

def query(request):
    context = {'page': 'query'}
    if request.method == 'POST':
        sql = request.POST.get('sql')
        if sql:
            result = services.execute_custom_sql(sql)
            context['result'] = result
            context['sql'] = sql
    return render(request, 'query.html', context)
