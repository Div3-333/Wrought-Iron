from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('tables/', views.tables, name='tables'),
    path('tables/<str:table_name>/', views.table_detail, name='table_detail'),
    path('query/', views.query, name='query'),
]