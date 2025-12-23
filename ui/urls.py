from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('login/', views.CustomLoginView.as_view(), name='login'),
    path('logout/', views.logout_view, name='logout'),
    path('tables/', views.tables, name='tables'),
    path('tables/<str:table_name>/', views.table_detail, name='table_detail'),
    path('query/', views.query, name='query'),
    path('module/<str:module_name>/', views.module_view, name='module_view'),
    path('api/files/', views.file_browser_api, name='file_browser_api'),
]
