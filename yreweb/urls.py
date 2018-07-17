from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('<int:source_id>/', views.similar_list, name='similar_list')
]
