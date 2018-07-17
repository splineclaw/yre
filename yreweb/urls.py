from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('tuple/<int:source_id>/', views.similar_list, name='similar_list'),
    path('urls/<int:source_id>/', views.urls_list, name='urls_list'),
    path('<int:source_id>/', views.similar_pics, name='similar_pics')
]
