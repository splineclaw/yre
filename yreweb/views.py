from django.shortcuts import render
from django.http import HttpResponse

from .yre.analysis import get_ten_similar

# Create your views here.
def index(request):
    return HttpResponse('index')

def similar_list(request, source_id):
    return(HttpResponse(str(get_ten_similar(source_id))))
