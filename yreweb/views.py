from django.shortcuts import render
from django.http import HttpResponse, HttpResponseRedirect

from .yre.analysis import get_ten_similar
from .yre.database import Database

# Create your views here.
def index(request):
    #redirect to example
    return HttpResponseRedirect('/1479413/')

def similar_list(request, source_id):
    return(HttpResponse(str(get_ten_similar(source_id))))

def urls_list(request, source_id):
    similar_ids = get_ten_similar(source_id)
    db = Database()
    urls = db.get_urls_for_ids(similar_ids)
    return(HttpResponse(str(urls)))

def similar_pics(request, source_id):
    similar_ids = get_ten_similar(source_id)[:8]
    db = Database()
    urls = db.get_urls_for_ids(similar_ids)

    urls_preview = []
    for u in urls:
        slicepoint = u.find('/data/')+len('/data/')
        new_url = u[:slicepoint]
        new_url += 'sample/'
        new_url += u[slicepoint:]
        urls_preview.append(new_url)

    e6prefix = 'http://localhost:8000/'#'https://e621.net/post/show/'
    e6urls = [e6prefix+str(id) for id in similar_ids]

    zipped = list(zip(similar_ids, urls, e6urls))

    context = {'source': source_id, 'zipped': zipped}
    return render(request, 'yreweb/response.html', context)
