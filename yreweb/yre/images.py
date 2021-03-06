import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from os.path import isfile, dirname, abspath
from os import makedirs
import inspect
from pathlib import Path
from shutil import copyfile

import time

try:
    from database import Database
    import constants
except ModuleNotFoundError:
    from .database import Database
    from . import constants

import urllib

def get_local(post_id, return_type='filename'):
    '''
    For a post id (12345), return its filename ('12345.jpg') as stored locally,
    downloading it from e621 if necessary.

    TODO: animation support
    '''
    self_path = dirname(abspath(inspect.getfile(inspect.currentframe())))
    previews_path = str(Path(self_path).parent) + '/static/yreweb/previews/'
    error_path = str(Path(self_path).parent) + '/static/yreweb/error.jpg'
    makedirs(previews_path, exist_ok=True)

    filename = str(post_id) + '.jpg'
    local_image = previews_path + filename

    if isfile(local_image):
        if return_type == 'filename':
            return filename
        elif return_type == 'cachehit':
            return (filename, 1)

    db = Database()
    urls = db.get_urls_for_ids([post_id])
    file_url = urls[0]

    prefix = 'https://static1.e621.net/data/'
    probable_sample_url = prefix + 'sample/' + file_url[len(prefix):-3] + 'jpg'

    try:
        urllib.request.urlopen(file_url)
        # success! time to download
        for attempt in range(10):
            try:
                urllib.request.urlretrieve(file_url, local_image)
                print('Downloaded', post_id)
                break
            except urllib.error.URLError:
                print('Download attempt {} failed on post {}.'.format(
                    attempt + 1, post_id
                ))
                time.sleep(0.2*1.5**attempt)
    except urllib.error.HTTPError:
        print('Could not download preview for', post_id)
        copyfile(error_path,local_image)
    if return_type == 'filename':
        return filename
    elif return_type == 'cachehit':
        return (filename, 0)

def image_with_delay(post_id):
    start = time.time()
    r = []
    i = 0
    while not r:
        r = get_local(post_id, 'cachehit')
        if not r:
            t = time.time()
            while time.time() - t < constants.REQUEST_DELAY * 2**i:
                time.sleep(0.01)
            i += 1


    name, hit = r
    if not hit:
        while time.time() - start < constants.REQUEST_DELAY:
            time.sleep(0.01)
    return name
