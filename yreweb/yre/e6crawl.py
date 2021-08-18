try:
    from . import constants
except ImportError:
    import constants

try:
    from .utilities import *
except ImportError:
    from utilities import *

import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
import psycopg2
import logging
import time

import dateutil.parser

import urllib.parse

import mechanize
import http.cookiejar

try:
    from bs4 import BeautifulSoup
except ImportError:
    # in case of differently named package
    from BeautifulSoup4 import BeautifulSoup

try:
    import coloredlogs
except ImportError:
    logging.info('Optional dependency coloredlogs not found.')



'''
e6crawl.py

Fetch data from e621.

Favorites retrieval operation (user-order):
    - update user directory
        no API endpoint, use /users (paginated)
            supports a/b syntax
        options for sort:
            join date
            name
            upload count
            note count
            post update count
    - select users expected to be most active
    - fetch favorites for each user
        > api endpoint /favorites.json
            takes numeric user id
            yields post data
            limited to 75 posts
            403 on private
            404 on user_id not exist
        > favorites page
            page: favorites?user_id=333077
                takes numeric user id
                appears like posts page
                paginated, does not support a/b syntax
                supports &limit=
                fav:<username> appears in search box
                seems to return more posts than search? 232 vs 225 pages of 3
        > posts search
            page: posts?tags=fav%3Asplineclaw+
                takes text user name
                supports &limit=
                paginated, does not support a/b syntax
            

Alternate favorites retrieval (post-order):
    - e621 no longer provides favorites per post in its API
        (each user has to be included or excluded according to privacy preference,
         so the operation is expensive)
    - however, webpage providing same functionality is available
            /posts/{id}/favorites (paginated)
            source: e621ng/app/views/post_favorites/index.html.erb
            limited to 80 per page, not adjustable
            exposes:
                user name
                user id
                user's favorite count
    - due to expense, use only for pages ill-served by user-order fetching
    - enables user discovery (for fetch prioritization)

"https://github.com/zwagoth/e621ng/issues/248
    The crux of the problem is that results have to be filtered by visibility on users.
    Loading thousands of user records takes a long time, lots of memory, etc.
    The way it's currently expressed in the database isn't conducive to rapid filtering on the database side either.
    Limiting this to a single direction(user->posts) solves a lot of the visibility check problems."

As of 2021-07 the maximum user id is roughly 1M and maximum post id is roughly 3M.
    Some users have hundereds of thousands of favorites.

As a side note, user-order retrieval is necessary for furaffinity favorites,
    but not furrynetwork promotions or twitter hearts/retweets.

Undocumented directory e621.net/db_export/ has daily .csv.gz files for:
    - pools
    - posts
    - tag_aliases
    - tag_implications
    - tags
    - wiki_pages

Images, when available, meet the following format:
(example post 1925483 md5 608cea6ab06a4b0dd78b5506e3f0fb1b)
Preview (150x150 crop, ~8KiB)
    /data/preview/
    https://static1.e621.net/data/preview/60/8c/608cea6ab06a4b0dd78b5506e3f0fb1b.jpg
Sample (800x800 crop, ~200KiB)
    /data/sample
    https://static1.e621.net/data/sample/60/8c/608cea6ab06a4b0dd78b5506e3f0fb1b.jpg
Original (15000x15000 limit, file size limit depends on file type)
    /data/
    https://static1.e621.net/data/60/8c/608cea6ab06a4b0dd78b5506e3f0fb1b.png

Maximum dimensions: 15000px in either direction.

Type	Deprecated	Maximum Filesize
PNG	        No	         75 MB	
JPEG	    No	         75 MB	
GIF	        No	         20 MB	
APNG	    No	         20 MB
    Filetype has to be .png instead of .apng.
    Extension can be simply renamed as both are valid extensions.
WebM	    No	        100 MB
    VP9 with Opus for audio preferred, VP8 and Vorbis supported. Matroska container (.mkv) not supported.
    Should be YUV420 8bit, audio either mono or stereo and 48KHz/44.1KHz.
SWF (Flash)	Yes	        100 MB
    No longer accepted as upload format. See forum #283261 for more information.


Details on search pages:

    Searches with additional pages have an <a id="paginator-next">.
    'article' includes
        post id
        has sound
        tags
        rating
        uploader id
        uploader name
        file extension
        score
        favcount
        is favorited
        file url
        sample url
        preview url

    Post hyperlink url includes post id

    Preview url (if exist) includes MD5

    Mouseover text includes:
        Rating
        ID
        Date (creation)
        Status
        Score (sum)
        Tags


Example valid URLs:
https://e621.net/posts?page=3&limit=5


How Long Will This Take?
    Assume:
        1/5 of users have favorites
        There are 1 million users
        Favorites are sampled in sets of 300
        Each query, hit or miss, takes 2 seconds
        There are 150 million favorites total
    Then:
        miss_count = 4/5 * 1e6 = 800e3
        hit_count = 150e6 / 300 = 500e3
        query_count = miss_count + hit_count = 1.3e6
        time = 1.3e6 * 2 s = 2.6e6 s = 30.09 days

'''

class NetInterface():
    '''
    Provides connection to e6 and site-specific error handling.
    Does ratelimiting.
    '''

    def __init__(self):
        
        self.s = requests.session()
        self.s.headers.update({'User-Agent': constants.USER_AGENT,
                                'login': constants.API_USER,
                                'api_key': constants.API_KEY})

        retries = Retry(
            total=None,
            connect=10,
            read=5,
            redirect=2,
            status=5, # most likely from experience, sometimes 520s will be thrown for ~30s
            backoff_factor=4, # 0, 4, 8, 16 ... seconds
            status_forcelist=[421, 500, 502, 520, 522, 524, 525]
            )
        retries.BACKOFF_MAX = 600, # wait no more than 10 minutes

        self.s.mount('https://', HTTPAdapter(max_retries=retries))

        self.throttle_stop = 0 # timestamp of throttle conclusion

        self.logger = logging.getLogger('e6crawl.NetInterface')

        self.logged_in = False
        

    def wait(self, throttle_duration=constants.PAGE_DELAY):
        while time.time() < self.throttle_stop:
            time.sleep(0.01)
        self.throttle_stop = time.time() + throttle_duration

    def get(self, url, params={}):
        throttle_duration = constants.REQUEST_DELAY if '.json' in url else constants.PAGE_DELAY
        self.wait(throttle_duration)
        response = self.s.get(url, params=params)
        self.logger.debug('get ({}) took {:.3f}s on {}'.format(
            response.status_code, response.elapsed.total_seconds(), response.url
        ))
        return response
    

    def do_login(self):
        self.logger.debug('Trying login.')
        br = mechanize.Browser()
        cj = http.cookiejar.CookieJar()
        br.set_cookiejar(cj)
        br.set_header('User-Agent', constants.USER_AGENT)
        br.set_handle_robots(False)

        br.open('https://e621.net/session/new')
        br.select_form(nr=0)
        br.form['name'] = constants.API_USER
        br.form['password'] = constants.USER_PASSWORD
        br.submit()

        for c in cj:
            self.logger.debug('Got cookie {} = {}'.format(c.name, c.value))
            print(c.name, c.name == '_danbooru_session')
            if c.name == '_danbooru_session':
                self.s.headers.update(
                    {'cookie': '{}={}'.format(
                        c.name, c.value
                    )}
                )          
                self.logger.info('Login successful.')
                self.logged_in = True
                break

        if not self.logged_in:
            raise self.LoginFailed()

    def login_if_needed(self):
        if not self.logged_in:
            self.do_login()


    class BadFormatException(Exception):
        '''
        Recieved data that did not meet expected format
        '''
        pass

    class LoginFailed(Exception):
        '''
        Login failed (did not find cookie)
        '''
        pass

    class EmailProtectedException(Exception):
        '''
        Detected protected email
        '''
        pass
    
    def fetch_api_user_fav_posts(self, user_id):
        '''
        From a user, fetch and return favorite posts, using the API.
        Limited to 75 favorites.

        user_id : numeric id of user
        
        Returns either
            - None, if no favorites are available
            - list of posts in JSON objects
        '''
        r = self.get('https://e621.net/favorites.json',
                        params={'user_id': user_id})
        if r.status_code in (403, 404):
            '''
            API endpoint /favorites.json:
                403 on private
                404 on user_id not exist
            '''
            return None
        
        j = json.loads(r.text)
        return j['posts']
    
    def fetch_user(self, user_id):
        '''
        Fetch and parse information on a single user from their page.

        Returns a dict of pertinent info.
                {user_id, user_name, join_date,
                posts, favorites, post_changes}
        '''
        data = {'user_id':user_id}

        url = 'https://e621.net/users/{}'.format(user_id)
        r = self.get(url)
        soup = BeautifulSoup(r.text, 'lxml')

        #with open('pagesamples/users-33077.html', 'r') as f:
        #    soup = BeautifulSoup(f, 'lxml')
        
        data['user_name'] = soup.find('a','user-member').text

        data['join_date'] = soup.find('th', text='Join Date').next_sibling.next_sibling.text

        pdat = [c for c in soup.find('th', text='Posts').next_sibling.next_sibling.children]
        data['posts'] = int(pdat[1].text)

        data['favorites'] = int(soup.find('th', text='Favorites').next_sibling.next_sibling.text)

        pdat = [c for c in soup.find('th', text='Post Changes').next_sibling.next_sibling.children]
        data['post_changes'] = int(pdat[1].text)

        return data

    def bs_tag_has_text(self, tag):
        return tag.string is not None

    def fetch_users(self, a=None, b=None):
        '''
        Fetch and parse information on multiple users.

        a: get users after this user_id
        b: get users before this user_id

        note that favorite count ('favorites') is not included

        returns a list of dicts
               {user_id, user_name, join_date,
                posts, post_changes}

        Some usernames are broken by Cloudflare's email protection. For now, these are skipped.

        142548, named qVmZ&HGv&xAS4k@^xgl7
        actual
            qVmZ&amp;HGv&amp;xAS4k@^xgl7
        cloudflare'd
            qVmZ&amp;HGv&amp;<span class="__cf_email__" data-cfemail="4a320b197e210a">[email protected]</span>^xgl7
        '''
        
        url = 'https://e621.net/users?page={}{}'.format(
            'a' if a is not None else 'b', a if a is not None else b
        )
        r = self.get(url)
        soup = BeautifulSoup(r.text, 'lxml')
        
        # with open('pagesamples/users-page=a333076.html', 'r') as f:
        #    soup = BeautifulSoup(f, 'lxml')

        table = soup.find('table')
        thead = table.find('thead')
        tbody = table.find('tbody')
        
        headings = [h.text for h in thead.find_all('th') if h.text]

        # ['Name', 'Posts', 'Deleted', 'Notes', 'Edits', 'Level', 'Joined']
        # indexes of column from which to get data
        lookups = {'user_name':0, 'join_date':6, 'posts':1, 'post_changes':4}
        results = []
        rows = tbody.find_all('tr')
        for row in rows:

            entries = [s for s in row.strings if not s=='\n']

            if len(entries) != 7:
                message = 'Expected user row length 7, got length {} ({})'.format(
                    len(entries), entries
                )
                self.logger.error(message)
                self.logger.debug(row)
            else:

                returnable = {}
                for l in lookups:
                    returnable[l] = entries[lookups[l]]

                link = row.find('a',attrs={'rel':'nofollow'})['href']
                returnable['user_id'] = int(link.split('/')[-1])
                results.append(returnable)

        if len(results) == 0:
            raise self.BadFormatException()
        return results
    
    def fetch_fav_ids(self, user_id):
        '''
        use  /favorites?user_id=  page to get post_ids favorited by a user_id

        returns list of unique post_ids
        '''
        base_url = 'https://e621.net'
        url = 'https://e621.net/favorites?user_id={}&limit=300'.format(user_id)
        ids = []

        while url:
            r = self.get(url)
            soup = BeautifulSoup(r.text, 'lxml')
            # get ids
            arts = soup.find_all('article')
            for art in arts:
                ids.append( art['data-id'] )
            
            next_btn = soup.find('a', id='paginator-next')
            if not next_btn:
                url = None
                break
            url = urllib.parse.urljoin(base_url, next_btn['href'])
        
        ids = list(set(ids))

        self.logger.debug('got {} favorites from user_id {}'.format(len(ids), user_id))
        return ids


    def fetch_post_favs(self, post_id):
        '''
        Get favorites of a post.
        /posts/{id}/favorites

        todo:
        - merge code between this and fetch_users

        returns: list of dicts {'user_name':, 'user_favcount':, 'user_id':}
        '''
        self.login_if_needed()

        base_url = 'https://e621.net'
        url = 'https://e621.net/posts/{}/favorites'.format(post_id)
        ids = []

        results = []

        while url:
            r = self.get(url)
            soup = BeautifulSoup(r.text, 'lxml')

            # do stuff

            table = soup.find('table')
            thead = table.find('thead')
            tbody = table.find('tbody')
            rows = tbody.find_all('tr')
            headings = [h.text for h in thead.find_all('th') if h.text]
            lookups = {'user_name':0, 'user_favcount':1}
            
            for row in rows:


                entries = [s for s in row.strings if not s=='\n']

                if len(entries) != 2:
                    message = 'Expected user row length 2, got length {} ({})'.format(
                        len(entries), entries
                    )
                    self.logger.error(message)
                    self.logger.debug(row)
                else:

                    returnable = {}
                    for l in lookups:
                        returnable[l] = entries[lookups[l]]

                    link = row.find('a',attrs={'rel':'nofollow'})['href']
                    returnable['user_id'] = int(link.split('/')[-1])
                    results.append(returnable)


            next_btn = soup.find('a', id='paginator-next')
            if next_btn:
                url = urllib.parse.urljoin(base_url, next_btn['href'])
            else:
                url = None
                break 

        #ids = list(set(ids))
        self.logger.debug('got {} favorites from post id {}'.format(len(data), post_id))
        #return ids
        return results









    

    

class DBInterface():
    '''
    Connect to and modify backing database.
    '''
    def __init__(self):
        self.conn = psycopg2.connect("dbname='{}' user='{}' password='{}' host='{}'".format(
            constants.DB_NAME, constants.DB_USER, constants.DB_PASSWORD, constants.DB_HOST
        ))

        self.c = self.conn.cursor()

        self.criterion_stale_ops_count = 50 # operations
        self.criterion_stale_time_interval = 120 # seconds

        self.stale_ops = 0 # number of operations since last commit
        # time at which database is considered stale
        self.stale_time = time.time() + self.criterion_stale_time_interval

        self.commit_on_del = True

        self.logger = logging.getLogger('e6crawl.DBInterface')

    def __del__(self):
        self.logger.debug('DB __del__, will {} commit'.format(
            'not' if not self.commit_on_del else ''
        ))
        if self.commit_on_del:
            self.conn.commit()
        self.conn.close()
        del self

    def did_op(self):
        '''
        Run whenever an operation is done.
        Handles automatic committing.
        '''
        self.stale_ops += 1

        if self.stale_ops >= 50:
            self.do_commit()
            self.logger.debug('Did automatic commit (quantity criterion).')
        elif self.stale_time < time.time():
            self.do_commit()
            self.logger.debug('Did automatic commit (time criterion).')
            

    def do_commit(self):
        self.conn.commit()
        self.stale_ops = 0
        self.stale_time = time.time() + self.criterion_stale_time_interval


    def save_favs_from_user(self, user_id, post_ids):
        '''
        user_name : user whose favorites are to be stored
        post_ids : list of integer post ids of favorited posts

        no return
        '''
        for post_id in post_ids:
            self.c.execute(
                            '''INSERT INTO
                                favorites(post_id, user_id)
                                VALUES (%s,%s)
                                ON CONFLICT DO NOTHING''',
                            (post_id, user_id))
            self.c.execute(
                            '''INSERT INTO
                                user_favorites_meta(user_id, updated)
                                VALUES (%s,%s)
                                ON CONFLICT (user_id) DO UPDATE SET
                                (user_id, updated) =
                                (EXCLUDED.user_id, EXCLUDED.updated)''',
                            (user_id, time.time()))
        self.did_op()

    def save_favs_from_post(self, post_id, user_ids):
        '''
        post_id : post whose favorites are to be stored
        user_ids : list of integer user ids of favoriting users

        no return
        '''
        for user_id in user_ids:
            self.c.execute(
                            '''INSERT INTO
                                favorites(post_id, user_id)
                                VALUES (%s,%s)
                                ON CONFLICT DO NOTHING''',
                            (post_id, user_id))
            self.c.execute(
                            '''INSERT INTO
                                post_favorites_meta(post_id, updated)
                                VALUES (%s,%s)
                                ON CONFLICT (post_id) DO UPDATE SET
                                (post_id, updated) =
                                (EXCLUDED.post_id, EXCLUDED.updated)''',
                            (user_id, time.time()))
        self.did_op()

    def save_user_favcounts(self, data):
        for d in data:
            try:
                self.c.execute(
                    '''
                    UPDATE users
                    SET favorites = %s, meta_updated = %s
                    WHERE user_id = %s
                    ''',
                    (d['user_favcount'], time.time(), d['user_id'])
                )
            except psycopg2.errors.NotNullViolation:
                pass

    def save_posts(self, posts):
        '''
        posts : list of JSON objects of post data
        '''
        for post in posts:
            self.save_post(post)

    def save_post(self, post):
        '''
        post : JSON object of post data
        '''

        id = post['id']
        created_at_timestamp = dateutil.parser.isoparse(post['created_at']).timestamp()
        updated_at_timestamp = dateutil.parser.isoparse(post['updated_at']).timestamp()
        change_seq = post['change_seq']
        meta_updated_timestamp = time.time()
        fav_count = post['fav_count']

        file = post['file']
        file_ext = file['ext']
        file_md5 = file['md5']
        file_size = file['size']
        file_width = file['width']
        file_height = file['height']
        
        sample = post['sample']
        sample_exist = sample['has']
        sample_width = sample['width']
        sample_height = sample['height']

        flags = post['flags']
        is_pending = flags['pending']
        is_flagged = flags['flagged']
        is_deleted = flags['deleted']

        scores = post['score']
        score = scores['total']
        score_up = scores['up']
        score_down = scores['down']

        #rated_s, rated_q, rated_e = [l is post['rating'] for l in ('s','q','e')]
        rating = post['rating']

        self.save_tags(id, post['tags'])
        self.save_relationships(id, post['relationships'])
        self.save_pools(id, post['pools'])
        self.save_sources(id, post['sources'])

        self.c.execute(
            '''
            INSERT INTO posts
            (id, md5, fav_count, rating, change_seq,
             score, score_up, score_down,
             created, updated, meta_updated,
             file_ext, file_size, file_width, file_height,
             sample_exist, sample_width, sample_height,
             is_pending, is_flagged, is_deleted
             )
            VALUES
            (%(id)s, %(md5)s, %(fav_count)s, %(rating)s, %(change_seq)s,
             %(score)s, %(score_up)s, %(score_down)s, 
             %(created)s, %(updated)s, %(meta_updated)s,
             %(file_ext)s, %(file_size)s, %(file_width)s, %(file_height)s,
             %(sample_exist)s, %(sample_width)s, %(sample_height)s,
             %(is_pending)s, %(is_flagged)s, %(is_deleted)s
              ) 
            ON CONFLICT (id) DO UPDATE SET
            (id, md5, fav_count, rating, change_seq,
             score, score_up, score_down,
             created, updated, meta_updated,
             file_ext, file_size, file_width, file_height,
             sample_exist, sample_width, sample_height,
             is_pending, is_flagged, is_deleted
             ) = 
             (EXCLUDED.id, EXCLUDED.md5, EXCLUDED.fav_count, EXCLUDED.rating, EXCLUDED.change_seq,
             EXCLUDED.score, EXCLUDED.score_up, EXCLUDED.score_down,
             EXCLUDED.created, EXCLUDED.updated, EXCLUDED.meta_updated,
             EXCLUDED.file_ext, EXCLUDED.file_size, EXCLUDED.file_width, EXCLUDED.file_height,
             EXCLUDED.sample_exist, EXCLUDED.sample_width, EXCLUDED.sample_height,
             EXCLUDED.is_pending, EXCLUDED.is_flagged, EXCLUDED.is_deleted
             )
            ''',
            {'id':id, 'md5':file_md5, 'fav_count':fav_count, 'score':score, 'score_up':score_up, 'score_down':score_down, 'rating':rating, 'change_seq':change_seq,
             'created':created_at_timestamp, 'updated':updated_at_timestamp, 'meta_updated':meta_updated_timestamp,
             'file_ext':file_ext, 'file_size':file_size, 'file_width':file_width, 'file_height':file_height,
             'sample_exist':sample_exist, 'sample_width':sample_width, 'sample_height':sample_height,
             'is_pending':is_pending, 'is_flagged':is_flagged, 'is_deleted':is_deleted}
        )

        self.logger.debug('Saved post {}'.format(str(id)))
        self.did_op()

    def save_tags(self, id, tags):
        '''
        TODO
        tags : JSON array group

        save post's tags, remove old tags, update tag metadata

        tags are given in named form, seperated by tag class.
        later: change these to tag id for storage efficiency
        '''
        pass

    def save_relationships(self, id, relationships):
        '''
        TODO
        relationships: JSON array group
        '''
        pass

    def save_pools(self, id, pools):
        '''
        TODO
        pools : list of pool IDs
        '''
        pass

    def save_sources(self, id, sources):
        pass

    def save_user(self, datadict):
        '''
        Save user information.
        '''
        if not 'favorites' in datadict:
            # happens if scraped from user listing
            datadict['favorites'] = None
        if not 'meta_updated' in datadict:
            datadict['meta_updated'] = time.time()
        self.c.execute(
            '''
            INSERT INTO users
            (
                user_id, user_name, join_date,
                posts, favorites, post_changes,
                meta_updated
            )
            VALUES
            (
                %(user_id)s, %(user_name)s, %(join_date)s,
                %(posts)s, %(favorites)s, %(post_changes)s,
                %(meta_updated)s
            )
            ON CONFLICT (user_id) DO UPDATE SET
            (
                user_id, user_name, join_date,
                posts, favorites, post_changes,
                meta_updated
            ) =
            (
                EXCLUDED.user_id, EXCLUDED.user_name, EXCLUDED.join_date,
                EXCLUDED.posts, EXCLUDED.favorites, EXCLUDED.post_changes,
                EXCLUDED.meta_updated
            )
            ''',
            datadict
        )

        self.logger.debug('Saved user {} ({})'.format(datadict['user_id'], datadict['user_name']))
        self.did_op()

    def fetch_user_ids_generator(self, start_id=0):
        '''
        Yields user ids one at a time. Suitable for slow iteration.
        '''
        has_data = True

        with self.conn.cursor() as id_c:
            id_c.execute('SELECT user_id FROM users WHERE user_id>=%s ORDER BY user_id', (start_id,))

            while has_data:
                fetch = id_c.fetchone()
                if not fetch:
                    has_data = False
                    break
                yield fetch[0]












class Scraper():
    '''
    Fetch data from e6 and save it to database.
    '''

    def __init__(self, net, db):
        '''
        net : NetInterface instance
        db : DBInterface instance
        '''
        self.net = net
        self.db = db

        self.logger = logging.getLogger('e6crawl.Scraper')


    def single_user_favs(self, user_id):
        '''
        Fetch and save favorite posts (and data therein) for a user,
        given their numeric user_id.

        Returns list of post ids.
        '''
        posts = self.net.fetch_user_fav_posts(user_id)
        self.logger.debug('Got {} favorites from user {}'.format(len(posts), user_id))
        post_ids = [p['id'] for p in posts]
        self.db.save_favs_from_user(user_id=user_id, post_ids=post_ids)
        self.db.save_posts(posts)
        return post_ids


    def single_user(self, user_id):
        '''
        Fetch and save information on a single user,
        given their numeric user_id.

        Returns user data dict.
        '''
        data = self.net.fetch_user(326127)
        self.db.save_user(data)
        return data


    def multi_user(self, a=None, b=None):
        '''
        Fetch and save information on multiple users.
        a: get users after this user_id
        b: get users before this user_id

        Returns list of user data dicts.
        '''
        multi_data = self.net.fetch_users(a, b)
        for user in multi_data:
            self.db.save_user(user)
        return multi_data

    
    def crawl_all_users(self, start=None, end=None):
        '''
        Crawl through user listing in ascending id order.
        Fetch and saves information on all users hit.

        Stop when no new users are returned in request,
        or when user_id 'end' is exceeded.

        start: first user_id to include in crawl
        end: stop after this user_id is included
        '''
        a = 0
        if start:
            a = max(0, start-1)

        running = True
        while running:
            multi_data = self.multi_user(a=a)
            new_a = max([u['user_id'] for u in multi_data])
            if(new_a <= a):
                running = False
                self.logger.debug('User crawl stopped: no new users')
                break
            if(end and new_a < end):
                running = False
                self.logger.debug('User crawl stopped: reached end id')
                break
            a = new_a
        self.logger.info('Done crawling users. Stopped at user_id {}.'.format(a))

    
    def crawl_favs_known_users(self, start_id=0):
        '''
        Get favorites from known user_ids in ascending order.
        Saves favorites graph and post information.
        '''

        # do loop here
        for user_id in self.db.fetch_user_ids_generator(start_id):
            
            favs = self.net.fetch_fav_ids(user_id)
            self.db.save_favs_from_user(user_id, favs)

    def single_post_favs(self, post_id):
        '''
        Fetch and save favorite information on a single post.

        Returns data dict.
        '''

        data = self.net.fetch_post_favs(post_id)
        user_ids = [u['user_id'] for u in data]
        user_favcounts = [u['user_favcount'] for u in data]
        self.db.save_favs_from_post(post_id=post_id, user_ids=user_ids)
        self.db.save_user_favcounts(data)
        return data










def main():
    
    net = NetInterface()
    db = DBInterface()
    s = Scraper(net,db)

    # use user 326127 (VolcanicAsh) for fetch testing due to only 16 favorites
    '''
    weird usernames
    ---------------
    user   5099 named !@N
    user   9022 named the bay6@hotmail.com
    user  97318 named $%#!@#$%%
    user 140590 named (two spaces)
    user 142548 named qVmZ&HGv&xAS4k@^xgl7
    

    other oddities
    --------------
    user 213725 (BlueDingo) has -13 favorites

    test accounts
    -------------
    user 326127 (VolcanicAsh): only 16 favorites
    user 333077 (splineclaw): hey that's me
    user 979066 (test_acct_pls_ignore): me, also, but safe
    '''

    #print(s.net.fetch_user_fav_posts(326127))
    #s.single_user_favs(326127)

    #db.save_user(net.fetch_user(326127))
    #print(net.fetch_users(a=326127))
    #s.multi_user(a=326127)
    #s.crawl_all_users(start=186609)

    #print(net.fetch_fav_ids(333077))

    #s.crawl_favs_known_users(10970)

    #net.do_login()
    #net.fetch_post_favs(2169530)

    s.single_post_favs(2169530)

if __name__ == '__main__':
    try:
        fmt = '%(asctime)s %(name)s[%(process)d] %(levelname)s %(message)s'
        datefmt = '%m-%d %H:%M:%S'
        coloredlogs.install(level='DEBUG', fmt=fmt, datefmt=datefmt)
    except NameError:
        logging.basicConfig(level=logging.DEBUG)
    main()




