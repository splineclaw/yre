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

try:
    from bs4 import BeautifulSoup
except ImportError:
    # in case of differently named package
    from BeautifulSoup4 import BeautifulSoup


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
            yields post data
            403 on private
            404 on user_id not exist
        > could also search with fav:username (no reason to, though)

Alternate favorites retrieval (post-order):
    - e621 no longer provides favorites per post in its API
        (each user has to be included or excluded according to privacy preference,
         so the operation is expensive)
    - however, webpage providing same functionality is available
            /posts/{id}/favorites (paginated)
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

'''

class NetInterface():
    '''
    Provides connection to e6 and site-specific error handling.
    Does ratelimiting.
    '''

    def __init__(self):
        
        self.s = requests.session()
        self.s.headers.update({'user-agent': constants.USER_AGENT,
                                'login': constants.API_USER,
                                'api_key': constants.API_KEY})

        retries = Retry(
            total=2,
            backoff_factor=4,
            status_forcelist=[421, 500, 502, 520, 522, 524, 525]
            )

        self.s.mount('https://', HTTPAdapter(max_retries=retries))

        self.throttle_stop = 0 # timestamp of throttle conclusion

    def wait(self, throttle_duration=constants.PAGE_DELAY):
        while time.time() < self.throttle_stop:
            time.sleep(0.01)
        self.throttle_stop = time.time() + throttle_duration

    def get(self, url, params={}):
        throttle_duration = constants.REQUEST_DELAY if '.json' in url else constants.PAGE_DELAY
        self.wait(throttle_duration)
        response = self.s.get(url, params=params)
        logging.debug('get ({}) took {:.3f}s on {}'.format(
            response.status_code, response.elapsed.total_seconds(), response.url
        ))
        return response
    
    def fetch_user_fav_posts(self, user_id):
        '''
        From a user, fetch and return favorite posts.

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
        return tag.text.strip() is not None

    def fetch_users(self, a=None, b=None):
        '''
        Fetch and parse information on multiple users.

        a: get users after this user_id
        b: get users before this user_id

        note that favorite count ('favorites') is not included

        returns a list of dicts
               {user_id, user_name, join_date,
                posts, post_changes}
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
            # it seemed like a good idea at the time
            returnable = {}
            contents = []
            for col in row.text.split('\n'):
                c = col.strip()
                if c:
                    contents.append(c)
            for l in lookups:
                returnable[l] = contents[lookups[l]]
            link = row.find('a',attrs={'rel':'nofollow'})['href']
            returnable['user_id'] = int(link.split('/')[-1])
            results.append(returnable)

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

    def __del__(self):
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
            logging.debug('Did automatic commit (quantity criterion).')
        elif self.stale_time < time.time():
            self.do_commit()
            logging.debug('Did automatic commit (time criterion).')
            

    def do_commit(self):
        self.conn.commit()
        self.stale_ops = 0
        self.stale_time = time.time() + self.criterion_stale_time_interval


    def save_favs_from_user(self, user_name, post_ids):
        '''
        user_name : user whose favorites are to be stored
        post_ids : list of integer post ids of favorited posts

        no return
        '''
        for post_id in post_ids:
            self.c.execute(
                            '''INSERT INTO
                                post_favorites(post_id, favorited_user)
                                VALUES (%s,%s)
                                ON CONFLICT DO NOTHING''',
                            (post_id, user_name))
        self.did_op()
    
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

        logging.debug('Saved post {}'.format(str(id)))
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

        logging.debug('Saved user {} ({})'.format(datadict['user_id'], datadict['user_name']))
        self.did_op()












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


    def single_user_favs(self, user_id):
        '''
        Fetch and save favorite posts (and data therein) for a user,
        given their numeric user_id.

        Returns list of post ids.
        '''
        posts = self.net.fetch_user_fav_posts(user_id)
        logging.debug('Got {} favorites from user {}'.format(len(posts), user_id))
        post_ids = [p['id'] for p in posts]
        self.db.save_favs_from_user(user_id, post_ids)
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
                logging.debug('User crawl stopped: no new users')
                break
            if(end and new_a < end):
                running = False
                logging.debug('User crawl stopped: reached end id')
                break
            a = new_a
        logging.info('Done crawling users. Stopped at user_id {}.'.format(a))











def main():
    
    net = NetInterface()
    db = DBInterface()
    s = Scraper(net,db)

    # use user 326127 (VolcanicAsh) for fetch testing due to only 16 favorites

    #print(s.net.fetch_user_fav_posts(326127))
    #s.single_user_favs(326127)

    #db.save_user(net.fetch_user(326127))
    #print(net.fetch_users(a=326127))
    #s.multi_user(a=326127)
    s.crawl_all_users(start=9050)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()




