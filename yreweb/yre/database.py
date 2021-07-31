import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
import psycopg2
import logging

import json
import time
import random
from os.path import isfile, dirname, abspath
import inspect
import datetime
import dateutil.parser

try:
    from . import constants
except ImportError:
    import constants

try:
    from .utilities import *
except ImportError:
    from utilities import *


class Database():
    '''
    Database handles database access.
    For now, it also performs remote access.

    PRIORITY TODO:
    - fix save_tags to use dict
    - replace status with flag system
    - store score components
    - store creation time (now alphanumeric)
    - store file, sample, preview URLs from array group
    - resolve reliance on before_id

    TODO:
    - better remote error handling
    - fix post sampling progress indication when using stop condition
    '''
    def __init__(self):
        self.conn = psycopg2.connect("dbname='{}' user='{}' password='{}' host='{}'".format(
            constants.DB_NAME, constants.DB_USER, constants.DB_PASSWORD, constants.DB_HOST
        ))

        self.c = self.conn.cursor()
        self.s = requests.session()
        self.s.headers.update({'user-agent': constants.USER_AGENT,
                                'login': constants.API_USER,
                                'api_key': constants.API_KEY})

        retries = Retry(
            total=2,
            backoff_factor=1,
            status_forcelist=[421, 500, 502, 520, 522, 524, 525]
            )
        #self.s.mount('http://', HTTPAdapter(max_retries=retries))
        self.s.mount('https://', HTTPAdapter(max_retries=retries))

        self.commit_on_del = True

    def __del__(self):
        if self.commit_on_del:
            self.conn.commit()
        self.conn.close()
        del self

    def init_db(self):
        self.c.execute('''CREATE TABLE IF NOT EXISTS posts
            (id integer primary key, status text, fav_count integer, score integer, rating text,
            uploaded bigint, updated bigint, md5 text,
            full_url text, sample_url text, preview_url text,
            unique(id))''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS post_tags
            (post_id integer, tag_name text,
             unique(post_id, tag_name))''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS post_favorites
            (post_id integer, favorited_user text,
             unique(post_id, favorited_user))''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS favorites_subset
            (post_id integer, favorited_user text,
             unique(post_id, favorited_user))''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS favorites_meta
            (post_id integer, updated bigint,
             unique(post_id))''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS tags
            (id integer primary key, name text,
             count integer, type integer)''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS post_similars
                       (source_id integer, updated bigint,
                       sim_post integer, sim_rank integer,
                       unique(source_id,sim_rank))''')

        self.conn.commit()
        print("Database ready.")

    def save_tags(self, post_id, tag_string):
        '''
        todo: search for tags in db that are not in current tags
        '''
        print(post_id, tag_string)
        tags = tag_string.split(' ')

        for tag in tags:
            self.c.execute('''INSERT INTO post_tags(post_id, tag_name) VALUES
                              (%s, %s) ON CONFLICT DO NOTHING''',
                              (post_id,
                              tag))

    def save_post(self, post_dict, updated=None):
        if not updated:
            updated = time.time()
        d = post_dict  # for brevity

        print(d)

        #self.save_tags(d['id'], d['tags']) #disabled for now (now a dict rather than list)

        has_sample = 0
        if 'sample_url' in d and d['sample_url'] != d['file_url']:
            has_sample = 1

        has_preview = 0
        if 'preview_url' in d and d['preview_url'] != d['file_url']:
            has_preview = 1

        creation_time = dateutil.parser.isoparse(d['created_at']).timestamp()

        self.c.execute('''INSERT INTO posts VALUES
                      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                      ON CONFLICT (id) DO UPDATE SET
                      fav_count = EXCLUDED.fav_count,
                      score = EXCLUDED.score,
                      rating = EXCLUDED.rating,
                      updated = EXCLUDED.updated''',
                       (d['id'],
                        0, #d['status'], # disabled for now (changed to flag system)
                        d['fav_count'],
                        d['score']['total'],
                        d['rating'],
                        creation_time, # d['created_at'], changed to alphanumeric
                        updated,
                        d['file']['md5'],
                        d['file']['url'], #d['file'] if 'file_url' in d else 0, (changed to array group)
                        d['sample']['url'] if 'sample' in d else 0,
                        d['preview']['url'] if 'preview' in d else 0
                        ))

    def get_all_posts_forward(self, before_id=None, after_id=None,
                      stop_count=None, per_get_limit=320):
        # iterates from oldest to newest (low to high id)
        # starts above before_id (the id that is before the fetch interval)
        # and continues to id_after
        # can be limited to stop_count posts to fetch
        max_id = None
        count = 0
        if before_id == None:
            before_id = 0

        while before_id != -1:
            start = time.time()
            '''
            the old way
            r = self.s.get('https://e621.net/post/index.json',
                    params={'before_id': before_id, 'limit': '320'})
            '''
            '''
            From API Doc:
            page The page that will be returned.
            Can also be used with a or b + post_id to get the posts after or before the specified post ID.
            For example a13 gets every post after post_id 13 up to the limit.
            '''
            search = 'score:>={} -video'.format(constants.MIN_FAVS)

            r = self.s.get('https://e621.net/posts.json',
                    params={
                        'tags': search,
                        'limit': str(per_get_limit),
                        'page':'a{}'.format(before_id)})
            request_elapsed = time.time() - start
            logging.debug('GET to {} took {:.3f}s'.format(r.url, request_elapsed))
            logging.debug(r)
            
            j = json.loads(r.text)['posts'] # provides list of posts

            if len(j) > 0:
                count += len(j)
                t = time.time()
                for p in j:
                    self.save_post(p, updated=t)
                self.conn.commit()
                save_elapsed = time.time() - t
                before_id = max([p['id'] for p in j])

            else:
                # we've exhausted all posts
                before_id = -1
                break

            if after_id and (before_id >= after_id):
                before_id = -1
                break

            if stop_count and count >= stop_count:
                print('Stopping; sampled {} posts ({} target)'.format(
                    count, stop_count
                ))
                before_id = - 1
                break

            while time.time() - start < constants.PAGE_DELAY:
                # rate limit to 1 hz to nearest 10ms
                time.sleep(0.01)

    def get_newer_posts(self):
        self.c.execute('''SELECT MAX(id) FROM posts''')
        before_id = [id for id in self.c.fetchall()][0][0]
        print('Found newest post:', before_id)
        self.get_all_posts_forward(before_id=before_id)

    def get_post_ids(self):
        self.c.execute(
            '''select posts.id from posts''')
        results = self.c.fetchall()
        return [id[0] for id in results]

    def save_favs_from_user(self, user_id, post_ids):
        for post_id in post_ids:
            self.c.execute(
                          '''INSERT INTO
                             post_favorites(post_id, favorited_user)
                             VALUES (%s,%s)
                             ON CONFLICT DO NOTHING''',
                          (post_id, user_id))

        self.c.execute(
                        '''INSERT INTO
                            favorites_meta(post_id, updated)
                            VALUES (%s,%s)
                            ON CONFLICT DO NOTHING''',
                        (user_id, time.time()))


    def get_favs_from_user(self, user_id):
        '''
        used to be able to get users per post like so
        r = self.s.get('https://e621.net/favorite/list_users.json',
                       params={'id': id},
                       timeout=constants.FAV_REQ_TIMEOUT)

        functionality has been removed from API
        still available at https://e621.net/posts/{id}/favorites

        https://github.com/zwagoth/e621ng/blob/master/app/views/post_favorites/index.html.erb

        https://github.com/zwagoth/e621ng/issues/248
        The crux of the problem is that results have to be filtered by visibility on users.
        Loading thousands of user records takes a long time, lots of memory, etc.
        The way it's currently expressed in the database isn't conducive to rapid filtering on the database side either.
        Limiting this to a single direction(user->posts) solves a lot of the visibility check problems.


        Move to iterating across users? https://e621.net/users (no api endpoint)

        max user:  965269
        max post: 2852084

        '''
        r = self.s.get('https://e621.net/favorites.json',
                        params={'user_id': user_id})
        j = json.loads(r.text)
        post_ids = [p['id'] for p in j['posts']]
        self.save_favs(user_id, post_ids)
        return

    def sample_favs_from_posts(self, fav_limit = constants.MIN_FAVS):
        print('Reading known posts...')
        self.c.execute(
            '''select
                id, fav_count from posts
               where
                fav_count >= %s
               order by
                fav_count desc''',
               (fav_limit,))
        remaining = self.c.fetchall()

        q = len(remaining)
        print('{:,} posts to get (fav limit {}). Optimal time {}.'.format(
            q, fav_limit, seconds_to_dhms(q*constants.REQUEST_DELAY)))

        allstart = time.time()
        qty = 0

        for r, favs in remaining:
            start = time.time()
            self.get_favs(r)
            qty += 1
            print('Got favs for', r, 'in',
                  round(time.time()-start, 2), 'seconds.',
                  favs, 'favs.')
            if qty % 20 == 0:
                dt = time.time() - allstart
                rate = dt/qty
                eta = (q-qty) * rate
                print('Total {} in {:.2f}s: {:.3f}s/post. {} remain.'.format(
                        qty, dt, rate, seconds_to_dhms(eta)))

            self.conn.commit()


            while time.time() - start < constants.REQUEST_DELAY:
                time.sleep(0.001)

        print('All favorites sampled.')

    def find_similar_need_update(self):
        '''
        Return ids for which favorites are known but similars are not.
        '''
        remaining = [r[0] for r in self.c.execute(
            '''select distinct post_id from favorites_meta
               where post_id not in
               (select distinct source_id from similars)''')]
        return remaining

    def get_branch_favs(self, post_id, mode='partial'):
        '''
            returns list of tuples. each tuple contains:
            (post_id, branch_favs, post_favs)

            mode is one of 'partial' or 'full'.
            'partial' uses favorites_subset while 'full' uses post_favorites.
        '''
        source_db = 'post_favorites' if mode == 'full' else 'favorites_subset'
        self.c.execute('''
        select post_id, branch_favs, posts.fav_count from
        (select post_id, count(post_id) as branch_favs from {} where favorited_user in
            (select favorited_user from post_favorites as subtable where post_id = %s order by random() limit 256)
            group by post_id order by count(post_id) desc)
        as toptable inner join posts on post_id = posts.id
        '''.format(source_db),
        (post_id,))

        return self.c.fetchall()

    def write_similar_row(self, source_id, update_time, similar_list):
            insert_list = []
            for i, s in enumerate(similar_list):
                r = i + 1
                insert_list.append((source_id, update_time, s, r))

            self.c.executemany('''
                           insert into post_similars
                           values(%s,%s,%s,%s)
                           ON CONFLICT (source_id, sim_rank) DO UPDATE SET
                           updated = EXCLUDED.updated,
                           sim_post = EXCLUDED.sim_post
                           ''',
                           insert_list)

            self.conn.commit()

    def get_urls_for_ids(self, id_list):
        urls = []
        for id in id_list:
            self.c.execute('''
                           select sample_url from posts where id = %s
                           ''',
                           (id,))
            fetched = self.c.fetchall()
            if fetched:
                urls.append(fetched[0][0])
            else:
                urls.append('')
                print('No URL for {}!'.format(id))

        return urls

    def select_similar(self, source_id):
        self.c.execute('''select * from similars where source_id = %s''',
                     (source_id,))
        return self.c.fetchall()

    def select_similars(self, source_id):
        self.c.execute('''select * from post_similars where source_id = %s
                          order by sim_rank asc''',
                     (source_id,))
        return self.c.fetchall()

    def select_n_similar(self, source_id, limit=10):

        mode = constants.SYM_SIM_MODE
        self.c.execute('''select * from sym_similarity where low_id = %s or high_id = %s
                       order by {} desc limit %s'''.format(mode),
                     (source_id, source_id, limit))
        return self.c.fetchall()

    def update_favorites_subset(self, limit=constants.SUBSET_FAVS_PER_POST, fav_min=constants.MIN_FAVS, fav_max=9999):
        '''
        Loads only posts over favorite threshhold into table favorites_subset.
        Additionally limits number of favorites per post.
        Dramatically reduces compute time.
        '''
        print('Updating favorites subset. This will take several minutes.')
        start = time.time()

        print('Deleting old...')
        self.c.execute('''delete from favorites_subset''')

        print('Selecting and writing new subset, fav range {}-{}, limit {:,}...'.format(
            fav_min, fav_max, limit))

        post_ids = self.get_post_ids()

        q = max(post_ids)

        for id in post_ids:
            # print progress
            if id % 10000 == 0:
                print('{}/{}: {:5.2f}%'.format(
                    id, q, id/q * 100
                ))

            self.c.execute('''
                           insert into favorites_subset
                           select post_id, favorited_user from post_favorites
                                inner join posts on post_id = posts.id
                                where post_id = %s and
                                posts.fav_count >= %s and posts.fav_count <= %s
                                order by random()
                                limit %s''',
                                (id, fav_min, fav_max, limit))

        print('Committing changes.')
        self.conn.commit()
        status = 'Done with subset. Fav min {}, limit {:,}. Took {} ({:.4f}ms per post.)'.format(
            fav_min, limit, seconds_to_dhms(time.time()-start),
            (time.time()-start)*1000/q)
        print(status)
        #print('Vacuuming...')
        #self.c.execute('''vacuum''')
        return status

    def get_favcount_stats(self, fav_count):
        self.c.execute('''select count(*) from posts where fav_count=%s''',
                       (fav_count,))
        return self.c.fetchall()[0][0]

    def get_favcount(self, post_id):
        self.c.execute('''select fav_count from posts where id=%s''',
                        (post_id,))
        return self.c.fetchall()[0][0]

    def have_favs_for_id(self, source_id):
        '''
        returns boolean reflecting whether the source has had its favorites recorded.
        '''
        self.c.execute('''
                       select * from favorites_meta where post_id = %s
                       ''',
                       (source_id,))
        return self.c.fetchall()

    def have_post_for_id(self, source_id):
        '''
        returns boolean reflecting whether the post is in the posts table.
        '''
        self.c.execute('''
                       select * from posts where id = %s
                       ''',
                       (source_id,))
        return self.c.fetchall()

    def get_overlap(self, a, b):
        '''
        Returns the quantity of users who favorited both post a and b.
        '''
        self.c.execute(
            '''
            select count(favorited_user) from post_favorites
            where post_id = %s and favorited_user in
            (
            select favorited_user from post_favorites
            where post_id = %s
            )
            ''',
            (a, b,))
        return self.c.fetchall()[0][0]

    def get_post(self, id):
        self.get_all_posts(before_id=id+2, stop_count=1)

    def calc_and_put_sym_sim(self, low_id, high_id, verbose=False):
        '''
        Computes and inserts into the database the symmetric similarity between
        two posts, low_id and high_id.
        Fetches favs if necessary.
        '''
        if low_id > high_id:
            low_id, high_id = high_id, low_id

        a = high_id
        b = low_id

        badpair = False

        for id in [a,b]:
            if not self.have_favs_for_id(id):
                self.get_favs(id)
            if not self.have_post_for_id(id):
                self.get_post(id)

                # possible if deleted
                if not self.have_post_for_id(id):
                    print("NOTICE: ID {} does not exist".format(id))
                    badpair=True

        if badpair:
            # skip this pair
            return 1

        overlap = self.get_overlap(a, b)
        a_favs = self.get_favcount(a)
        b_favs = self.get_favcount(b)

        add_sim = overlap / (a_favs + b_favs - overlap)

        mult_sim = overlap**2 / (a_favs * b_favs)

        if verbose:

            print('a_favs {} overlap {} b_favs {}'.format(
                a_favs, overlap, b_favs))

            print('mutual {}    add_sim {:4f}   mult_sim {:4f}'.format(
                overlap, add_sim, mult_sim
            ))

        if add_sim > 1:
            print("ADD_SIM FOR {}, {} IS > 1  ({})".format(a, b, add_sim))
            add_sim = 1

        if mult_sim > 1:
            print("MULT_SIM FOR {}, {} IS > 1  ({})".format(a, b, mult_sim))
            mult_sim = 1

        self.write_sym_sim_row(low_id, high_id, overlap, add_sim, mult_sim)
        return 0

    def write_sym_sim_row(self, low_id, high_id, overlap, add_sim, mult_sim):
        self.c.execute('''
                       insert into sym_similarity values
                       (%s, %s, %s, %s, %s)
                       ON CONFLICT (low_id, high_id) DO UPDATE SET
                       common = EXCLUDED.common,
                       add_sim = EXCLUDED.add_sim,
                       mult_sim = EXCLUDED.mult_sim
                       ''',
                       (low_id, high_id, overlap, add_sim, mult_sim))






def main():
    db = Database()

    #db.init_db()

    #db.get_all_posts()
    db.get_newer_posts()

    db.sample_favs()

    db.update_favorites_subset()


if __name__ == '__main__':
    main()
