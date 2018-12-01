import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
import psycopg2

import json
import time
import random
from os.path import isfile, dirname, abspath
import inspect

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

    TODO:
    - better remote error handling
    - fix post sampling progress indication when using stop condition
    '''
    def __init__(self):
        self.conn = psycopg2.connect("dbname=yre user=postgres")

        self.c = self.conn.cursor()
        self.s = requests.session()
        self.s.headers.update({'user-agent': constants.USER_AGENT})

        retries = Retry(
            total=10,
            backoff_factor=1,
            status_forcelist=[421, 500, 502, 520, 522, 524, 525]
            )
        self.s.mount('http://', HTTPAdapter(max_retries=retries))
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

        self.c.execute('''CREATE TABLE IF NOT EXISTS similars
                       (source_id integer primary key, updated bigint,
                       top_1 integer, top_2 integer, top_3 integer,
                       top_4 integer, top_5 integer, top_6 integer,
                       top_7 integer, top_8 integer, top_9 integer,
                       top_10 integer)''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS sym_similarity
                       (low_id integer, high_id integer,
                       common integer,
                       add_sim real, mult_sim real,
                       unique(low_id, high_id))
                       ''')

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

        self.save_tags(d['id'], d['tags'])

        has_sample = 0
        if 'sample_url' in d and d['sample_url'] != d['file_url']:
            has_sample = 1

        has_preview = 0
        if 'preview_url' in d and d['preview_url'] != d['file_url']:
            has_preview = 1

        self.c.execute('''INSERT INTO posts VALUES
                      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                      ON CONFLICT (id) DO UPDATE SET
                      fav_count = EXCLUDED.fav_count,
                      score = EXCLUDED.score,
                      rating = EXCLUDED.rating,
                      updated = EXCLUDED.updated''',
                       (d['id'],
                        d['status'],
                        d['fav_count'],
                        d['score'],
                        d['rating'],
                        d['created_at']['s'],
                        updated,
                        d['md5'] if 'md5' in d else 0,
                        d['file_url'] if 'file_url' in d else 0,
                        d['sample_url'] if 'sample_url' in d else 0,
                        d['preview_url' if 'preview_url' in d else 0]))

    def get_all_posts(self, before_id=None, after_id=0, stop_count=None):
        max_id = None
        count = 0
        while before_id != -1:
            start = time.time()
            r = self.s.get('https://e621.net/post/index.json',
                    params={'before_id': before_id, 'limit': '320'})
            request_elapsed = time.time() - start
            j = json.loads(r.text)

            if len(j) > 0:
                count += len(j)
                t = time.time()
                for p in j:
                    self.save_post(p, updated=t)
                self.conn.commit()
                save_elapsed = time.time() - t
                before_id = min([p['id'] for p in j])

            else:
                # we've exhausted all posts
                before_id = -1
                break

            if max_id is None:
                max_id = j[0]['id']
                print('Starting with {}'.format(max_id))
            else:
                # print progress and statistics
                quantity = max_id - after_id
                progress = (max_id - before_id - after_id) / quantity
                print('{}/{} ({:05.2f}%)  req: {:04.3f}s, save: {:04.3f}s'.format(
                                           str(before_id).zfill(7),
                                           str(quantity).zfill(7),
                                           progress*100,
                                           request_elapsed,
                                           save_elapsed))

            if before_id < after_id:
                before_id = -1
                break

            if stop_count and count >= stop_count:
                print('Stopping; sampled {} posts ({} target)'.format(
                    count, stop_count
                ))
                before_id = - 1
                break

            while time.time() - start < constants.PAGE_DELAY:
                # rate limit to 1 hz
                time.sleep(0.001)

    def get_older_posts(self):
        # only useful for partial initial downloads
        before_id = [id for id in self.c.execute(
            '''SELECT MIN(id) FROM posts''')][0][0]
        print('Found oldest post:', before_id)
        self.get_all_posts(before_id)

    def get_newer_posts(self):
        after_id = [id for id in self.c.execute(
            '''SELECT MAX(id) FROM posts''')][0][0]
        print('Found newest post:', after_id)
        self.get_all_posts(after_id=after_id)

    def get_recent_posts(self, stop_count=1000):
        print('Getting newest {} posts.'.format(stop_count))
        self.get_all_posts(stop_count=stop_count)

    def get_newer_and_recent(self, recent_count=1000):
        after_id = [id for id in self.c.execute(
            '''SELECT MAX(id) FROM posts''')][0][0]
        print('Found newest post:', after_id)
        self.get_all_posts(after_id=after_id - recent_count)

    def get_post_ids(self):
        self.c.execute(
            '''select posts.id from posts''')
        results = self.c.fetchall()
        return [id[0] for id in results]

    def save_favs(self, post_id, favorited_users):
        for u in favorited_users:
            self.c.execute(
                          '''INSERT INTO
                             post_favorites(post_id, favorited_user)
                             VALUES (%s,%s)
                             ON CONFLICT DO NOTHING''',
                          (post_id, u))

            self.c.execute(
                          '''INSERT INTO
                             favorites_meta(post_id, updated)
                             VALUES (%s,%s)
                             ON CONFLICT DO NOTHING''',
                          (post_id, time.time()))

    def get_favs(self, id):
        r = self.s.get('https://e621.net/favorite/list_users.json',
                       params={'id': id},
                       timeout=constants.FAV_REQ_TIMEOUT)
        j = json.loads(r.text)
        if 'favorited_users' not in j:
            print('No favs retrieved! Timed out%s')
            return
        favorited_users = j['favorited_users'].split(',')
        if len(favorited_users) == 0:
            print('No favs retrieved! Timed out%s')
        self.save_favs(id, favorited_users)
        return

    def sample_favs(self, fav_limit = constants.MIN_FAVS):
        print('Reading known posts...')
        self.c.execute(
            '''select
                id, fav_count from posts
               where
                fav_count >= %s and
                id not in
                 (select post_id from favorites_meta
                  where post_id is not null)
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
            self.c.execute('''
                           insert into similars
                           values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT DO NOTHING
                           ''',
                           (source_id, update_time, *similar_list[:10]))

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

    def have_favs_for_id(self, source_id):
        '''
        returns boolean reflecting whether the source has had its favorites recorded.
        '''
        self.c.execute('''
                       select * from favorites_meta where post_id = %s
                       ''',
                       (source_id,))
        return self.c.fetchall()

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

        if not self.have_favs_for_id(a):
            self.get_favs(a)
        if not self.have_favs_for_id(b):
            self.get_favs(b)
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

    db.init_db()
    #db.get_all_posts()

    db.sample_favs()

    db.update_favorites_subset()


if __name__ == '__main__':
    main()
