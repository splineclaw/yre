import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
import sqlite3

import time
from tqdm import tqdm

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
    def __init__(self, db_path=None):
        self.path = dirname(abspath(inspect.getfile(inspect.currentframe())))
        self.db_path = self.path + '/db.sqlite' if not db_path else db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA journal_size_limit=104857600")
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

        self.commit_on_del = False

    def __del__(self):
        if self.commit_on_del:
            self.conn.commit()
        self.conn.close()
        del self

    def init_db(self):
        try:
            self.c.execute('''CREATE TABLE posts
                (id integer primary key, status text, fav_count integer, score integer, rating text,
                uploaded integer, updated real, md5 text,
                file_url text, sample_available integer, preview_available integer,
                unique(id))''')

            self.c.execute('''CREATE TABLE post_tags
                (post_id integer, tag_name text,
                 unique(post_id, tag_name))''')

            self.c.execute('''CREATE TABLE post_favorites
                (post_id integer, favorited_user text,
                 unique(post_id, favorited_user))''')

            self.c.execute('''CREATE TABLE favorites_subset
                (post_id integer, favorited_user text,
                 unique(post_id, favorited_user))''')

            self.c.execute('''CREATE TABLE favorites_meta
                (post_id integer, updated real,
                 unique(post_id))''')

            self.c.execute('''CREATE TABLE tags
                (id integer primary key, name text,
                 count integer, type integer)''')

            self.c.execute('''CREATE TABLE similar
                           (source_id integer primary key, updated real,
                           top_1 integer, top_2 integer, top_3 integer,
                           top_4 integer, top_5 integer, top_6 integer,
                           top_7 integer, top_8 integer, top_9 integer,
                           top_10 integer)''')

            self.c.execute('''CREATE TABLE similar_multi
                          (source_id integer,
                           related_id integer,
                           rank integer)''')

            self.c.execute('''CREATE TABLE sym_similarity
                           (low_id integer primary key, high_id integer,
                           common integer,
                           add_sim real, mult_sim real)''')

            self.conn.commit()
            print("Created database.")
        except sqlite3.OperationalError:
            print("Database exists.")

    def save_tags(self, post_id, tag_string):
        '''
        todo: search for tags in db that are not in current tags
        '''
        tags = tag_string.split(' ')

        for tag in tags:
            for retry in range(10):
                try:
                    self.c.execute('''INSERT OR IGNORE INTO post_tags(post_id, tag_name) VALUES
                                      (?,?)''',
                                      (post_id,
                                      tag))
                    break
                except sqlite3.OperationalError:
                    if retry == 9:
                        raise sqlite3.OperationalError
                    # database probably locked, back off a bit
                    time.sleep(random.random()*(retry+1)**1.2/10)

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

        for retry in range(10):
            try:
                self.c.execute('''INSERT OR REPLACE INTO posts VALUES
                              (?,?,?,?,?,?,?,?,?,?,?)''',
                               (d['id'],
                                d['status'],
                                d['fav_count'],
                                d['score'],
                                d['rating'],
                                d['created_at']['s'],
                                updated,
                                d['md5'] if 'md5' in d else 0,
                                d['file_url'] if 'file_url' in d else 0,
                                has_sample,
                                has_preview))
                break
            except sqlite3.OperationalError:
                if retry == 9:
                    raise sqlite3.OperationalError
                # database probably locked, back off a bit
                time.sleep(random.random()*(retry+1)**1.2/10)

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
        return [id[0] for id in self.c.execute(
            '''select id from posts''')]

    def save_favs(self, post_id, favorited_users):
        for u in favorited_users:
            for retry in range(20):
                try:
                    self.c.execute(
                                  '''INSERT OR IGNORE INTO
                                     post_favorites(post_id, favorited_user)
                                     VALUES (?,?)''',
                                  (post_id, u))
                    break
                except sqlite3.OperationalError:
                    if retry == 19:
                        raise sqlite3.OperationalError
                    print('Encountered lock saving favs. Retries:', retry)
                    # database probably locked, back off a bit
                    sleep = random.random() + 0.1
                    sleep *= 1.2**retry
                    time.sleep(sleep)

        for retry in range(10):
            try:
                self.c.execute(
                              '''INSERT OR IGNORE INTO
                                 favorites_meta(post_id, updated)
                                 VALUES (?,?)''',
                              (post_id, time.time()))
                break
            except sqlite3.OperationalError:
                if retry == 9:
                    raise sqlite3.OperationalError
                print('Encountered lock saving favs metadata. Retries:', retry)
                # database probably locked, back off a bit
                time.sleep(random.random()*(retry+1)**1.2/10)

    def get_favs(self, id):
        r = self.s.get('https://e621.net/favorite/list_users.json',
                       params={'id': id},
                       timeout=constants.FAV_REQ_TIMEOUT)
        j = json.loads(r.text)
        if 'favorited_users' not in j:
            print('No favs retrieved! Timed out?')
            return
        favorited_users = j['favorited_users'].split(',')
        if len(favorited_users) == 0:
            print('No favs retrieved! Timed out?')
        self.save_favs(id, favorited_users)
        return

    def sample_favs(self, fav_limit = constants.MIN_FAVS):
        # get favs for known ids that don't have fav list yet
        print('Reading known posts...')
        for retry in range(10):
            try:
                remaining = [r[0] for r in self.c.execute(
                    '''select distinct id from posts
                       where fav_count >= ? and
                       id not in
                       (select distinct post_id from favorites_meta)
                       order by fav_count desc''',
                       (fav_limit,))]
                break
            except sqlite3.OperationalError:
                print('Encountered lock reading unstored favs. Retries:', retry)
                if retry == 9:
                    raise sqlite3.OperationalError
                # database probably locked, back off a bit
                time.sleep(random.random()*(retry+1)**1.2/10)


        q = len(remaining)
        print('{:,} posts to get (fav limit {}). Optimal time {}.'.format(
            q, fav_limit, seconds_to_dhms(q*constants.REQUEST_DELAY)))

        allstart = time.time()
        qty = 0

        for r in remaining:
            start = time.time()
            self.get_favs(r)
            qty += 1
            print('Got favs for', r, 'in',
                  str(round(time.time()-start, 2)).zfill(2), 'seconds')
            if qty % 20 == 0:
                dt = time.time() - allstart
                rate = dt/qty
                eta = (q-qty) * rate
                print('Total {} in {:.2f}s: {:.3f}s/post. {} remain.'.format(
                        qty, dt, rate, seconds_to_dhms(eta)))
            for retry in range(10):
                try:
                    self.conn.commit()
                    break
                except sqlite3.OperationalError:
                    print('Encountered lock committing favs. Retries:', retry)
                    if retry == 9:
                        raise sqlite3.OperationalError
                    # database probably locked, back off a bit
                    time.sleep(random.random()*(retry+1)**1.2/10)

            while time.time() - start < constants.REQUEST_DELAY:
                time.sleep(0.001)

        print('All favorites sampled.')

    def find_similar_need_update(self):
        '''
        Return ids for which favorites are known but similars are not.
        '''
        for retry in range(10):
            try:
                remaining = [r[0] for r in self.c.execute(
                    '''select distinct post_id from favorites_meta
                       where post_id not in
                       (select distinct source_id from similar)''')]
                break
            except sqlite3.OperationalError:
                print('Encountered lock reading unstored favs. Retries:', retry)
                if retry == 9:
                    raise sqlite3.OperationalError
                # database probably locked, back off a bit
                time.sleep(random.random()*(retry+1)**1.2/10)
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
            (select favorited_user from post_favorites where post_id = ? order by random() limit 256)
            group by post_id order by count(post_id) desc)
        inner join posts on post_id = posts.id
        '''.format(source_db),
        (post_id,))

        raw = self.c.fetchall()

        return [tuple([int(i) for i in x]) for x in raw] # convert all values to ints

    def write_similar(self, source_id, update_time, similar_list):

        print(similar_list)

        multirows = []
        for r, s in enumerate(similar_list):
            multirows.append((source_id, s, r+1,))

        print(multirows)
        print(source_id)

        for retry in range(10):
            try:
                self.c.execute('''
                               insert or replace into similar
                               values (?,?,?,?,?,?,?,?,?,?,?,?)
                               ''',
                               (source_id, update_time, *similar_list[:10]))
                self.c.execute('''
                               delete from similar_multi where source_id = ?
                               ''',
                               (source_id,))
                self.c.executemany('''
                               insert or replace into similar_multi
                               values (?,?,?)
                               ''',
                               multirows)
                self.conn.commit()
                print("Similars written.")
                break
            except sqlite3.OperationalError:
                print('Encountered lock writing similars. Retries:', retry)
                if retry == 9:
                    raise sqlite3.OperationalError
                # database probably locked, back off a bit
                time.sleep(random.random()*(retry+1)**2/10)

    def have_favs_for_id(self, source_id):
        '''
        returns boolean reflecting whether the source has had its favorites recorded.
        '''
        for retry in range(10):
            try:
                self.c.execute('''
                               select * from favorites_meta where post_id = ?
                               ''',
                               (source_id,))
                return self.c.fetchall()
            except sqlite3.OperationalError:
                print('Encountered lock checking if fav recorded. Retries:', retry)
                if retry == 9:
                    raise sqlite3.OperationalError
                # database probably locked, back off a bit
                time.sleep(random.random()*(retry+1)**1.2/10)

    def get_urls_for_ids(self, id_list):
        urls = []
        for id in id_list:
            for retry in range(10):
                try:
                    self.c.execute('''
                                   select file_url from posts where id = ?
                                   ''',
                                   (id,))
                    fetched = self.c.fetchall()
                    if fetched:
                        urls.append(fetched[0][0])
                    else:
                        urls.append('')
                        print('No URL for {}!'.format(id))
                    break
                except sqlite3.OperationalError:
                    print('Encountered lock writing similar row. Retries:', retry)
                    if retry == 9:
                        raise sqlite3.OperationalError
                    # database probably locked, back off a bit
                    time.sleep(random.random()*(retry+1)**1.2/10)

        return urls

    def select_similar(self, source_id):
        for retry in range(10):
            try:
                self.c.execute('''select * from similar where source_id = ?''',
                             (source_id,))
                return self.c.fetchall()
            except sqlite3.OperationalError:
                print('Encountered lock writing similar row. Retries:', retry)
                if retry == 9:
                    raise sqlite3.OperationalError
                # database probably locked, back off a bit
                time.sleep(random.random()*(retry+1)**1.2/10)

    def update_favorites_subset(self, limit=constants.SUBSET_FAVS_PER_POST, fav_min=constants.MIN_FAVS, fav_max=1e9):
        '''
        Loads only posts over favorite threshhold
        into table favorites_subset. Dramatically reduces compute time.
        '''
        print('Updating favorites subset. This will take several minutes.')
        start = time.time()

        print('Deleting old...')
        self.c.execute('''delete from favorites_subset''')

        delete_end = time.time()

        print('Selecting and writing new subset, fav range {}-{}, limit {:,}...'.format(
            fav_min, fav_max, limit))

        post_ids = self.get_post_ids()

        q = max(post_ids)

        for id in tqdm(post_ids):
            # print progress
            '''if id % 10000 == 0:
                print('{}/{}: {:5.2f}%'.format(
                    id, q, id/q * 100
                ))'''

            self.c.execute('''
                           insert into favorites_subset
                           select post_id, favorited_user from post_favorites
                                inner join posts on post_id = posts.id
                                where post_id = ? and
                                posts.fav_count >= ? and posts.fav_count <= ?
                                order by random()
                                limit ?''',
                                (id, fav_min, fav_max, limit))

        execute_end = time.time()

        print('Committing changes.')
        self.conn.commit()

        commit_end = time.time()
        status = 'Done with subset. Fav min {}, limit {:,}. Took {} ({:.4f}ms per post.)'.format(
            fav_min, limit, seconds_to_dhms(time.time()-start),
            (time.time()-start)*1000/q)
        print(status)

        if constants.ENABLE_VACUUM:
            print('Vacuuming...')
            self.c.execute('''vacuum''')
        vacuum_end = time.time()

        print('Delete: {:.0}s. Execute: {:.0}s. Commit: {:.0}s. Vacuum: {:.0}s.'.format(
              delete_end - start, execute_end - delete_end, commit_end - execute_end,
              vacuum_end - commit_end
              ))
        return status

    def get_favcount_stats(self, fav_count):
        self.c.execute('''select count(*) from posts where fav_count=?''',
                       (fav_count,))
        return self.c.fetchall()[0][0]

    def get_favcount(self, post_id):
        self.c.execute('''select fav_count from posts where id=?''',
                        (post_id,))
        return self.c.fetchall()[0][0]

    def get_overlap(self, a, b):
        '''
        Returns the quantity of users who favorited both post a and b.
        '''
        self.c.execute(
            '''
            select count(favorited_user) from post_favorites
            where post_id = ? and favorited_user in
            (
            select favorited_user from post_favorites
            where post_id = ?
            )
            ''',
            (a, b,))
        return self.c.fetchall()[0][0]

    def calc_and_put_sym_sim(self, low_id, high_id):
        '''
        Computes and inserts into the database the symmetric similarity between
        two posts, low_id and high_id.
        Fetches favs if necessary.
        '''
        if low_id > high_id:
            low_id, high_id = high_id, low_id






def main(update_posts=True):
    db = Database()

    if isfile(db.db_path):
        if update_posts:
            db.get_newer_and_recent()

    else:
        db.init_db()
        db.get_all_posts()

    db.sample_favs()
    db.update_favorites_subset()


if __name__ == '__main__':
    #main(update_posts=False)
    db = Database()
    db.update_favorites_subset()
