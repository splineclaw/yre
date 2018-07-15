import requests
import json
import sqlite3
import constants
import json
import time
import random

class Database():
    def __init__(self, db_path=None):
        self.db_path = 'db.sqlite' if not db_path else db_path
        self.conn = sqlite3.connect(self.db_path)
        self.c = self.conn.cursor()

    def __del__(self):
        self.conn.commit()
        self.conn.close()
        del self

    def init_db(self):
        try:
            self.c.execute('''CREATE TABLE posts
                (id integer primary key, status text, fav_count integer, score integer, rating text,
                uploaded integer, updated real, md5 text, file_url text)''')

            self.c.execute('''CREATE TABLE post_tags
                (rowid integer primary key, post_id integer, tag_name text)''')

            self.c.execute('''CREATE TABLE post_favorites
                (rowid integer primary key, post_id integer, favorited_user text)''')

            self.c.execute('''CREATE TABLE tags
                (id integer primary key, name text, count integer, type integer)''')

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
            self.c.execute('''INSERT OR IGNORE INTO post_tags(post_id, tag_name) VALUES
                              (?,?)''',
                              (post_id,
                              tag))

    def save_post(self, post_dict, updated = None):
        if not updated:
            updated = time.time()
        d = post_dict # for brevity

        self.save_tags(d['id'], d['tags'])

        self.c.execute('''INSERT OR REPLACE INTO posts VALUES
                        (?,?,?,?,?,?,?,?,?)''',
                        (d['id'],
                        d['status'],
                        d['fav_count'],
                        d['score'],
                        d['rating'],
                        d['created_at']['s'],
                        updated,
                        d['md5'] if 'md5' in d else 0,
                        d['file_url'] if 'file_url' in d else 0))



    def get_all_posts(self, before_id = None, after_id = 0):
        max_id = None
        while before_id != -1:
            start = time.time()
            r = requests.get('https://e621.net/post/index.json', params={'before_id':before_id, 'limit':'320'}, headers={'user-agent':constants.USER_AGENT})
            j = json.loads(r.text)

            if max_id == None:
                max_id = j[0]['id']

                print('Starting with {}'.format(max_id))
            else:
                quantity = max_id - after_id
                progress = (max_id - before_id - after_id) / quantity
                print('{}/{} ({}%)'.format(str(before_id).zfill(7),
                                           str(quantity).zfill(7),
                                           round(progress*100,2)))

            if len(j) > 0:
                t = time.time()
                for p in j:
                    db.save_post(p, updated=t)
                self.conn.commit()
                before_id = min([p['id'] for p in j])
            else:
                # we've exhausted all posts
                before_id = -1
                break

            if before_id < after_id:
                before_id = -1
                break

            while time.time() - start < 1:
                # rate limit to 1 hz
                time.sleep(0.01)

    def get_older_posts(self):
        # only useful for partial initial downloads
        before_id = [id for id in self.c.execute('''SELECT MIN(id) FROM posts''')][0][0]
        print('found', before_id)
        self.get_all_posts(before_id)

    def get_newer_posts(self):
        after_id = [id for id in self.c.execute('''SELECT MAX(id) FROM posts''')][0][0]
        print('found', after_id)
        self.get_all_posts(after_id=after_id)

    def save_favs(self, post_id, favorited_users):
        for u in favorited_users:
            self.c.execute('''INSERT OR IGNORE INTO post_favorites(post_id, favorited_user) VALUES
                              (?,?)''',
                              (post_id,
                              u))

    def get_favs(self, id):
        r = requests.get('https://e621.net/favorite/list_users.json', params={'id':id}, headers={'user-agent':'yre 0.0.00 (splineclaw)'})
        j = json.loads(r.text)
        favorited_users = j['favorited_users'].split(',')
        self.save_favs(id, favorited_users)


    def sample_favs(self):
        print('Reading known posts...')
        # list of post ids with at least one favorite
        posts = [r[0] for r in self.c.execute('''select id from posts where fav_count > 0''')]

        print('Reading sampled posts...')
        # list of posts already sampled, including in db
        sampled = [r[0] for r in self.c.execute('''select distinct post_id from post_favorites''')]
        # posts - sampled
        remaining = [p for p in posts if not p in sampled]

        random.shuffle(remaining)

        for r in remaining:
            start = time.time()
            print('Sampling post', r)
            self.get_favs(r)
            self.conn.commit()

            while time.time() - start < 1:
                time.sleep(0.01)



if __name__ == '__main__':
    db = Database()
    db.init_db()

    db.get_all_posts()
    db.sample_favs()
