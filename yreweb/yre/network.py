import threading
import queue
import time
import json

try:
    from . import constants, database, utilities
except ImportError:
    import constants, database, utilities

class sampleThread(threading.Thread):
    def __init__(self, q, lock, orch):
        threading.Thread.__init__(self)
        self.q = q
        self.lock = lock
        self.orch = orch

    def run(self):
        while not self.q.empty():
            self.lock.acquire()
            self.orch.wait_for_run()
            next_id = self.q.get()
            self.lock.release()

            print('{} '.format(next_id), end='', flush = True)
            self.save_id_favs(next_id)


    def save_id_favs(self, id):
        db = database.Database()
        r = db.s.get('https://e621.net/favorite/list_users.json',
                       params={'id': id},
                       timeout=constants.FAV_REQ_TIMEOUT)
        try:
            j = json.loads(r.text)
        except json.decoder.JSONDecodeError as e:
            print(e)
            print('Got status code {}'.format(r.status_code))
            print('Full text recieved:\n' + r.text)
            return

        if 'favorited_users' not in j:
            print('Favorited users not returned! Timed out?')
            print(j)
            return
        favorited_users = j['favorited_users'].split(',')
        if len(favorited_users) == 0:
            print('No users in returned data! Timed out?')
            return
        db.save_favs(id, favorited_users)


class Orchestrator:
    # Provides delay handling for a group.
    # Activate lock before calling!

    def __init__(self, period):
        self.period = period
        self.next_run = 0

    def wait_for_run(self):
        td = self.next_run - time.time() # timedelta. future is positive.
        if td < 0:
            if self.next_run > 0: # omit first run
                print("Can't keep up! {:.2f}ms of {:.2f}ms behind. Increase thread count.".format(
                    td*-1000, self.period*1000
                ))
            self.next_run = time.time() + self.period
            return 0
        else:
            self.next_run += self.period
            time.sleep(td)
            return td


def sample_favs_threaded():
    db = database.Database()

    print('Reading known posts...')
    db.c.execute(
        '''select
            id, fav_count from posts
           where
            fav_count >= %s
           order by
            fav_count desc''',
           (constants.MIN_FAVS,))

    lock = threading.Lock()
    q = queue.Queue(0)
    orch = Orchestrator(constants.REQUEST_DELAY)

    results = db.c.fetchall()

    for id, favs in results:
        q.put(id)

    threads = []

    for v in range(10): # thread count
        thread = sampleThread(q, lock, orch)
        thread.start()
        threads.append(thread)

    while not q.empty():
        time.sleep(60)
        print('\n ----- {} remain in queue, {} remaining -----'.format(
            q.qsize(),
            utilities.seconds_to_dhms(q.qsize()*constants.REQUEST_DELAY)))

    for t in threads:
        t.join()

    print('All favorites sampled.')

if __name__ == '__main__':
    sample_favs_threaded()
