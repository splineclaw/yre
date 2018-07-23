try:
    from .database import Database
    from .utilities import *
    from . import constants

except:
    from database import Database
    from utilities import *
    import constants

import time
import random
import math



def get_ten_similar(source_id,
                    stale_time=constants.DEFAULT_STALE_TIME,
                    from_full=False):
    '''
    Returns a list of the 10 most similar posts to the source.
    Uses database to cache results.

    Default stale time is 10**6 seconds, or 11.6 days.
    '''

    compute_print = True  # show table of statistics?

    print('Getting top ten similar for', source_id)

    # check if in db
    db = Database()
    results = db.select_similar(source_id)

    if len(results) == 0:
        # not yet in db. let's add it!
        print('Not in database. Fetching...')
        top_ten = compute_similar(source_id,
                                  from_full=from_full,
                                  print_enabled=compute_print)

    else:
        print('Found in database.')
        result = results[0]
        last_time = result[1]
        top_ten = result[-10:]

        age = time.time() - last_time

        if age > stale_time:
            # this hasn't been updated in a while.
            print('Cache stale ({} old). Fetching...'.format(
                seconds_to_dhms(age)
            ))
            top_ten = compute_similar(source_id,
                                      from_full=from_full,
                                      print_enabled=compute_print)

    return top_ten


def compute_similar(source_id, from_full=False, print_enabled=False):
    '''
    computes top 10 similar to the source, saves it to the database,
    and returns their ids as a list
    '''

    min_branch_favs = 2
    min_post_favs = constants.MIN_FAVS

    print('Finding similar to', source_id)
    if from_full:
        print('FROM FULL DATASET ENABLED! Get comfortable, this will take a while.')

    db = Database()

    if not db.have_favs_for_id(source_id):
        # post not in database. let's fetch it and recalculate.
        print('Post favorites not in database, fetching...')
        db.get_favs(source_id)

    print('Finding common favorites...')
    results = db.get_branch_favs(source_id)

    source_favs = max([r[1] for r in results])

    print('Computing...')

    posts = []
    for r in results:
        # (post_id, branch_favs, post_favs)
        if r[0] == source_id or r[1] < min_branch_favs or r[2] < min_post_favs:
            # exclude the source and posts with insufficient favs
            continue
        # branch_favs / post_favs
        # the fraction of target favoriters who are also source favoriters
        denom = r[2] if from_full else min(r[2], constants.MIN_FAVS)
        relevance = r[1]/denom
        # branch_favs / source_favs
        # the fraction of source favoriters who are also target favoriters
        popularity = r[1] / source_favs

        product = relevance * popularity

        # id, branch_favs, post_favs, popularity, relevance, product ...
        posts.append((*r, popularity, relevance, product))

    print('Sorting...')

    product_sorted = [x for x in sorted(posts,
                                        key=lambda x: (x[-1]),
                                        reverse=True)][:10]
    if print_enabled:
        linewidth = 99
        print('\n'+'-'*linewidth)
        print('SORTED BY SIMILARITY')
        for r in product_sorted:
            print(
                   ('id:{:7d}  common favs:{:4d}  total favs:{:4d}  ' +
                    'popularity:{:.4f}  relevance:{:.4f}  ' +
                    'product:{:.4f}'
                    ).format(*r)
                  )

    top_ten_ids = [x[0] for x in product_sorted]

    if top_ten_ids:
        # if there are fewer than 10 similar posts, fill with zeros
        top_ten_ids = (top_ten_ids + [0]*10)[:10]

        db.write_similar_row(source_id, time.time(), top_ten_ids)

        return top_ten_ids
    else:
        return None


def presample_randomly():
    db = Database()

    print('Searching for posts needing similars computed...')
    need_update = db.find_similar_need_update()
    random.shuffle(need_update)

    period = 0

    for id in need_update:
        start = time.time()
        compute_similar(id)
        delta = time.time() - start
        if not period:
            period = delta
        else:
            # ema 20
            period = period * 0.95 + delta * 0.05

        print('Similar computation took {:5.2f}s. {:5.2f} per minute.'.format(
            delta, 60/period
        ))

def presample_tree(root_id):
    db = Database()

    traversed = [root_id]
    unsampled = []

    unsampled = list(get_ten_similar(root_id))

    period = 0

    while len(unsampled) > 0:
        next_id = unsampled.pop(0)
        traversed.append(next_id)

        start = time.time()
        branches = get_ten_similar(next_id)
        delta = time.time() - start

        for b in branches:
            if b not in traversed and b not in unsampled:
                unsampled.append(b)

        if not period:
            period = delta
        else:
            # ema 20
            # geometric average
            period = 1 / (1/period * 0.95 + 1/delta * 0.05)

        print('Similar computation took {:5.2f}s. {:5.2f} per minute.'.format(
            delta, 60/period
        ))


def benchmark():
    post_ids = [
        1402994,  # ~5300 favs
        1267110,  # ~2000 favs
        1453248,  # ~1000 favs
        1509866,  # ~500 favs
        548809,   # ~250 favs
        1419991,  # ~100 favs
        688954,   # ~75 favs
        298119,   # ~50 favs
        964535,   # ~25 favs
        222936    # ~10 favs
    ][::3]
    repeats = 3

    times_by_post = []
    for id in post_ids:
        times_by_repeat = []
        for r in range(repeats):
            start = time.time()
            get_ten_similar(id, 0)
            dt = time.time() - start
            times_by_repeat.append(dt)
        times_by_post.append(times_by_repeat)

    averages = [sum(times)/repeats for times in times_by_post]
    average = sum(averages)/len(post_ids)

    for id, t in zip(post_ids, averages):
        print('id {:7d} took {:5.2f}s on average ({} repeats)'.format(
            id, t, repeats
        ))

    print('Average {:7.4}s'.format(average))

    return average





if __name__ == '__main__':
    user_post_id = int(input('Enter post id: '))
    presample_tree(user_post_id)
    #benchmark()
