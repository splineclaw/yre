try:
    from .database import Database
    from .utilities import *
    from . import constants
    from . import images

except ModuleNotFoundError:
    from database import Database
    from utilities import *
    import constants
    import images

import time
import random
import math
import sys
import itertools
from operator import itemgetter



def get_n_similar(source_id,
                    stale_time=constants.DEFAULT_STALE_TIME,
                    from_full=False):
    '''
    Returns a list of the most similar posts to the source.
    Uses database to cache results.
    '''

    compute_print = True  # show table of statistics?

    print('Getting top similar for', source_id)

    # check if in db
    db = Database()
    results = db.select_similars(source_id)

    if len(results) < constants.SIM_PER_POST:
        # not yet in db. let's add it!
        print('Not in database ({}/{} expected found). Fetching...'.format(
            len(results), constants.SIM_PER_POST
        ))
        top_n = compute_similar(source_id,
                                  from_full=from_full,
                                  print_enabled=compute_print)

    else:
        print('Found in database.')
        result = [x[2] for x in results]
        last_time = min(x[1] for x in results)
        top_n = result[-constants.SIM_PER_POST:]

        age = time.time() - last_time

        if age > stale_time:
            # this hasn't been updated in a while.
            print('Cache stale ({} old, threshhold {}). Fetching...'.format(
                seconds_to_dhms(age), seconds_to_dhms(stale_time)
            ))
            top_n = compute_similar(source_id,
                                      from_full=from_full,
                                      print_enabled=compute_print)

    return top_n

def compute_similar(source_id, from_full=False, print_enabled=False):
    '''
    computes top similar to the source, saves it to the database,
    and returns their ids as a list
    '''

    min_branch_favs = constants.BRANCH_FAVS_MIN
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
    # slow
    branch_time = time.time()
    results = db.get_branch_favs(source_id)
    branch_time = time.time() - branch_time

    if not results:
        return None

    source_favs = max([r[1] for r in results])

    print('Computing... {} candidates.'.format(len(results)))

    bs = []
    selected = 0
    newresults = []
    for r in results:
        if r[0] == source_id or r[1] < min_branch_favs or r[2] < min_post_favs:
            # exclude the source and posts with insufficient favs
            continue
        newresults.append(r)
    print(len(newresults),'/',len(results),'selected ({}%)'.format(
        len(newresults)/len(results)*100
    ))
    results = sorted(newresults, key=itemgetter(1), reverse=True)
    rq = len(results)
    slicept = min(int(rq*constants.BRANCH_FAVS_COEFF), constants.BRANCH_FAVS_MAX)
    results = results[:slicept]

    print(len(results), 'results. Computing similarities.')
    # slow

    bs = []
    for r in results:
        selected += 1
        bs.append(r[0])

    a = source_id
    sym_time = time.time()
    for b in bs:
        db.calc_and_put_sym_sim(a,b)
    sym_time = time.time() - sym_time

    print('Sorting... {} selected.'.format(selected))

    top_n = db.select_n_similar(source_id, constants.SIM_PER_POST)

    if print_enabled:
        linewidth = 99
        print('\n'+'-'*linewidth)
        print('SORTED BY SIMILARITY')
        for r in top_n:
            print(
                   ('id low:{:7d}  id high:{:7d}  common favs:{:4d}  ' +
                    'add:{:.4f}  mult:{:.4f}  '
                    ).format(*r)
                  )
    top_n_ids = []
    print(branch_time,'s for branching,',sym_time,'s for',slicept,'posts')
    low_high = list(zip([p[0] for p in top_n],[p[1] for p in top_n]))

    for x,y in low_high:
        if x != source_id:
            top_n_ids.append(x)
        else:
            top_n_ids.append(y)


    for j, currentid in enumerate(top_n_ids):
        #find index
        for i,x in enumerate(results):
            if x[0] == currentid:
                print('result {} in location {}/{}'.format(
                    j+1, i+1, slicept
                ))

    if top_n_ids:
        # if there are not enough similar posts, fill with zeros
        top_n_ids = (top_n_ids + [0]*constants.SIM_PER_POST)[:constants.SIM_PER_POST]

        db.write_similar_row(source_id, time.time(), top_n_ids)

        return top_n_ids
    else:
        return None


def presample_randomly():
    db = Database()

    print('Searching for posts needing similars computed...')
    need_update = db.find_similar_need_update()
    random.shuffle(need_update)

    period = -1

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


def presample_tree(root_id, download='True'):
    db = Database()

    traversed_ids = [root_id]

    # each element is (depth, priority, id)
    unsampled_posts = [[1, 1, id] for id in get_n_similar(root_id)]

    period = -1
    new_count = 0

    while len(unsampled_posts) > 0:
        # iterate, ordered by smallest depth first,
        # and highest popularity in conflict.
        # (popularity is the number of known parents a post has.)

        unsampled_depths = [a[0] for a in unsampled_posts]
        unsampled_popularities = [a[1] for a in unsampled_posts]
        unsampled_ids = [a[2] for a in unsampled_posts]

        min_depth = min(unsampled_depths)
        depth_candidates = []
        for p in unsampled_posts:
            depth, priority, id = p
            if depth == min_depth:
                depth_candidates.append(p)

        max_priority = max([a[1] for a in depth_candidates])
        priority_candidates = []
        for p in depth_candidates:
            depth, priority, id = p
            if priority == max_priority:
                priority_candidates.append(p)

        next_post = priority_candidates[
            random.randrange(len(priority_candidates))
            ]

        next_depth, next_priority, next_id = next_post
        if download:
            images.image_with_delay(next_id)
        traversed_ids.append(next_id)
        print('Selected post {}. Depth {}, priority {}.'.format(
            next_id, next_depth, next_priority
        ))

        start = time.time()
        branch_ids = get_n_similar(next_id)
        delta = time.time() - start

        branch_depth = next_depth + 1

        new = 0
        for b_id in branch_ids:
            if b_id in unsampled_ids and b_id != next_id:
                # known id, so update popularity and depth (if applicable)
                i = unsampled_ids.index(b_id)
                unsampled_posts[i][0] = min(
                    branch_depth, unsampled_posts[i][0])
                unsampled_posts[i][1] += 1
            else:
                # it's new!
                unsampled_posts.append([branch_depth, 1, b_id])
                new += 1
        # now it's safe to remove the post
        unsampled_posts.pop(unsampled_posts.index(next_post))

        if period == -1:
            if delta > 0.05:
                period = delta
        else:
            # ema 20
            # geometric average
            if delta > 0.05:  # omit cache hits
                period = 1 / (1/period * 0.95 + 1/delta * 0.05)
                new_count += 1

        print('Similar computation took {:5.2f}s. {:5.2f} per minute. {} fetched, {} traversed. {} ({} new) in queue.'.format(
            delta, 60/period, new_count, len(traversed_ids), len(unsampled_posts), new
        ))

def sym_sims(a, b, verbose=False):
    '''
    Takes as argument two post ids, a and b.

    Returns a tuple:
        quantity of mutual favorites,
        additive symmetric similarity,
        multiplicative symmetric similarity

    Quantity of mutual favorites is the number of users who have favorited both a and b.

    Let
    m = mutual favorites
    a = quantity of favorites of post a (a favcount)
    b = quantity of favorites of post b (b favcount)

    add_sim = m / (a + b - m)

    mult_sim = m**2 / (a * b)


    In words:
    Additive symmetric similarity is the ratio of the quantity of users who favorited
        both post a and b to the quantity of users who favorited either post a or post b.

    Multiplicative symmetric similarity is the product of the ratio of the quantity of users
        who favorited both post a and b to the total quantity of users who favorited post a
        and the ratio of the quantity of users who favorited both post a and b to the
        total quantity of users who favorited post b.
        [/////     ] X [////////  ] = [///       ]
        a common ratio X b common ratio = multiplicative similarity

    '''
    print('Similarity between {} and {}'.format(a,b))
    db = Database()
    if not db.have_favs_for_id(a):
        db.get_favs(a)
    if not db.have_favs_for_id(b):
        db.get_favs(b)
    overlap = db.get_overlap(a, b)
    a_favs = db.get_favcount(a)
    b_favs = db.get_favcount(b)

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

    return(overlap, add_sim, mult_sim)



def single_benchmark():
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
            get_n_similar(id, 0)
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

def symmetric_benchmark():
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
    ]

    combos = list(itertools.product(post_ids, post_ids))

    times_by_combo = []
    similarities = []
    for combo in combos:
        a, b = combo
        start = time.time()

        similarities.append(sym_sims(a,b))

        dt = time.time() - start
        times_by_combo.append(dt)


    average = sum(times_by_combo)/len(times_by_combo)

    for dt, combo, s in zip(times_by_combo, combos, similarities):
        a, b = combo
        print('ids {}+{} (sim {:5f}) took {:7.4f}s'.format(
            a, b, s[2], dt
        ))

    print('Took {:.4f}s for {} combos: {:8.4f}ms per combo.'.format(
          sum(times_by_combo), len(times_by_combo), average*1000)
         )

    for id in post_ids:
        times = []
        for dt, combo in zip(times_by_combo, combos):
            if id in combo:
                times.append(dt)
        print('Post {} averages {:7.4f}s'.format(id,sum(times)/len(times)))

if __name__ == '__main__':
    args = sys.argv[1:]
    if args:
        if args[0] == 'bench':
            symmetric_benchmark()
            post_id = 0
        else:
            post_id = int(args[0])
    else:
        i = input('Enter post id: ')
        post_id = int(i) if i else None
    if post_id:
        presample_tree(post_id)
    else:
        print('No input provided. Sampling from default post.')
        presample_tree(constants.EXAMPLE_POST_ID)
