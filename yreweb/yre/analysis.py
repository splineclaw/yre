from .database import Database
from .utilities import *
import time



def get_ten_similar(source_id, stale_time=10**6):
    '''
    Returns a list of the 10 most similar posts to the source.
    Uses database to cache results.

    Default stale time is 10**6 seconds, or 11.6 days.
    '''

    compute_print = False # show table of statistics?

    print('Getting top ten similar for', source_id)

    # check if in db
    db = Database()
    db.c.execute('''select * from similar where source_id = ?''',
                 (source_id,))
    results = db.c.fetchall()

    if len(results) == 0:
        # not yet in db. let's add it!
        print('Not in database. Fetching...')
        top_ten = compute_similar(source_id, print_enabled=compute_print)

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
            top_ten = compute_similar(source_id, print_enabled=compute_print)

    return top_ten


def compute_similar(source_id, print_enabled=False):
    '''
    computes top 10 similar to the source, saves it to the database,
    and returns their ids as a list
    '''

    min_branch_favs = 5
    min_post_favs = 10

    print('Finding similar to', source_id)

    db = Database()

    print('Querying...')
    if not db.have_favs_for(source_id):
        # post not in database. let's fetch it and recalculate.
        print('Post favorites not in database, fetching...')
        db.get_favs(source_id)
        print('Fetch success. Querying...')

    results = db.get_branch_favs(source_id)

    source_favs = max([r[1] for r in results])

    print('Computing...')

    posts = []
    for r in results:
        if r[0] == source_id or r[1] < min_branch_favs or r[2] < min_post_favs:
            # exclude the source and posts with insufficient favs
            continue
        # branch_favs / post_favs
        # the fraction of target favoriters who are also source favoriters
        relevance = r[1]/r[2]

        # branch_favs / source_favs
        # the fraction of source favoriters who are also target favoriters
        popularity = r[1] / source_favs

        product = relevance * popularity

        # id, branch_favs, post_favs, popularity, relevance, product
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

    db.write_similar_row(source_id, time.time(), top_ten_ids)

    return top_ten_ids


if __name__ == '__main__':
    print(get_ten_similar(1484432))
