from database import Database

source_id = 1484432
min_branch_favs = 5
min_post_favs = 10

print('Finding similar to', source_id)

db = Database()

results = db.get_branch_favs(source_id)
if not results:
    # post not in database. let's fetch it and recalculate.
    print('Post not in database, fetching...')
    db.get_favs(source_id)
    print('Fetch success.')
    results = db.get_branch_favs(source_id)

source_favs = max([r[1] for r in results])

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

    # relevance * popularity
    product = relevance * popularity

    # id, branch_favs, post_favs, popularity, relevance, product
    posts.append((*r, popularity, relevance, product))

sorts = [
         [x for x in sorted(posts,
                            key=lambda x: (x[sort_i]),
                            reverse=True)][:10]
         for sort_i in [3, 4, 5]  # sort by popularity, relevance, product
        ]

for i, sorted in enumerate(sorts):
    print('\n'+'-'*99)
    print('SORTED BY {}'.format(
                                ['POPULARITY', 'RELEVANCE', 'PRODUCT'][i]
                                ).center(99))
    for r in sorted:
        print(
               ('id:{:7d}  common favs:{:4d}  total favs:{:4d}  ' +
                'popularity:{:.4f}  relevance:{:.4f}  product:{:.4f}'
                ).format(*r)
              )
