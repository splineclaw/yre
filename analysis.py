from database import Database

target_id = 1484432
min_branch_favs = 5
min_post_favs = 10

print('Finding similar to', target_id)

db = Database()

results = db.get_branch_favs(target_id)

# prune low favorite entries
pruned_results = []
for r in results:
    if r[1] >= min_branch_favs and r[2] >= min_post_favs:
        pruned_results.append(r)
results = pruned_results

top_ten_popular = [r[0] for r in results[:11]]
top_ten_popular.remove(target_id)
print(top_ten_popular)

relevances = [r[1]/r[2] for r in results]  # branch_favs / post_favs

relevant = [
             # set to None if it's the target_id so we can remove it
             (x[0], a, x[1], x[2]) if x[0] != target_id else None
             # sort according to relevance, then by favorites
             for a, x in sorted(zip(relevances, results),
                                key=lambda x: (x[0], x[1][1]),
                                reverse=True)
             ]
relevant.remove(None)

for r in relevant[:10]:
    print('id:{}\t relevance:{:.4f}\t popularity:{}\t favs:{}'.format(
        *r
    ))
