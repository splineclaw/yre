from database import Database

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

plt.figure(num=None, figsize=(14,5), dpi=300, facecolor='w', edgecolor='k')



db = Database()

max_favs = 1000

# index corresponds to fav_count
post_counts = []
post_favs = []

for f in range(max_favs + 1):
    count = db.get_favcount_stats(f)
    favs = count * f
    print('{} posts with {} total favs for favcount {}'.format(count, favs, f))
    post_counts.append(count)
    post_favs.append(favs)

# cumulative counts (owo)
def accumulate(in_list):
    cum = [in_list[0]]
    for i in in_list[1:]:
        cum.append(i + cum[-1])
    return cum

post_counts_cum = accumulate(post_counts)
post_favs_cum = accumulate(post_favs)

plt.grid(True)
plt.plot(post_favs_cum)
plt.ylabel('cumulative favs')
plt.xlabel('post favcount')
plt.savefig('graphs/cum_favs.png')

plt.clf()
plt.grid(True)
plt.plot(post_favs)
plt.ylabel('favs by favcount')
plt.xlabel('post favcount')
plt.savefig('graphs/favs.png')

plt.clf()
plt.grid(True)
plt.plot(post_counts_cum)
plt.ylabel('cumulative posts')
plt.xlabel('post favcount')
plt.savefig('graphs/cum_posts.png')


plt.clf()
plt.grid(True)
plt.plot(post_counts)
plt.ylabel('posts by favcount')
plt.xlabel('post favcount')
plt.savefig('graphs/posts.png')
