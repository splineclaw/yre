VERSION = '0.0.06'

USER_AGENT = 'yre {} (splineclaw)'.format(VERSION)
EXAMPLE_POST_ID = 546281

# rate limiting
PAGE_DELAY = 2  # 30 per minute
REQUEST_DELAY = 0.5  # 120 per minute
FAV_REQ_TIMEOUT = 2  # seconds

MIN_FAVS = 200
SUBSET_FAVS_PER_POST = 256
BRANCH_FAVS_MIN = 5
BRANCH_FAVS_COEFF = 0.2 # only top n posts by branch favs will be analysed
BRANCH_FAVS_MAX = 2000 # ... or this number, whichever is lesser

DEFAULT_STALE_TIME = 10**7 # seconds
