VERSION = '0.0.07'

USER_AGENT = 'yre {} (splineclaw)'.format(VERSION)
EXAMPLE_POST_ID = 694758

# rate limiting
PAGE_DELAY = 2  # 30 per minute
REQUEST_DELAY = 0.5  #120 per minute
FAV_REQ_TIMEOUT = 2  # seconds

SAVE_N = 50
SHOW_N = 30

MIN_FAVS = 80
SUBSET_FAVS_PER_POST = 64

DEFAULT_STALE_TIME = 1e7 # seconds

ENABLE_VACUUM = False
