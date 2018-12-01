VERSION = '0.0.07'

USER_AGENT = 'yre {} (splineclaw)'.format(VERSION)
EXAMPLE_POST_ID = 912182

# rate limiting
PAGE_DELAY = 2  # 30 per minute
REQUEST_DELAY = 0.5  # 120 per minute
FAV_REQ_TIMEOUT = 2  # seconds

MIN_FAVS = 25
SUBSET_FAVS_PER_POST = 256
BRANCH_FAVS_MIN = 5
BRANCH_FAVS_COEFF = 1 # only this fraction of top posts by branch favs will be analysed
BRANCH_FAVS_MAX = 1000 # ... or this number, whichever is lesser
SYM_SIM_MODE = 'add_sim' # 'add_sim' or 'mult_sim'. see analysis.sym_sims for details.
SIM_PER_POST = 100 # store n similars per post
SIMS_SHOWN = 10 # show n similars per post
PRE_DOWNLOAD = True # download posts during presampling

DEFAULT_STALE_TIME = 10**7 # seconds
