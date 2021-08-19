from secrets import *

VERSION = '0.0.09'

USER_AGENT = 'yre {} ({})'.format(VERSION, API_USER)
EXAMPLE_POST_ID = 1802000

# rate limiting
PAGE_DELAY = 2  # 30 per minute
REQUEST_DELAY = 1  # 60 per minute
FAV_REQ_TIMEOUT = 3  # seconds
MAX_CONCURRENT_STATIC = 5 # concurrent requests for static content

MIN_FAVS = 20 # applies to both fetch and resample
SUBSET_FAVS_PER_POST = 256
BRANCH_FAVS_MIN = 5
BRANCH_FAVS_COEFF = 1 # only this fraction of top posts by branch favs will be analysed
BRANCH_FAVS_MAX = 1000 # ... or this number, whichever is lesser
SYM_SIM_MODE = 'add_sim' # 'add_sim' or 'mult_sim'. see analysis.sym_sims for details.
SIM_PER_POST = 25 # store n similars per post
SIMS_SHOWN = 10 # show n similars per post
PRE_DOWNLOAD = False # download SIM_PER_POST posts during presampling

POST_FAV_STALE_TIME = 2.6e6 # seconds
USER_FAV_STALE_TIME = 2.6e6 # seconds
DEFAULT_STALE_TIME = 10**7 # seconds