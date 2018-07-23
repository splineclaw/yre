VERSION = '0.0.05'

USER_AGENT = 'yre {} (splineclaw)'.format(VERSION)

# rate limiting
PAGE_DELAY = 2  # 30 per minute
REQUEST_DELAY = 0.5  #120 per minute

MIN_FAVS = 100

DEFAULT_STALE_TIME = 10**7 # seconds
