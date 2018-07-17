def seconds_to_dhms(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)

    return '%03dd %02dh:%02dm:%6.3fs' % (d, h, m, s)
