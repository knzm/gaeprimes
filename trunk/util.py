from itertools import repeat, chain


class TooManyRetry(Exception):
    pass


def retry(limit=None, raise_if_failed=False):
    if limit:
        if raise_if_failed:
            class raise_when_iter(object):
                def __iter__(self):
                    return self
                def next(self):
                    raise TooManyRetry
            return chain(repeat(True, limit), raise_when_iter())
        else:
            return chain(repeat(True, limit), repeat(False))
    else:
        return repeat(True)
