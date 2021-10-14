import re
import logging
from typing import List

from numba import jit


@jit(nopython=True)
def average_2(a, b):
    return (a + b) / 2.0

@jit(nopython=True)
def weighted_average_2(a, b):
    return a * 0.4 + b * 0.6

@jit(nopython=True)
def average_3(a, b, c):
    return (a + b + c) / 3.0

def text_clean(text: str) -> str:
    text = re.sub("[^A-Za-z가-힣]", "", text).lower().strip()
    return text

def chunks(lst: List, n: int):
    if n == 0:
        return lst
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

def merge_dict(a, b, path=None):
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dict(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass
            else:
                raise Exception("Conflict at %s" % ".".join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a

def get_logger(level = logging.INFO):
    LOG_FORMAT = "[%(asctime)-10s] (line: %(lineno)d) %(levelname)s - %(message)s"
    logging.basicConfig(format=LOG_FORMAT)
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    return logger