import logging
import os
import sys


root = logging.getLogger()
root.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s %(message)s')

# create file and console handler and set level to debug
fh = logging.FileHandler(os.path.join(os.getcwd(), 'log'))
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

root.addHandler(fh)


def setup_stdout_level(logger, level):
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(formatter)

    logger.addHandler(ch)
