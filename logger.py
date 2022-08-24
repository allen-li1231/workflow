import logging
import os
import sys


root = logging.getLogger("workflow")
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


def set_log_level(log, verbose):
    has_stream_handler = False
    for handler in log.handlers:
        if isinstance(handler, logging.StreamHandler):
            has_stream_handler = True
            if verbose:
                handler.setLevel(logging.INFO)
            else:
                handler.setLevel(logging.WARNING)

    if not has_stream_handler:
        if verbose:
            setup_stdout_level(log, logging.INFO)
        else:
            setup_stdout_level(log, logging.WARNING)
