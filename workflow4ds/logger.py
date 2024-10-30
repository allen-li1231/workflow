import logging
import os
import sys

root = logging.getLogger("workflow")
root.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s %(message)s')


def set_log_path(log, path):
    # create file handler and set level to debug
    basenanme = os.path.dirname(path)
    if not os.path.exists(basenanme):
        os.mkdir(basenanme)

    fh = None
    for handler in log.handlers:
        if isinstance(handler, logging.FileHandler):
            fh = handler
            break

    if fh is None:
        fh = logging.FileHandler(path)
        log.addHandler(fh)

    fh.baseFilename = os.path.abspath(path)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)


def setup_stdout_level(logger, level):
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(formatter)

    logger.addHandler(ch)


def set_stream_log_level(log, verbose):
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

log_file = os.path.join(os.getcwd(), "workflow.log")
set_log_path(root, path=log_file)
