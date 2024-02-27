import importlib.util

__all__ = []

# only allow user to use Oracle class if cx_Oracle is installed
if importlib.util.find_spec("impala"):
    from .api import HiveClient
    __all__ = ["HiveClient"]
