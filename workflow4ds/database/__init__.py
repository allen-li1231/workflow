from importlib.util import find_spec

__all__ = []

# only allow user to use Oracle class if cx_Oracle is installed
if find_spec("cx_Oracle"):
    from .oracle import Oracle
    __all__.append("Oracle")

# only allow user to use Oracle class if cx_Oracle is installed
if find_spec("adbc_driver_manager") and find_spec("adbc_driver_flightsql"):
    from .doris import Doris
    __all__.append("Doris")