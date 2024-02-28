import importlib.util

__all__ = []

# only allow user to use Oracle class if cx_Oracle is installed
if importlib.util.find_spec("cx_Oracle"):
    from .apps import Oracle

    __all__.append("Oracle")

# only allow user to use SSH tunnel if paramiko is installed
if importlib.util.find_spec("paramiko"):
    from .tunnels import SSH, SFTP

    __all__.extend(["SSH", "SFTP"])
