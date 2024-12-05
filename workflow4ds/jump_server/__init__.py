import importlib.util

__all__ = []

# only allow user to use SSH tunnel if paramiko is installed
if importlib.util.find_spec("paramiko"):
    from .tunnels import SSH, SFTP
    __all__.extend(["SSH", "SFTP"])
