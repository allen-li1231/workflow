"""
@Author: Allen Li 7/6/2021
"""
import sys
from typing import Iterable
import argparse
import re
import json
import subprocess
import psutil
import requests

import numpy as np
import pandas as pd

KERNEL_REGEX = re.compile(r".+kernel-(.+)\.json")
NBSERVER_REGEX = re.compile(r"nbserver-\d+?\.json")
NOTEBOOK_REGEX = re.compile(r"(https?://([^:/]+):?(\d+)?)/?(\?token=([a-z0-9]+))?")

ipython_vars = ['In', 'Out', 'exit', 'quit', 'get_ipython', 'ipython_vars']


def get_kernel_proc_info():
    pids = psutil.pids()

    # memory info from psutil.Process
    df_mem = []

    for pid in pids:
        try:
            proc = psutil.Process(pid)
            cmd = " ".join(proc.cmdline())
        except psutil.NoSuchProcess:
            continue

        if len(cmd) > 0 and ("jupyter" in cmd or "Jupyter" in cmd or "ipython" in cmd) and "kernel" in cmd:
            # kernel
            kernel_ID = re.sub(KERNEL_REGEX, r"\1", cmd)

            # memory
            mem = proc.memory_info()[0]

            uname = proc.username()

            # user, pid, memory, kernel_ID
            df_mem.append([uname, pid, mem, kernel_ID])

    df_mem = pd.DataFrame(df_mem, columns=["user", "pid", "memory", "kernel_ID"])
    df_mem.sort_values("memory", ascending=False, inplace=True)

    return df_mem


def get_notebook_server():
    lst_nbserver = []

    for n in subprocess.Popen(
            ["jupyter", "notebook", "list"], stdout=subprocess.PIPE
            ).stdout.readlines()[1:]:
        match = re.match(NOTEBOOK_REGEX, n.decode())
        if match:
            base_url, host, port, _, token = match.groups()
            lst_nbserver.append({"base_url": base_url, "token": token})
        else:
            print("Unknown format: {}".format(n.decode()))

    # provide an alternative, jupyter notebook list could return empty if using ssl tunnel
    if len(lst_nbserver) == 0:
        runtime_dir = subprocess.Popen(
            ["jupyter", "--runtime-dir"], stdout=subprocess.PIPE
            ).stdout.read().decode().strip("\n")

        res = subprocess.Popen(
            ["ls", runtime_dir], stdout=subprocess.PIPE
            ).stdout.read().decode()
        lst_nbserver_json = re.findall(NBSERVER_REGEX, res)

        for nbserver_json in lst_nbserver_json:
            try:
                with open(f"{runtime_dir}/{nbserver_json}", "r") as f:
                    nbserver = json.load(f)
                    lst_nbserver.append({"base_url": nbserver["url"].strip("/"),
                                         "token": nbserver["token"]})
            except (json.JSONDecodeError, FileNotFoundError):
                pass

    return lst_nbserver


def get_notebook_session_info(jupyter_password=None):
    """
    get currently running notebook session info

    :param jupyter_password: optional, pass jupyter password if necessary, default to None
    :return: DataFrame
    """

    lst_nb = []
    df_nb = pd.DataFrame(columns=["kernel_ID",
                                  "kernel_name",
                                  "kernel_state",
                                  "kernel_connections",
                                  "notebook_path"])
    kernels = []

    lst_running_server = get_notebook_server()

    if len(lst_running_server) == 0:
        return df_nb

    for server in lst_running_server:
        s = requests.Session()
        if server["token"] is not None:
            s.get(server["base_url"] + "/?token=" + server["token"])
        else:
            # do a get to the base url to get the session cookies
            s.get(server["base_url"])
        if jupyter_password is not None:
            # Seems jupyter auth process has changed, need to first get a cookie,
            # then add that cookie to the data being sent over with the password
            data = {"password": jupyter_password}
            data.update(s.cookies)
            s.post(server["base_url"] + "/login", data=data)

        res = s.get(server["base_url"] + "/api/sessions")

        if res.status_code != 200:
            raise Exception(res.text)

        for sess in res.json():
            kernel_ID = sess["kernel"]["id"]
            if kernel_ID not in kernels:
                kernel = {
                    "notebook_path": sess["path"],
                    "kernel_ID": kernel_ID,
                    "kernel_name": sess["kernel"]["name"],
                    "kernel_state": sess["kernel"]["execution_state"],
                    "kernel_conn": sess["kernel"]["connections"],
                    # "notebook_url": notebook["base_url"] + "/notebook/" + sess["id"],
                    }
                kernel.update(server)
                lst_nb.append(kernel)
                kernels.append(kernel_ID)

    if len(lst_nb) > 0:
        df_nb = pd.DataFrame(lst_nb)

    return df_nb


def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']:
        if size < 1024. or unit == 'PiB':
            break
        size /= 1024.

    return f"{size:.{decimal_places}f} {unit}"


def _apply_usage(obj):
    if isinstance(obj, pd.DataFrame):
        return obj.memory_usage(deep=True).sum() + sys.getsizeof(obj)

    if isinstance(obj, pd.Series):
        return obj.memory_usage(deep=True) + sys.getsizeof(obj)

    if isinstance(obj, np.ndarray):
        return obj.data.nbytes + sys.getsizeof(obj)

    if isinstance(obj, Iterable):
        level_size = 0.
        for item in obj:
            level_size += _apply_usage(item)

        return level_size + sys.getsizeof(obj)

    else:
        return sys.getsizeof(obj)


def get_variable_mem_usage(scope, human_readable=True):
    """
    get variables' memory usage under specific variable scope

    :param scope: pass globals() to this parameter to iterate over variables in current global scope
    :param human_readable: optional, whether to show human readable memory size, default to True
    :return: DataFrame
    """

    lst_vars = [x for x in scope
                if not x.startswith('_')
                and x not in sys.modules
                and x not in ipython_vars]
    lst_objects = [scope.get(x) for x in lst_vars]
    lst_memory_use = list(map(_apply_usage, lst_objects))

    # Get a sorted list of the objects and their sizes
    df_var_mem_usage = pd.DataFrame({"name": lst_vars,
                                     "memory": lst_memory_use})
    df_var_mem_usage.sort_values("memory", ascending=False, inplace=True)
    df_var_mem_usage.set_index("name", inplace=True)

    if human_readable:
        df_var_mem_usage.loc[:, "memory"] = df_var_mem_usage["memory"].apply(human_readable_size)

    return df_var_mem_usage


def get_kernel_mem_usage(jupyter_password=None, human_readable=True, print_ascii=False):
    df_mem = get_kernel_proc_info()
    df_nb = get_notebook_session_info(jupyter_password)

    # joining tables
    df = pd.merge(df_nb, df_mem, on=["kernel_ID"], how="inner")
    df.sort_values("memory", inplace=True)

    if human_readable:
        df.loc[:, "memory"] = df["memory"].apply(human_readable_size)

    if print_ascii:
        import tabulate
        print(tabulate.tabulate(df, headers=(df.columns.tolist()), showindex=False))

    df.set_index("kernel_ID", inplace=True)
    return df


def main():
    parser = argparse.ArgumentParser(description="Retrieve jupyter notebook memory usage.")
    parser.add_argument("-p", "--password", nargs="?", help="Jupyter password (if pass-protected)")
    args = vars(parser.parse_args())

    get_kernel_mem_usage(args["password"], print_ascii=True)
    return 0


if __name__ == "__main__":
    main()
