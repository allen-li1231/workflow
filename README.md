# Workflow: Data Pipeline Integrated for Data Scientists
## Includes Modules:

- [Jupyter API](#Jupyter-API): For those who work with a remote Jupyter server, workflow provides file upload/download, terminal interaction, notebook API implementation and performance visualization for Jupyter.
- [Apache Hive Client API](#Apache-Hive-Client-API): **Stable** Data fetching and parsing to Pandas Dataframe based on modern HiveServer2 for **both Python 2 and 3** as the official python package for HiveServer2 is unstable in the latest python version. Supports concurrent hive sql execution (with a nice progress bar).
- [Apache Hue Notebook API](#Apache-Hue-Notebook-API): For those whose company deploys [Apache Hue](https://gethue.com/), workflow provides data fetching and parsing to Pandas Dataframe from Apache Hue service, supports concurrent sql execution (with progress bar) and hive settings.
- [Apache Zeppelin API](#Apache-Zeppelin-API): implementations on Zeppelin notebook API, supports interaction notebook, upload and download, python file to notebook and vice versa.
- [Oracle SQL Interface](#Oracle-SQL-Interface): Data fetching and parsing to Pandas Dataframe using official Oracle package (i.e. cx_Oracle)
- [Tunnels](#Tunnels): Interactive SSH, FTP and SFTP.
- [Other Useful Tools](#Other-Useful-Tools)

## How to Install

The module includes multiple submodule, you can conveniently install all submodules via:
```sh
pip install "workflow4ds[all]"
```

You can also install one of modules based on your needs.
To install only Hive module:
```sh
pip install "workflow4ds[hive]"
```

To install only Hue module:
```sh
pip install "workflow4ds[hue]"
```

To install only tunnel module:
```sh
pip install "workflow4ds[tunnel]"
```
___

## Jupyter API
``` python
from workflow.jupyter import Jupyter, mem_usage

j = Jupyter()

# upload local file to Jupyter server
j.upload(file_path="LOCAL_FILE_PATH", dst_path="DESTINATION")

# download to local
j.download(file_path="./example.file", dst_path=".")

# memory inspect
mem_usage.get_kernel_mem_usage("PASSWORD_IF_ANY")

# check variables' memory usage under specific scope
mem_usage.get_variable_mem_usage(globals())
```

### Interactive Terminal
``` python
# get a list of currently opened terminals
j.get_terminals()

# or initialize a terminal
terminal_name = j.new_terminal()

# connect to a terminal
conn = j.connect_terminal(terminal_name)

# execute a command, the results will be printed to sys.stdout
conn.execute("ls -l")

# stop the terminal
j.close_terminal(terminal_name)
```
___

## Apache Hive Client API
> :bell:To use the client, one must first install [impyla](https://github.com/cloudera/impyla) via pip,
otherwise the following example will not work.
``` python
from workflow.hive import HiveClient
import pandas as pd

# Explicitly provide hiveServer
# Default hiveServer IP and port settings can be set manually in ./settings.py
hive = HiveClient(auth={
    "host": "127.0.0.1",
    "port": "2020",
    "user": "admin",
    "password": ""})

# automatically retrieve data and parse to pandas dataframe
# **Warning：`;` is not allowed in sql**
df = hive.run_hql("show databases")
df.head()

# concurrent hql execution, progressbar is shown by default
lst_results = hive.run_hqls(["show databases", "show tables"], progressbar=True)
print(lst_results[0])   # data from sql "show databases"
print(lst_results[1])   # data from sql "show tables"

# execute sql file
# concurrency can be toggled with submission
lst_results = hive.run_hql_file("PATH-TO-SQL.sql", concurrent=True)
# or, submit sql without tracking the results（non-blocking）
hive.run_hql_file("PATH-TO-SQL.sql", sync=False)
```
___

## Apache Hue Notebook API
``` python
import workflow
import pandas as pd

# login by instantiate hue object
HUE = workflow.hue("USERNAME", "PASSWORD")

# or, provide username only and type password in prompt
HUE = workflow.hue("USERNAME")

# run sql and parse into dataframe
result = HUE.run_sql("select 1;")
df = pd.DataFrame(**result.fetchall())
print(df.head())

# get data from a existing table
df = HUE.get_table("table_name")
print(df.head())

# upload dataframe as a table
HUE.upload(df, table_name=table_name)

# insert dataframe to a table, if not exists, will create the table
HUE.insert_data(df, table_name=table_name)

# kill yarn application
HUE.kill_app("yarn_application_id")

# stop connection with Hue 
HUE.stop()
```

## Apache Zeppelin API
``` python
from workflow.zeppelin import Zeppelin


### get Zeppelin instance
z = Zeppelin(USERNAME, PASSWORD)
# get visible notes
print(z.list_notes())

# get note instance
note = z.get_note("note_path/note_name")

```
### Use Note Instance
``` python
# save as python script (currently only support python)
note.export_py("python_script_path")
# 删除note
z.delete_note("note_path/note_name")

# convert python script to Zeppelin note and upload to Zeppelin server
# automatically detect interpreter name in comment
# for example if the interpreter name is spark, the corresponding comment will be #%spark
new_note = z.import_py(
    data="python file path (ends with .py) or python code string",
    note_name="path/new_note_name"
    # default interpreter can be set in settings.py
    interpreter="spark"
)

# run all
note.run_all()
# stop all
note.stop_all()
# get all paragraph execution status
result = note.get_all_status()
# clear results
note.clear_all_results()
# set note permission
note.set_permission(
    readers=["your_username"],
    owners=["your_username"],
    runners=["your_username"],
    writers=["your_username"]
)
# delete itself from server
note.delete()
```

### Use Paragraph in a Note
``` python
# get paragraph by index
p = note.get_paragraph_by_index(6)
# iterate over paragraphs
for p in note.iter_paragraphs():
    print(p.text)
# get all paragraph objects in a note
lst_paragraph = note.get_all_paragraphs()
# generate a new paragraph
# paragraph index: 0 being the first and -1 being the last
# append to last by default
p = note.create_paragraph("CONTEXT", index=0)


# get paragraph context
print(p.text)
# revise the text
p.text = "import pandas as pd"
# move the paragraph
p.move_to_index(0)

# execute the paragraph code
p.run()
# stop execution
p.stop()
# check execution status
print(p.status)
# read execution result
print(p.results)
# get execution job name
print(p.job_name)
# get execution job finish time
print(p.date_finished)

# delete paragraph
p.delete()
```
> more api can be found in [zeppelin.\__init__](zeppelin/__init__.py)
___

### Oracle SQL Interface
> :bell:To use the interface, one must first manually install [cx_Oracle](https://oracle.github.io/python-cx_Oracle/),
otherwise the following example will not work.
```python
from workflow.jump_server import Oracle


# default server name, hostname can be set in settings.py
o = Oracle(username, password, service_name, hostname
)

# execute sql or procedure
o.execute("select 1")
o.execute_proc("procedure")

# pass to dataframe
df = pd.DataFrame(**o.fetchall())
print(df)
```
___

### Tunnels
> :bell:To use the interface, one must first install [paramiko](https://www.paramiko.org/) via pip,
otherwise the following example will not work.
#### Interactive SSH Tunnel
```python
from workflow.jump_server import SSH


# default server name, hostname can be set in settings.py
ssh = SSH(username, password, host=host, port=port)

# execute whatever command via tunnel
ssh.execute("ls -al")
# the result will be printed to sys.stdout

# explicitly get result
print(ssh.msg)

# close tunnel
ssh.close()
```
#### Interactive FTP/SFTP Tunnel
```python
from workflow.jump_server import SFTP


# default server name, hostname can be set in settings.py
sftp = SFTP(username, password, host=host, port=port)

# sftp put file
sftp.put("local_file_path", "remote_path")

# sftp get file
sftp.get("remote_file_path", "local_path")

# close tunnel
ssh.close()
```
---
### Other Useful Tools
> :bell:To use the interface, one must first install [openpyxl](https://openpyxl.readthedocs.io/en/stable/)
via pip, otherwise the following example will not work.
```python
from workflow import utils


# sometimes pandas dataframe is not smart enough to determine
# the data type and causes memory overhead, we can reduce large 
# dataframe memory usage manually by:
df_reduced = utils.reduce_mem_usage(df)

# read a large file in chunks, reduces memory overhead:
with open(file, "r") as f:
    for i, chunk in utils.read_file_in_chunks(f):
        print(chunk)

# append a dataframe to existing csv file
utils.append_df_to_csv(filename, df)

# append a dataframe to existing excel file
utils.append_df_to_excel(filename, df, sheet_name='Sheet1')
```