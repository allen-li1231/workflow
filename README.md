# Workflow: Data Pipeline Integration for Data Scientists
## Includes Modules:

- [Jupyter API](#Jupyter API): File upload/download, terminal interaction, notebook API implementation and performance visualization for Jupyter.
- [Apache Hive Client API](#Apache Hive Client API): Data fetching and parsing to Pandas Dataframe based on modern HiveServer2 for **both Python 2 and 3**, supports concurrent hive sql execution (with progress bar).
- [Apache Hue Notebook API](#使用huenotebook调用hue-notebook-api): Data fetching and parsing to Pandas Dataframe from Apache Hue, supports concurrent sql execution (with progress bar) and hive settings.
- [Apache Zeppelin API](#使用zeppelin模块): implementations on Zeppelin notebook API, supports interaction notebook, upload and download, python file to notebook and vice versa.
- [Oracle SQL/PLSQL Interface]: Data fetching and parsing to Pandas Dataframe using official Oracle package (i.e. cx_Oracle)
- [Tunnels]: Interactive SSH, FTP and SFTP.


## Apache Hive Client API
``` python
from workflow.hive import HiveClient
import pandas as pd

# Explicitly provide hiveServer
# Default hiveServer IP and port settings can be set manually in ./settings.py
hive = HiveClient(auth={
    "host": "127.0.0.1",
    "port": "2020",
    "user": "admin",
    "password": "admin"})

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

## 使用hue模块运行sql，拉取结果
``` python
import workflow
import pandas as pd

# 可直接提供密码（不推荐，他人pull你的repo的时候会看到你的明文密码）
HUE = workflow.hue("USERNAME", "PASSWORD")

# 仅提供username，会在程序运行的shell提示输入密码（推荐）
HUE = workflow.hue("USERNAME")

result = HUE.run_sql("select 1;")
### 交互Pandas API
df = pd.DataFrame(**result.fetchall())
df.head()
```
## 使用上传下载功能
``` python
# 使用下载平台
data = HUE.download(table_name="TABLE_NAME",
                    reason="YOUR_REASON",
                    decrypt_columns=COL_TO_DECODE)

# 或直接保存csv到本地
result = HUE.run_sql("select 1;")
result.to_csv("PATH_TO_CSV_FILE")

# 根据提示的文件名后缀确定下载格式，更多选项可查阅代码文档
HUE.download(table_name="TABLE_NAME",
             reason="YOUR_REASON",
             decrypt_columns=LST_COLS_TO_DECODE,
             path="SAVE_FILE_PATH")

# 上传本地文件到Hue
HUE.upload(data="UPLOAD_FILE_PATH", # 可以是文件路径，也可以是Pandas DataFrame or Series
           reason="YOUR_REASON",
           encrypt_columns=LST_COLS_TO_ENCODE)

# 批量下载
HUE.batch_download(tables=["table1", "table2", "table3"],
                   reasons="YOUR_REASON",
                   decrypt_columns=[["col1". "col2"], None, ["col3"]])

# 下载平台的kill Yarn application 功能
# HUE.run_sql返回的NotebookResult，自带解析任务app id的功能
result = HUE.run_sql("select count(*) from dwf.merchant;", sync=False)
HUE.kill_app(result.app_id)
```
> HUE的接口实现主要使用workflow.hue.hue_sys，其本质上是实例化的Notebook类
> workflow.hue.Notebook提供了更为丰富的接口，可以使用上面的HUE.hue_sys获得notebook实例，也可以对Notebook显式地调用，参考如下：

### 使用hue.Notebook调用hue notebook api
``` python

# 使用方法1
with Notebook("USERNAME",
              "PASSWORD",   # 不推荐明文密码，建议仅提供username，然后程序运行时在命令行输入密码
              name="Notebook name",
              description="Notebook description",
              verbose=True)\
        as notebook:

    res = notebook.execute("select 1;")
    data = res.fetchall()


# 使用方法2
notebook = Notebook(name="Notebook name",
                    description="Notebook description",
                    verbose=True)

with notebook.login("USERNAME"):
    res = notebook.execute("select 1;")
    data = res.fetchall()


# 使用方法3
notebook = Notebook(name="Notebook name",
                    description="Notebook description")
notebook.login("USERNAME")
res = notebook.execute("SET hive.execution.engine;")
data = res.fetchall()

# 按需手动释放notebook和session资源
notebook.close()

# 登出Hue
notebook.logout()
```

### Hive设置
``` python
# 设置任务优先级
notebook.set_priority("high")

# 设置Hive execute engine
notebook.set_engine("tez")  # mr/tez/spark

# 设置内存资源的使用
notebook.set_memory_multiplier(1.2)

# 设置更多额外参数
notebook.set_hive("hive.queue.name", "fengkong")

# 反设置
del notebook.hive_settings["hive.queue.name"]
notebook._set_hive(notebook.hive_settings)

# 重置设置
notebook._set_hive(None)
```
> 关于默认Hive设置，请查阅workflow.settings.HIVE_PERFORMANCE_SETTINGS

### Tip: 手动多开notebook，并行多条sql
``` python
import time
from workflow.hue import Notebook

notebook = Notebook(name="Notebook name",
                    description="Notebook description",
                    hive_settings={}, # 可手动取消执行加速指令
                    verbose=True)
notebook.login("USER_NAME")

d_notebook = {}
for sql in LST_SQLS:
    # 打开新的notebook
    notebook = notebook.new_notebook(name=sql_file, verbose=True)
    # 异步执行sql
    result = notebook.execute(sql, sync=False)
    d_notebook[notebook] = False

# notebookResult.is_ready会检查sql执行结果是否准备完毕
while not all(is_ready for is_ready in d_notebook.values()):
    for notebook, is_ready in d_notebook.items():
        if is_ready:
            continue

        if notebook._result.is_ready():
            # 对结果进行fetchall()等操作后...
            
            notebook.close()
            d_notebook[notebook] = True

    time.sleep(5)
```
___

## 使用Jupyter模块
``` python
from workflow.jupyter import Jupyter

j = Jupyter()

# 上传本地文件到jupyter文件系统
j.upload(file_path="./utils.py", dst_path="lizhonghao")

# 下载jupyter文件到本地
j.download(file_path="lizhonghao/utils.py", dst_path=".")
```

### 连接Jupyter shell并运行shell指令
``` python
# 获取当前开启的terminal
j.get_terminals()

# 打开新的terminal
terminal_name = j.new_terminal()

# 创建websocket连接terminal
conn = j.connect_terminal(terminal_name)

# 发送command，默认会将结果输出到console
conn.execute("ls -l")

# 关闭terminal
j.close_terminal(terminal_name)
```
___

## 使用Zeppelin模块
``` python
from workflow.zeppelin import Zeppelin


### Zeppelin
z = Zeppelin(USERNAME, PASSWORD)
# 获得note列表
print(z.list_notes())

# 获得note实例
note = z.get_note("note_path/note_name")
# 删除note
z.delete_note("note_path/note_name")

```
### 使用Note
``` python
# 保存为python脚本
note.export_py("python_script_path")

# 导入python脚本, 并上传至Zeppelin
# 会自动识别comment中的interpreter, 并按此分割paragraphs
new_note = z.import_py(
    data="python file path (ends with .py) or python code string",
    note_name="path/new_note_name"
    # 默认 interpreter为zf_fk_spark.pyspark, 选填
    interpreter="zf_fk_spark.pyspark"
)

# 运行整个note
note.run_all()
# 停止运行
note.stop_all()
# 获取note跑批任务状态
result = note.get_all_status()
# 清除返回结果
note.clear_all_results()
# 设置note权限, 默认所有人
note.set_permission(
    readers=["your_username"],
    owners=["your_username"],
    runners=["your_username"],
    writers=["your_username"]
)
# 删除note
note.delete()
```

### 使用Paragraph
``` python
# 获取某个paragraph
p = note.get_paragraph_by_index(6)
# 遍历paragraphs
for p in note.iter_paragraphs():
    print(p.text)
# 获取所有paragraphs实例
lst_paragraph = note.get_all_paragraphs()
# 创建新的paragraph
# 默认在最后创建，可设置index, 0为开头, -1为最后
p = note.create_paragraph("CONTEXT", index=0)


# 获取文本
print(p.text)
# 修改文本
p.text = "import pandas as pd"
# 移动到某个index
p.move_to_index(0)

# 运行代码
p.run()
# 停止运行
p.stop()
# 运行状态
print(p.status)
# 运行结果
print(p.results)
# 获取job nme
print(p.job_name)
# 获取完成时间
print(p.date_finished)

# 删除paragraph
p.delete()
```
> 更多功能请查阅[zeppelin.\__init__](zeppelin/__init__.py)