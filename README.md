# 风控部工具包
### 包含以下模块:

- 使用hue notebook api实现的hive sql运行、query状态跟踪和数据拉取等功能
- 通过爬虫对hue下载系统的上传下载进行自动化
- 使用jupyter api实现的上传本地数据到jupyter远端文件系统
- 结合远端服务器命令行实现的jupyter kernel内存使用情况和变量内存占用的可视化

### 使用hue模块运行sql并拉取结果
``` python
import workflow
import pandas as pd

HUE = workflow.hue("USERNAME", "PASSWORD")
result = HUE.hue_sys.execute("select 1;)

# 直接拉取
data = result.fetchall()

# 或使用下载平台
data = HUE.download_data(table_name="TABLE_NAME",
                         reason="YOUR_REASON",
                         Decode_col=COL_TO_DECODE)
                         
# 使用Pandas API
df = pd.DataFrame(**data)
df.head()

# 或直接保存csv到本地
result = HUE.hue_sys.execute("select 1;")
result.to_csv("PATH_TO_CSV_FILE")
```

> Note: workflow.hue.Notebook提供了更为丰富的接口，可以使用上面的HUE.hue_sys获得notebook实例，也可以对Notebook显式地调用，参考如下：

### 使用hue.Notebook调用hue notebook api
``` python
# 使用方法1
with Notebook("USERNAME", "PASSWORD",
              name="Notebook name",
              description="Notebook description",
              verbose=True)\
        as notebook:

    res = notebook.execute("SET hive.execution.engine;")
    data = res.fetchall()


# 使用方法2
notebook = Notebook(name="Notebook name",
                    description="Notebook description",
                    verbose=True)

with notebook.login("USERNAME", "PASSWORD"):
    res = notebook.execute("SET hive.execution.engine;")
    data = res.fetchall()


# 使用方法3
notebook = Notebook(name="Notebook name",
                    description="Notebook description")
notebook.login("USERNAME", "PASSWORD")
res = notebook.execute("SET hive.execution.engine;")
data = res.fetchall()

# 按需手动释放notebook和session资源
notebook.close()

# 登出Hue
notebook.logout()
```

### 多开notebook，并行多条sql
``` python
import time
from workflow.hue import Notebook

notebook = Notebook(name="Notebook name",
                    description="Notebook description",
                    hive_settings=None, # 无需执行加速指令
                    verbose=True)
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

        if notebook._result.is_ready:
            # 对结果进行fetchall()等操作后...
            
            notebook.close()
            d_notebook[notebook] = True

    time.sleep(5)
``` 
