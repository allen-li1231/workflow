import sys
import importlib.util

HUE_BASE_URL = "http://10.19.166.2:8000"

HUE_DOWNLOAD_BASE_URL = "http://10.19.185.2:8000"

MAX_LEN_PRINT_SQL = 100

HUE_INACTIVE_TIME = 1800

HUE_MAX_CONCURRENT_SQL = 4

HUE_DOWNLOAD_LARGE_TABLE_ROWS = 100000

TEZ_SESSION_TIMEOUT_SECS = 300

HIVE_PERFORMANCE_SETTINGS = {
    # resource settings:
    # "mapreduce.map.memory.mb": f"{4096 * 2}",
    # "mapreduce.reduce.memory.mb": f"{8192 * 2}",
    # "mapreduce.map.java.opts": f"-Djava.net.preferIPv4Stack=true -Xms3276m -Xmx3276m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseCompressedOops -Xmx3435134976",
    # "mapreduce.reduce.java.opts": f"-Djava.net.preferIPv4Stack=true  -Xms6554m -Xmx6554m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseCompressedOops -Xmx6872367104",
    # "hive.exec.reducers.bytes.per.reducer": f"{67108864 // 2}"   # decrease by half would increase parallelism

    # when nodes read data from HDFS, combine small files < 16 MB to decrease number of mappers
    "hive.tez.input.format": "org.apache.hadoop.hive.ql.io.HiveInputFormat",
    # "tez.grouping.min-size": "16777216",
    # "tez.grouping.max-size": "256000000",
    "tez.grouping.split-waves": "1.8",
    # enable block read from HDFS, which decreases number of mappers while using mr engine
    # "mapred.min.split.size": "16777216",
    # "mapred.max.split.size": "256000000",
    # "mapreduce.input.fileinputformat.split.minsize": "16777216",
    # max(mapred.min.split.size, min(mapred.max.split.size, dfs.block.size))
    # "mapreduce.input.fileinputformat.split.maxsize": "256000000",

    # vectorization and parallelism
    "hive.vectorized.execution.enabled": "true",
    "hive.vectorized.execution.reduce.enabled": "true",
    "hive.vectorized.input.format.excludes": "",
    "hive.exec.parallel": "true",
    "hive.exec.parallel.thread.number": "4",
    "hive.exec.dynamic.partition.mode": "nonstrict",
    "hive.tez.auto.reducer.parallelism": "true",

    # enable output compression to save network IO
    "hive.exec.compress.output": "true",
    "hive.exec.compress.intermediate": "true",
    "hive.intermediate.compression.codec": "org.apache.hadoop.io.compress.SnappyCodec",
    "hive.intermediate.compression.type": "BLOCK",

    # enable inserting data into a bucketed or sorted table
    "hive.enforce.bucketing": "true",
    "hive.enforce.sorting": "true",
    # enable SMB map join
    # BUG: on cluster 166, this causes "ReduceWork" cannot be casted to "MapWork" error using Hive on Spark
    #"hive.optimize.bucketmapjoin": "true",
    "hive.optimize.bucketmapjoin.sortedmerge": "true",
    # total size of (n-1) tables that can be converted to mapjoin
    # ref: http://www.openkb.info/2016/01/difference-between-hivemapjoinsmalltabl.html
    "hive.auto.convert.join.noconditionaltask.size": "268435456",
    # BUG: on cluster 185, enabling all these ones could cause Vertex Error:
    #"hive.auto.convert.sortmerge.join": "true",
    #"hive.auto.convert.sortmerge.join.noconditionaltask": "true",
    #"hive.auto.convert.sortmerge.join.bigtable.selection.policy": "org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ",
    #"hive.auto.convert.sortmerge.join.to.mapjoin": "true",

    # allow subdirectory in mapreduce
    "hive.mapred.supports.subdirectories": "true",
    "mapreduce.input.fileinputformat.input.dir.recursive": "true",
    "mapred.input.dir.recursive": "true",
    "spark.hadoop.hive.input.dir.recursive": "true",
    "spark.hadoop.hive.mapred.supports.subdirectories": "true",
    "spark.hadoop.hive.supports.subdirectories": "true",
    "spark.hadoop.hive.mapred.input.dir.recursive": "true",

    "hive.optimize.skewjoin": "true",
    "hive.optimize.skewjoin.compiletime": "true",
    # "hive.skewjoin.key": "100000",
    # "hive.skewjoin.mapjoin.map.tasks": "10000",
    # "hive.skewjoin.mapjoin.min.split": "33554432",

    # "hive.optimize.union.remove": "true",

    "hive.ignore.mapjoin.hint": "false",
    "hive.cbo.enable": "true",
    "hive.compute.query.using.stats": "true",
    "hive.exec.orc.zerocopy": "true",

    # refer to: "Hive Understanding concurrent sessions queue allocation"
    "hive.execution.engine": "tez",
    # "tez.queue.name": "root.fengkong",

    # https://blog.cloudera.com/optimizing-hive-on-tez-performance/
    # "hive.prewarm.enabled": "true",
    # refer to: "Configure Tez Container Reuse"
    # "tez.am.mode.session": "true",
    # "tez.session.am.dag.submit.timeout.secs": f"{TEZ_SESSION_TIMEOUT_SECS}",
    # "tez.am.container.reuse.enabled": "true",
    # "tez.am.container.session.delay-allocation-millis": f"{TEZ_SESSION_TIMEOUT_SECS * 1000}",

    # resolve return code 3 from spark failure
    "mapred.map.tasks.speculative.execution": "true",
    "mapred.reduce.tasks.speculative.execution": "true",
}

PROGRESSBAR = {
    "disable": False,
    "leave": None,
    "bar_format": '{l_bar}{bar:25}|{elapsed}',
    "desc": "NotebookResult[{name}] awaiting {result}",
    "file": sys.stdout,
    "ascii": True
}

EXCEL_ENGINE = "xlsxwriter" if importlib.util.find_spec("xlsxwriter") else "openpyxl"

# jupyter
JUPYTER_URL = 'http://10.19.181.26:9999'
JUPYTER_TOKEN = "fengkong"
JUPYTER_MAX_UPLOAD_SIZE = 25 * 1024 * 1024

# jump_server
JUMP_SERVER_HOST = "10.1.82.105"
JUMP_SERVER_PORT = 22
JUMP_SERVER_BACKEND_HOST = "10.18.22.95"
JUMP_SERVER_ORACLE_HOST = "oracle.geexfinance.com"
JUMP_SERVER_ORACLE_SERVICE_NAME = None
JUMP_SERVER_ORACLE_SID = "geexdb"

# zeppelin
ZEPPELIN_URL = "http://10.19.166.33:8080"
ZEPPELIN_INTERPRETER = "pyspark"
ZEPPELIN_INACTIVE_TIME = 3600.
ZEPPELIN_PARAGRAPH_CONFIG = {
    'editorSetting': {
        'language': 'python',
        'editOnDblClick': False,
        'completionSupport': True,
        'completionKey': 'TAB'
    },
    'colWidth': 12.0,
    'editorMode': 'ace/mode/python',
    'fontSize': 9.0,
    'results': {},
    'enabled': True
}

# hive
HIVESERVER_IP = '116.213.205.157'
HIVESERVER_PORT = 10000
HIVECLI_MAX_CONCURRENT_SQL = 3
