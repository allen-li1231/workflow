import sys
import importlib.util

HUE_BASE_URL = "http://10.19.185.29:8889"

HUE_DOWNLOAD_BASE_URL = "http://10.19.185.103:8000"

YARN_BASE_URL = "http://10.19.185.102:8088"

MAX_LEN_PRINT_SQL = 100

TEZ_SESSION_TIMEOUT_SECS = 300

HIVE_PERFORMANCE_SETTINGS = {
    # resource settings:
    # "mapreduce.map.memory.mb": f"{2048 * 2}",
    # "mapreduce.reduce.memory.mb": f"{2048 * 2}",
    # "mapreduce.map.java.opts": f"-Djava.net.preferIPv4Stack=true -Xmx{1700 * 2}m",
    # "mapreduce.reduce.java.opts": f"-Djava.net.preferIPv4Stack=true -Xmx{1700 * 2}m",
    # "hive.exec.reducers.bytes.per.reducer": f"{134217728 // 2}"   # decrease by half would increase parallelism

    # when nodes read data from HDFS, combine small files < 32 MB to decrease number of mappers
    # "hive.tez.input.format": "org.apache.hadoop.hive.ql.io.HiveInputFormat",
    "tez.grouping.min-size": "33554432",
    "tez.grouping.max-size": "536870912",
    "tez.grouping.split-waves": "1.8",
    # enable block read from HDFS, which decreases number of mappers while using mr engine
    "mapred.min.split.size": "33554432",
    "mapred.max.split.size": "536870912",
    "mapreduce.input.fileinputformat.split.minsize": "33554432",
    # max(mapred.min.split.size, min(mapred.max.split.size, dfs.block.size))
    "mapreduce.input.fileinputformat.split.maxsize": "536870912",

    # vectorization and parallelism
    "hive.vectorized.execution.reduce.enabled": "true",
    "hive.vectorized.input.format.excludes": "",
    "hive.exec.parallel.thread": "true",
    "hive.exec.dynamic.partition.mode": "nonstrict",

    # enable output compression to save network IO
    "hive.exec.compress.output": "true",
    "hive.exec.compress.intermediate": "true",
    "hive.intermediate.compression.codec": "org.apache.hadoop.io.compress.SnappyCodec",
    "hive.intermediate.compression.type": "BLOCK",

    # enable inserting data into a bucketed or sorted table
    "hive.enforce.bucketing": "true",
    "hive.enforce.sorting": "true",
    "hive.optimize.bucketmapjoin": "true",
    "hive.optimize.bucketmapjoin.sortedmerge": "true",
    # BUG: enabling all these ones could cause Vertex Error:
    # "hive.auto.convert.sortmerge.join": "true",
    # "hive.auto.convert.sortmerge.join.noconditionaltask": "true",
    # "hive.auto.convert.sortmerge.join.bigtable.selection.policy": "org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ",
    # "hive.auto.convert.sortmerge.join.to.mapjoin": "true",

    "hive.optimize.skewjoin": "true",
    "hive.optimize.skewjoin.compiletime": "true",
    "hive.optimize.union.remove": "true",

    "hive.ignore.mapjoin.hint": "false",
    "hive.cbo.enable": "true",
    "hive.compute.query.using.stats": "true",

    # refer to: "Hive Understanding concurrent sessions queue allocation"
    "hive.execution.engine": "tez",
    "hive.tez.auto.reducer.parallelism": "true",
    "tez.queue.name": "root.fengkong",
    # refer to: "Configure Tez Container Reuse"
    "tez.session.am.dag.submit.timeout.secs": f"{TEZ_SESSION_TIMEOUT_SECS}",
    "tez.am.container.reuse.enabled": "true",
    "tez.am.container.session.delay-allocation-millis": f"{TEZ_SESSION_TIMEOUT_SECS * 1000}",

    # let spark app wait longer for executors' responses
    # "hive.spark.client.connect.timeout": "30000ms",
    # "hive.spark.client.server.connect.timeout": "300000ms"
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
