HUE_BASE_URL = "http://10.19.185.29:8889"

HUE_DOWNLOAD_BASE_URL = "http://10.19.185.103:8000"

MAX_LEN_PRINT_SQL = 100

TEZ_SESSION_TIMEOUT_SECS = 600

HIVE_PERFORMANCE_SETTINGS = {
    # resource settings:
    # "mapreduce.map.memory.mb": f"{2048 * 2}",
    # "mapreduce.reduce.memory.mb": f"{2048 * 2}",
    # "hive.exec.reducers.bytes.per.reducer": f"{134217728 // 2}"   # decrease by half would increase parallelism
    # when nodes read data from HDFS, combine small files < 64 MB to decrease number of mappers
    "mapreduce.input.fileinputformat.split.minsize": "67108864",

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
    # enable block read from HDFS, which decreases number of mappers
    "hive.tez.input.format": "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat",

    # let spark app wait longer for executors' responses
    "hive.spark.client.connect.timeout": "30000ms",
    "hive.spark.client.server.connect.timeout": "300000ms"
}
