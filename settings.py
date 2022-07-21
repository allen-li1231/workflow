HUE_BASE_URL = "http://10.19.185.29:8889"

MAX_LEN_PRINT_SQL = 100

TEZ_SESSION_TIMEOUT_SECS = 600

HIVE_PERFORMANCE_SETTINGS = {
    "hive.auto.convert.sortmerge.join": "true",
    "hive.vectorized.execution.reduce.enabled": "true",
    "hive.exec.parallel.thread": "true",
    "hive.exec.dynamic.partition.mode": "nonstrict",
    "hive.exec.compress.output": "true",
    "hive.exec.compress.intermediate": "true",
    "hive.intermediate.compression.codec": "org.apache.hadoop.io.compress.SnappyCodec",
    "hive.intermediate.compression.type": "BLOCK",
    "hive.optimize.bucketmapjoin": "true",
    "hive.optimize.bucketmapjoin.sortedmerge": "true",
    "hive.auto.convert.sortmerge.join.bigtable.selection.policy": "org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ",
    "hive.auto.convert.sortmerge.join.to.mapjoin": "true",
    "hive.optimize.skewjoin": "true",
    "hive.optimize.skewjoin.compiletime": "true",
    "hive.optimize.union.remove": "true",
    "hive.ignore.mapjoin.hint": "false",

    # refer to: "Hive Understanding concurrent sessions queue allocation"
    "hive.execution.engine": "tez",
    "hive.tez.auto.reducer.parallelism": "true",
    "tez.queue.name": "root.fengkong",
    # refer to: "Configure Tez Container Reuse"
    "tez.session.am.dag.submit.timeout.secs": f"{TEZ_SESSION_TIMEOUT_SECS}",
    "tez.am.container.reuse.enabled": "true",
    "tez.am.container.session.delay-allocation-millis": f"{TEZ_SESSION_TIMEOUT_SECS * 1000}",
}
