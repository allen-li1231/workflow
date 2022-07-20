HUE_BASE_URL = "http://10.19.185.29:8889"

MAX_LEN_PRINT_SQL = 100

TEZ_SESSION_TIMEOUT_SECS = 600

HIVE_PERFORMANCE_SETTINGS = {
    "hive.vectorized.execution.reduce.enabled": "true",
    "hive.exec.parallel.thread": "true",
    "hive.exec.dynamic.partition.mode": "nonstrict",
    "hive.exec.compress.output": "true",
    "hive.exec.compress.intermediate": "true",
    "hive.intermediate.compression.codec": "org.apache.hadoop.io.compress.SnappyCodec",
    "hive.intermediate.compression.type": "BLOCK",
    "hive.optimize.skewjoin": "true",
    "hive.ignore.mapjoin.hint": "false",

    # refer to: "Hive Understanding concurrent sessions queue allocation"
    "hive.tez.auto.reducer.parallelism": "true",
    "tez.queue.name": "root.fengkong",
    # refer to: "Configure Tez Container Reuse"
    "tez.session.am.dag.submit.timeout.secs": f"{TEZ_SESSION_TIMEOUT_SECS}",
    "tez.am.container.reuse.enabled": "true",
    "tez.am.container.session.delay-allocation-millis": f"{TEZ_SESSION_TIMEOUT_SECS * 1000}",
    "hive.execution.engine": "tez",
}
