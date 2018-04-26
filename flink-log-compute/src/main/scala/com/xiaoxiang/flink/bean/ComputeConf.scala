package com.xiaoxiang.flink.bean


case class ComputeFilter(name: String, operation: String, value: String)

case class ComputeRule(id: Int,
                     groupby: Array[String],
                     results: Array[String],
                     filters: Array[ComputeFilter],
                     periods: Array[Long] = Array[Long](60))

case class ComputeConf(code: Int, data: Array[ComputeRule])