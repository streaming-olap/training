package com.xiaoxiang.flink.bean

import com.xiaoxiang.flink.constants.Constants._

import scala.collection.mutable



case class ComputeResult(key : String,
                         metaData: mutable.HashMap[String, AnyRef],
                         dimensions: mutable.HashMap[String, String],
                         values: mutable.HashMap[String , Double],
                         periods: String = VALUE_UNDEFINED){
  def +(input: ComputeResult) = {
    val rt = this.values(DIMENSION_RT) + input.values(DIMENSION_RT)
    val count = this.values(FIELD_COUNT) + input.values(FIELD_COUNT)
    val successCount = this.values(FIELD_SUCCESS_COUNT) + input.values(FIELD_SUCCESS_COUNT)

    val values = mutable.HashMap(
      DIMENSION_RT -> rt,
      FIELD_COUNT -> count,
      FIELD_SUCCESS_COUNT -> successCount
    )

    ComputeResult(
      key = this.key,
      metaData = this.metaData,
      dimensions = this.dimensions,
      values = values,
      periods = this.periods
    )
  }
}

