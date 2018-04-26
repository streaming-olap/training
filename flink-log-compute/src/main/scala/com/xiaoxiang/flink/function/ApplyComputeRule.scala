package com.xiaoxiang.flink.function



import com.xiaoxiang.flink.bean.{ComputeConf, ComputeResult, LogEvent}
import com.xiaoxiang.flink.constants.Constants._
import com.xiaoxiang.flink.util.KeyUtil
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by luojiangyu on 3/18/18.
  */
class ApplyComputeRule extends CoFlatMapFunction[LogEvent, ComputeConf, ComputeResult]{

  private var computeConf = ComputeConf(0, Array())

  override def flatMap2(in2: ComputeConf, collector: Collector[ComputeResult]): Unit = {
    computeConf = in2
  }

  override def flatMap1(logEvent: LogEvent, collector: Collector[ComputeResult]): Unit = {
    applyConfToLog(logEvent, computeConf).foreach(collector.collect)
  }

  def applyConfToLog(logEvent: LogEvent, computeConf: ComputeConf): ArrayBuffer[ComputeResult] = {
    var array = mutable.ArrayBuffer[ComputeResult]()
    computeConf.data.foreach(rule => {
      var valid = true

      // filter 规则处理
      rule.filters.foreach(f => {
          if(logEvent.content.contains(f.name)) {
            val v = logEvent.content(f.name)
            f.operation match {
              case GT => valid = v.toDouble > f.value.toDouble
              case EQ => valid = v == f.value
              case LT => valid = v.toDouble < f.value.toDouble
              case GE => valid = v.toDouble >= f.value.toDouble
              case LE => valid = v.toDouble <= f.value.toDouble
              case NE => valid = v != f.value
              case _ => valid = false
            }
          }
        })

      if(valid) {


        val elmts = rule.groupby.foldLeft(mutable.ArrayBuffer[String](rule.id.toString)) {
          (array, dimension) => {
            array += logEvent.content.getOrElse(dimension, VALUE_UNDEFINED)
          }
        }
        val key = KeyUtil.hash(elmts)


        val dimensions = rule.groupby.foldLeft(mutable.HashMap[String, String]()) {
          (map, dimension) => {
            map += (dimension -> logEvent.content.getOrElse(dimension, VALUE_UNDEFINED))
          }
        }
        val meta = mutable.HashMap(
          FIELD_UNIQUE_ID -> key,
          FIELD_DATETIME -> logEvent.dateTime,
          FIELD_TIMESTAMP -> (logEvent.dateTime.getMillis/60000 *60000).asInstanceOf[AnyRef],
          FIELD_DATASOURCE -> rule.id.toString,
          FIELD_TIMESTAMP_INTERNAL -> logEvent.dateTime.getMillis.asInstanceOf[AnyRef]

        )
        val successCount = if (logEvent.content(DIMENSION_STATUS).toInt < 400) VALUE_DOUBLE_1 else VALUE_DOUBLE_0
        val values = mutable.HashMap(
          DIMENSION_RT -> logEvent.content.getOrElse[String](DIMENSION_RT, VALUE_STRING_0).toDouble,
          FIELD_COUNT -> 1.0,
          FIELD_SUCCESS_COUNT -> successCount
        )

        val periods = rule.periods.mkString(SEP_SEMICOL)

        array+= ComputeResult(key, meta, dimensions, values, periods)

      }


    })
    array
  }
}
