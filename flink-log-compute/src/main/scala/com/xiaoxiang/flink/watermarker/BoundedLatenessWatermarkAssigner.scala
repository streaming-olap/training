package com.xiaoxiang.flink.watermarker


import com.xiaoxiang.flink.constants.Constants._
import com.xiaoxiang.flink.bean.ComputeResult
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * Created by luojiangyu on 3/18/18.
  */
class BoundedLatenessWatermarkAssigner(allowLateness: Int) extends AssignerWithPeriodicWatermarks[ComputeResult] {
  private var maxTimestamp = -1L

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTimestamp - allowLateness * 1000L)
  }

  override def extractTimestamp(t: ComputeResult, l: Long): Long = {
    val timestamp = t.metaData(FIELD_TIMESTAMP_INTERNAL).asInstanceOf[Long]
    if (timestamp > maxTimestamp) {
      maxTimestamp = timestamp
    }
    timestamp
  }
}