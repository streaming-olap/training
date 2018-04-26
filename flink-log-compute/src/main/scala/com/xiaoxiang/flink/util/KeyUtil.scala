package com.xiaoxiang.flink.util

import com.roundeights.hasher.Algo

/**
  * Created by luojiangyu on 3/18/18.
  */
object KeyUtil {

    @throws(classOf[Exception])
    def hash(array: scala.collection.mutable.ArrayBuffer[String]): String = {
      array.foldLeft(Algo.sha1.foldable) {
        (accum, str) => accum(str)
      }.done.hex
    }

}
