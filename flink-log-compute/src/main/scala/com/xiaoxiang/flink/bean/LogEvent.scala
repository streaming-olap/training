package com.xiaoxiang.flink.bean

import org.joda.time.DateTime

import scala.collection.mutable

case class LogEvent(level: String,
                    dateTime: DateTime,
                    source: String,
                    tag: String,
                    content: mutable.HashMap[String, String]
                   )
