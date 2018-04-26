package com.xiaoxiang.flink


import java.util.Properties

import com.xiaoxiang.flink.bean.{ComputeResult, LogEvent}
import com.xiaoxiang.flink.constants.Constants._
import com.xiaoxiang.flink.function.{AggregateFunc, ApplyComputeRule}
import com.xiaoxiang.flink.schema.{ComputeResultSerializeSchema, LogEventDeserializationSchema}
import com.xiaoxiang.flink.source.ConfSource
import com.xiaoxiang.flink.watermarker.BoundedLatenessWatermarkAssigner
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}



object Launcher {

  /**
    *
    * @param args:
    * 0: bootstrap Servers
    * 1: groupId
    * 2: consumerTopic
    * 3: retries
    * 4: producerTopic
    * 5: url
    * 6: latency
    */
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /* Checkpoint */
    env.enableCheckpointing(60000L)
    val checkpointConf = env.getCheckpointConfig
    checkpointConf.setMinPauseBetweenCheckpoints(30000L)
    checkpointConf.setCheckpointTimeout(8000L)
    checkpointConf.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    /* Kafka consumer */
    val consumerProps = new Properties()
    consumerProps.setProperty(KEY_BOOTSTRAP_SERVERS, args(0))
    consumerProps.setProperty(KEY_GROUP_ID, args(1))
    val consumer = new FlinkKafkaConsumer010[LogEvent](
      args(2),
      new LogEventDeserializationSchema,
      consumerProps
    )

    val producerProps = new Properties()
    producerProps.setProperty(KEY_BOOTSTRAP_SERVERS, args(0))
    producerProps.setProperty(KEY_RETRIES, args(3))
    val producer =new FlinkKafkaProducer010[ComputeResult](
      args(4),
      new ComputeResultSerializeSchema(args(4)),
      producerProps
    )
    /* at_ least_once 设置 */
    producer.setLogFailuresOnly(false)
    producer.setFlushOnCheckpoint(true)

      /*confStream **/
    val confStream = env.addSource(new ConfSource(args(5)))
      .setParallelism(1)
      .broadcast


    env.addSource(consumer)
      .connect(confStream)
      .flatMap(new ApplyComputeRule)
      .assignTimestampsAndWatermarks(new BoundedLatenessWatermarkAssigner(args(6).toInt))
      .keyBy(FIELD_KEY)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .reduce(_ + _)
      .keyBy(FIELD_KEY, FIELD_PERIODS)
      .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(1)))
      .apply(new AggregateFunc())
      .addSink(producer)

    env.execute("log_compute")

  }

}
