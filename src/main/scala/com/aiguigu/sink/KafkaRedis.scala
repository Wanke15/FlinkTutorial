package com.aiguigu.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

case class SensorData(id: String, timestamp: Long, temperature: Double)

class MyRedisMapper extends RedisMapper[SensorData] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  override def getKeyFromData(data: SensorData): String = data.id

  override def getValueFromData(data: SensorData): String = data.temperature.toString
}

object KafkaRedis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(4)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "flink-sensor-group")
    val inputDataStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    val dataStream = inputDataStream.map(data => {
      val arr = data.split(",")
      SensorData(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    dataStream.addSink(new RedisSink[SensorData](conf, new MyRedisMapper))

    env.execute("flink kafka")

  }

}
