package com.aiguigu.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}

import org.apache.flink.table.api.scala._


object KafkaSqlDemo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    import org.apache.flink.table.api.EnvironmentSettings
    val bsSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, bsSettings)

    tableEnv.connect(new Kafka()
    .version("0.11")
        .topic("search-query-str-topic")
        .property("zookeeper.connect", "localhost:2181")
        .property("bootstrap.servers", "localhost:9092")
        .property("group.id", "search_query_str_group")
    )
        .withFormat(new Json())
        .withSchema(new Schema()
        .field("user_id", DataTypes.STRING())
            .field("query", DataTypes.STRING())
            .field("ts", DataTypes.INT())
        )
        .createTemporaryTable("user_behavior")

    val countTable: Table = tableEnv.sqlQuery(
      """
        |SELECT user_id, query, count(1) AS cnt FROM user_behavior GROUP BY user_id, query
        |""".stripMargin)

    tableEnv.createTemporaryView("query_count", countTable)


    val resultTable: Table = tableEnv.sqlQuery("""
      |SELECT query, cnt, row_num
      |FROM
      |(
      |   SELECT user_id, query, cnt, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY cnt desc) AS row_num
      |   FROM query_count
      |)
      |WHERE row_num <= 3
      |""".stripMargin
    )

    import org.apache.flink.api.scala._

    resultTable.toRetractStream[(String, Long, Long)].print("top N")

    env.execute("sql api test")

  }

}
