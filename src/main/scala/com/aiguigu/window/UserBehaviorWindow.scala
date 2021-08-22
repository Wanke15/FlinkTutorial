package com.aiguigu.window

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object UserBehaviorWindow {
  def main(args: Array[String]): Unit = {
    // 创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "flink-user-group")


    val inputStream = env.addSource( new FlinkKafkaConsumer011[String]("user_behavior", new SimpleStringSchema(), properties))

    // 转换为样例类，提取时间戳生成watermark
    val dataStream = inputStream.map(data => {
      val arr = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toLong, arr(3), arr(4).toLong)
    })
      .assignAscendingTimestamps(_.ts * 1000L)

    val aggStream = dataStream
      .filter(_.behavType=="pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResultFunction())

    val resultStream = aggStream
      .keyBy("windowEnd")
      .process(new TopNHotItems(3))

    //    dataStream.print("data")
    //    aggStream.print("agg")
    resultStream.print("result")

    env.execute("user window test")
  }

}

case class ItemViewCount(itemId: Long, windowEnd: Long, likeScore: Long)


// COUNT 统计的聚合函数实现，每出现一条记录加一
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L
  override def add(userBehavior: UserBehavior, acc: Long): Long = acc + 1
  override def getResult(acc: Long): Long = acc
  override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}
// 用于输出窗口的结果
class WindowResultFunction() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long],
                     collector: Collector[ItemViewCount]) : Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next
    collector.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

// 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  private var itemViewCountListState : ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    //    super.open(parameters)
    // 命名状态变量的名字和状态变量的类型
    //    val itemsStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])
    // 从运行时上下文中获取状态并赋值
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    // 每条数据都保存到状态中
    //    println("***************** process *****************", value)
    //  val time: Long = System.currentTimeMillis()
    //      println("定时器第一次注册：" + time)
    itemViewCountListState.add(value)
    // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
    // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
    context.timerService.registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 获取收到的所有商品点击量
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()
    val iter = itemViewCountListState.get().iterator()
    while (iter.hasNext) {
      allItems += iter.next()
    }

    itemViewCountListState.clear()
    // 按照点击量从大到小排序
    val sortedItems = allItems.sortBy(_.likeScore)(Ordering.Long.reverse).take(topSize)
    // 将排名信息格式化成 String, 便于打印
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for(i <- sortedItems.indices){
      val currentItem: ItemViewCount = sortedItems(i)
      // e.g.  No1：  商品ID=12224  浏览量=2413
      result.append("No").append(i+1).append(":")
        .append("  商品ID=").append(currentItem.itemId)
        .append("  浏览量=").append(currentItem.likeScore).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(5000)
    out.collect(result.toString)
  }
}

case class UserBehavior(userId: Long, itemId: Long, catId: Long, behavType: String, ts: Long)