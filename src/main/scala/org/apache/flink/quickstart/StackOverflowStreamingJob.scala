package org.apache.flink.quickstart

import java.util.Properties

import es.accenture.flink.Sink.KuduSink
import es.accenture.flink.Utils.RowSerializable
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.quickstart.utils._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.apache.kudu.client.KuduClient
import org.joda.time.DateTime

object StackOverflowStreamingJob {
  val descriptor = new ValueStateDescriptor[Integer]("month", Integer.TYPE)

  def main(args: Array[String]) {
    //val KUDU_MASTER = "192.168.56.101"
    val KUDU_MASTER = "34.202.225.92"
    val DEST_TABLE = "stack_small_bd"
    val TIME_SECONDS = 3600
    //val TIME_SECONDS = 150
    val DAY_MILLISECONDS = 24*60*60*1000

    val client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build()

    if(!client.tableExists(DEST_TABLE))
       CustomKuduUtils.createTable(DEST_TABLE, client)

    Thread.sleep(3000)

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure event-time characteristics
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // generate a Watermark every second
    //env.getConfig.setAutoWatermarkInterval(1000);

    val props: Properties = new Properties
    props.setProperty("zookeeper.connect", "localhost:2181"); // Zookeeper default host:port
    props.setProperty("bootstrap.servers", "localhost:9092"); // Broker default host:port
    props.setProperty("group.id", "myGroup");                 // Consumer group ID
    props.setProperty("auto.offset.reset", "earliest");       // Always read topic from start

    val kuduSink: KuduSink = new KuduSink(KUDU_MASTER, DEST_TABLE, CustomKuduUtils.aggTagColumns.toArray)

    // create the Kafka consumer
    val consumer =
      new FlinkKafkaConsumer010[String](
        "stack_topic",
        new SimpleStringSchema(),
        props)

    // create the Kafka source
    val lines: DataStream[String] = env.addSource(consumer)


    /*
     * Transform from a SoLine into several ParsedLines
     *
     * @param line SoLine with several tags in the same row
     *
     * return a Seq[ParsedLine] one row for each tag
     */
    def getParsedLine(line: SoLine): Seq[ParsedLine] = {
      val isClosed = line.closedDate.isDefined
      line.tags.map { tag =>
        ParsedLine(
          "test",
          tag,
          ProcessingUtils.getMonthYear(line.creationDate),
          if (isClosed) line.closedDate.get.getMillis - line.creationDate.getMillis else 0L,
          isClosed,
          line.score,
          line.userId,
          line.answerCount
        )
      }
    }


    def aggregateParsedLine(
      acc: AccTagInMemory,
      el: ParsedLine): AccTagInMemory = {

      AccTagInMemory(
        acc.tag,
        acc.creationMonth,
        acc.questions,
        if (el.closed) acc.closed_questions + 1 else acc.closed_questions,
        if (el.closed) acc.total_time_closing + el.timeToClose else acc.total_time_closing,
        acc.total_score + el.score,
        acc.unique_users ++ List(el.userId),
        acc.answerCount + el.answerCount
      )
    }

    // Cleaning data from the source returning SoLine Datastream
    val soLines: DataStream[SoLine] = lines.map{ line =>
      val fields: List[String] = line.split(",").toList
      SoLine(
        fields(0),
        new DateTime(fields(1).replaceAll("\\s", "")),
        ProcessingUtils.getOptionDateTime(fields(2)),
        ProcessingUtils.getOptionDateTime(fields(3)),
        ProcessingUtils.cleanInt(fields(4), 0),
        ProcessingUtils.cleanInt(fields(5), 0),
        ProcessingUtils.cleanInt(fields(6), 0),
        fields(7).split("\\|").toList
      )
    }

    val parsedLines: DataStream[ParsedLine] = soLines.flatMap(getParsedLine(_))
    /*
     * transforming data into ParsedLines in order to have one tag
     * per line. Then we partitionate with the tag.
     * Finally we process the window
     */
    val cc: DataStream[RowSerializable] = parsedLines
      .keyBy(_.tag)
      .timeWindow(Time.seconds(TIME_SECONDS))
      //.trigger(new MonthTrigger(descriptor))
      .apply { (
        tag: String,  // id, de la key
        window: TimeWindow,
        events: Iterable[ParsedLine],
        out: Collector[RowSerializable]) =>
        //println(events.toList.length)

      val group: Map[(String, String), Iterable[ParsedLine]] = events.groupBy(record => (record.tag, record.creationMonth))
        val tags: Iterable[(String, String)] = group.keys
        tags.map{ tagMonth =>
          val parsedLineIterable: Iterable[ParsedLine] = group(tagMonth)
          val defaultAccTagInMemory = AccTagInMemory(
            tagMonth._1,
            tagMonth._2,
            0L,
            0L,
            0L,
            0L,
            List.empty: List[Int],
            0L
          )
          val accTag: AccTagInMemory =
            parsedLineIterable.foldLeft(defaultAccTagInMemory: AccTagInMemory){(acc, el) =>
              aggregateParsedLine(acc, el)
            }

          val closingTimeDays: Int = (accTag.total_time_closing / DAY_MILLISECONDS).toInt
          val (year, month ): (String, String) = ProcessingUtils.getMonthAndYear(accTag.creationMonth)

          val row = new RowSerializable(9)
              row.setField(0, accTag.tag)
              row.setField(1, month)
              row.setField(2, year)
              row.setField(3, accTag.questions.toInt)
              row.setField(4, accTag.closed_questions.toInt)
              row.setField(5, closingTimeDays)
              row.setField(6, accTag.total_score.toInt)
              row.setField(7, accTag.unique_users.distinct.length)
              row.setField(8, accTag.answerCount.toInt)

          out.collect( row )
        }

      }
    cc.addSink(kuduSink)

    // execute program
    env.execute
  }
}