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

    val KUDU_MASTER = "34.202.225.92"
    val DEST_TABLE = "stack_bd_v6"
    val TIME_SECONDS = 3600
    var index = 1
    val DAY_MILLISECONDS = 24*60*60*1000

    val client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build()

    val tagList = List("spark", "hadoop", "flink", "impala", "drill", "scala", "java",
      "tableau", "mongo", "cassandra", "apex", "apache-drill", "ranger", "sentry", "mahout",
      "r", "python", "pandas", "druid", "amazon", "aws", "elastic", "logstash", "elk",
      "kibana", "google", "gcp", "cloudera", "hortonworks", "hbase", "lucene", "solr",
      "lambda", "kappa", "science", "presto", "storm", "pulsar", "redis", "berkley", "neo4j",
      "kafka", "samza", "hive", "pig", "sqoop", "flume", "sql", "kudu", "mongodb", "akka",
      "java", "amazon-kinesis", "amazon-web-services", "azure", "pentaho", "ambari", "maven",
      "sbt", "couchdb", "couchbase", "sql", "nosql", "docker", "kubernetes", "google-cloud",
      "hdfs", "mapreduce", "rabbitmq", "microservices", "amazon-s3")

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
    props.setProperty("group.id", "myGroup2");                 // Consumer group ID
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
      index = index + 1
      line.tags.map { tag =>
        ParsedLine(
          index,
          //tag.charAt(0).toString, // This is to partitionate
          "a", // This is to partitionate
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
        el.index,
        acc.tag,
        acc.creationMonth,
        acc.questions + 1,
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


    /*
     * transforming data into ParsedLines in order to have one tag
     * per line. Then we partitionate with the first letter of the tag.
     * Finally we process the window
     */
    val parsedLines: DataStream[ParsedLine] = soLines.flatMap(getParsedLine(_))
        .filter(line => tagList.contains(line.tag))
    val cc: DataStream[RowSerializable] = parsedLines
      .keyBy(_.environment)
      .timeWindow(Time.seconds(TIME_SECONDS))
      .trigger(new MonthTrigger(descriptor))
      .apply { (
        tag: String,  // id, de la key
        window: TimeWindow,
        events: Iterable[ParsedLine],
        out: Collector[RowSerializable]) =>

      val group: Map[(String, String), Iterable[ParsedLine]] = events.groupBy(record => (record.tag, record.creationMonth))
        val tags: Iterable[(String, String)] = group.keys
        tags.map{ tagMonth =>
          val parsedLineIterable: Iterable[ParsedLine] = group(tagMonth)
          val defaultAccTagInMemory = AccTagInMemory(
            1,
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

          val row = new RowSerializable(10)
              row.setField(0, accTag.index)
              row.setField(1, accTag.tag)
              row.setField(2, month)
              row.setField(3, year)
              row.setField(4, accTag.questions.toInt)
              row.setField(5, accTag.closed_questions.toInt)
              row.setField(6, closingTimeDays)
              row.setField(7, accTag.total_score.toInt)
              row.setField(8, accTag.unique_users.distinct.length)
              row.setField(9, accTag.answerCount.toInt)

          out.collect( row )
        }

      }
    // We add the kuduSink to insert
    cc.addSink(kuduSink)

    // execute program
    env.execute
  }
}