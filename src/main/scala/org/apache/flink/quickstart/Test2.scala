package org.apache.flink.quickstart

/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import java.util.Properties

import com.sun.tools.corba.se.idl.StringGen
import es.accenture.flink.Sink.KuduSink
import es.accenture.flink.Utils.RowSerializable
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kudu.client.KuduClient
import org.joda.time.DateTime
//import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}

import scala.runtime.ScalaRunTime

/**
  * Skeleton for a Flink Streaming Job.
  *
  * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
  * file in the same package/directory or have a look at the website.
  *
  * You can also generate a .jar file that you can submit on your Flink
  * cluster. Just type
  * {{{
  *   mvn clean package
  * }}}
  * in the projects root directory. You will find the jar in
  * target/flink-scala-project-0.1.jar
  * From the CLI you can then run
  * {{{
  *    ./bin/flink run -c org.apache.flink.quickstart.StreamingJob target/flink-scala-project-0.1.jar
  * }}}
  *
  * For more information on the CLI see:
  *
  * http://flink.apache.org/docs/latest/apis/cli.html
  */
object Test2 {
  val descriptor = new ValueStateDescriptor[Integer]("month", Integer.TYPE)

  def main(args: Array[String]) {
    val KUDU_MASTER = "192.168.56.101"
    val DEST_TABLE = "test_13"
    val client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build()


    // if(!client.tableExists(DEST_TABLE))
    //   client.createTable(DEST_TABLE)
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure event-time characteristics
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // generate a Watermark every second
    env.getConfig.setAutoWatermarkInterval(1000);

    val props: Properties = new Properties
    props.setProperty("zookeeper.connect", "localhost:2181"); // Zookeeper default host:port
    props.setProperty("bootstrap.servers", "localhost:9092"); // Broker default host:port
    props.setProperty("group.id", "myGroup");                 // Consumer group ID
    props.setProperty("auto.offset.reset", "earliest");       // Always read topic from start

    // create a Kafka consumer
    val consumer =
      new FlinkKafkaConsumer010[String](
        "stack_topic",
        new SimpleStringSchema(),
        props)

    // create a Kafka source
    val lines: DataStream[String] = env.addSource(consumer)
    // val formatter: DateTimeFormatter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.getDefault())

    def cleanInt(input: String, default: Int) = {
      val inputToParsed = input.replaceAll("\\s", "")
      if(inputToParsed == "NA") default else inputToParsed.toInt
    }

    def getOptionDateTime(timestamp: String): Option[DateTime] = {
      val time = timestamp.replaceAll("\\s", "")
      if(time == "NA") None else Some(new DateTime(time))

    }
    val soLines: DataStream[SoLine] = lines.map{ line =>
      val fields: List[String] = line.split(",").toList
      SoLine(
        fields(0),
        new DateTime(fields(1).replaceAll("\\s", "")),
        getOptionDateTime(fields(2)),
        getOptionDateTime(fields(3)),
        cleanInt(fields(4), 0),
        cleanInt(fields(5), 0),
        cleanInt(fields(6), 0),
        fields(7).split("\\|").toList
      )
    }

   /* def getMonthYear(date: DateTime): String = {
      val stringMonth = date.getMonthOfYear.toString
      val month =
        if(stringMonth.length == 1) "0" + stringMonth
        else stringMonth
      month + date.getYear.toString
    }*/
    //soLines.print
   /* def getParsedLine(line: SoLine): Seq[ParsedLine] = {
      val isClosed = line.closedDate.isDefined
      val x = line.tags.map { tag =>
        ParsedLine(
          tag,
          getMonthYear(line.creationDate),
          if(isClosed) line.closedDate.get.getMillis - line.creationDate.getMillis else 0L,
          isClosed,
          line.score,
          line.userId,
          line.answerCount
        )
      }
      x
    }*/
    /*val parsedLines: WindowedStream[ParsedLine, String, TimeWindow] = soLines.flatMap{ line =>
      val isClosed = line.closedDate.isDefined
      line.tags.map { tag =>
        ParsedLine(
          tag,
          line.creationDate,
          if(isClosed) line.closedDate.get.getMillis - line.creationDate.getMillis else 0L,
          isClosed,
          line.score,
          line.userId,
          line.answerCount
        )
      }
    }.keyBy(_.tag).timeWindow(Time.days(32))*/

    /*def aggregateData(
      acc: Map[String, AccTagInMemory],
      el: ParsedLine): Map[String, AccTagInMemory] = {

      println("tag: " + el.tag)
      if (acc.keys.toList.contains(el.tag)) {
        val accTag: AccTagInMemory = acc(el.tag)
        acc ++ Map(el.tag -> AccTagInMemory(
          el.tag,
          accTag.creationMonth,
          accTag.questions,
          if (el.closed) accTag.closed_questions + 1 else accTag.closed_questions,
          if (el.closed) accTag.total_time_closing + el.timeToClose else accTag.total_time_closing,
          accTag.total_score + el.score,
          accTag.unique_users ++ List(el.userId),
          accTag.answerCount + el.answerCount
        ))
      } else {
        acc ++ Map(el.tag -> AccTagInMemory(
          el.tag,
          el.creationMonth,
          1L,
          if (el.closed) 1L else 0L,
          if (el.closed) el.timeToClose else 0L,
          el.score,
          List(el.userId),
          el.answerCount
        ))
      }
    }
*/
    /*val parsedLines: WindowedStream[ParsedLine, String, TimeWindow] =
      soLines
        .flatMap(getParsedLine(_))
        .keyBy(_.tag)
        .timeWindow(Time.seconds(5))
        //.timeWindow(Time.days(32))
        .trigger(new MonthTrigger(descriptor))
*/
    /* val parsedLines2: DataStream[ParsedLine] =
       soLines
         .flatMap(getParsedLine(_))

     parsedLines2.print*/
    /*val accTag: DataStream[Map[String, AccTagInMemory]] =
      parsedLines.fold(Map.empty: Map[String, AccTagInMemory])((acc, el) =>
        aggregateData(acc, el))

    accTag.print
*/


    val columnNames = List("1", "2")
    val columns: Array[String] = Array[String]("idd", "creationDate", "closedDate", "deletionDate", "score", "userId", "answerCount", "tags")
    val columns2: List[String] = List("idd", "creationDate", "closedDate", "deletionDate", "score", "userId", "answerCount", "tags")

    //soLines.print

    //soLines.addSink(new KuduSink(KUDU_MASTER, DEST_TABLE, columnNames))
    val kuduSink: KuduSink = new KuduSink(KUDU_MASTER, DEST_TABLE, columns2.toArray)
    //lines.addSink(new KuduSink(KUDU_MASTER, DEST_TABLE, columns))
    val b: DataStream[RowSerializable] = soLines.map{line =>
      val row = new RowSerializable(8)
      row.setField(0, line.id.toString)
      row.setField(1, "a")
      row.setField(2, "b")
      row.setField(3, "c")
      row.setField(4, "d")
      row.setField(5, "e")
      row.setField(6, "f")
      row.setField(7, "h")
      row
    }
    //b.print
     b.addSink(kuduSink)
    /*val c: DataStream[String] = b.map(x => "adsf")
    val kafkaProducer: FlinkKafkaProducer010[String] = new FlinkKafkaProducer010[String](
      "stack_topic_2",
      new serialization.SimpleStringSchema(),
      props)

    c.addSink(kafkaProducer)*/


    /**
      * Here, you can start creating your execution plan for Flink.
      *
      * Start with getting some data from the environment, like
      *  env.readTextFile(textPath);
      *
      * then, transform the resulting DataStream[String] using operations
      * like
      *   .filter()
      *   .flatMap()
      *   .join()
      *   .group()
      *
      * and many more.
      * Have a look at the programming guide:
      *
      * http://flink.apache.org/docs/latest/apis/streaming/index.html
      *
      */


    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}