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
import org.apache.flink.util.Collector
import org.apache.kudu.{ColumnSchema, Type}
import org.apache.kudu.client.{CreateTableOptions, KuduClient}
import org.joda.time.DateTime
//import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import org.apache.kudu.Schema
import scala.runtime.ScalaRunTime




object Test2 {
  val descriptor = new ValueStateDescriptor[Integer]("month", Integer.TYPE)
  val aggTagColumns = List("tag", "creationMonth", "questions", "closed_questions", "total_time_closing", "total_score", "unique_users", "answerCount")

  def createTable(tableName: String, client: KuduClient) = {
    try{
      val columns2: List[(String, String)] =
      List(
        ("id", "INT"),
        ("tag", "STRING"),
        ("creationMonth", "STRING"),
        ("questions", "INT"),
        ("closed_questions", "INT"),
        ("total_time_closing", "INT"),
        ("total_score", "INT"),
        ("unique_users", "INT"),
        ("answerCount", "INT"))

      val columns: List[ColumnSchema] = columns2.map { column =>
        if (column._2 == "INT") {
          new ColumnSchema.ColumnSchemaBuilder(column._1, Type.INT32)
            .key(true)
            .build()
        } else if( column._2 == "FLOAT"){
          new ColumnSchema.ColumnSchemaBuilder(column._1, Type.FLOAT)
            .key(true)
            .build()
        }else{
          new ColumnSchema.ColumnSchemaBuilder(column._1, Type.STRING)
            .key(true)
            .build()
        }
      }


      // Para particionado
      val rangeKeys: List[String] = List("tag")

      import scala.collection.JavaConverters._
      val javaColumns: java.util.List[ColumnSchema] = columns.asJava

      val schema: Schema = new Schema(javaColumns)

      client.createTable(tableName, schema,
        new CreateTableOptions()
          .setRangePartitionColumns(rangeKeys.asJava).addHashPartitions(rangeKeys.asJava, 1))
      true
    } catch {
      case e=>
        /*Print stack disable for JUnit*/
        false
    }
  }

  def main(args: Array[String]) {
    //val KUDU_MASTER = "192.168.56.101"
    val KUDU_MASTER = "34.202.225.92"
    val DEST_TABLE = "no_stack_v10"
    //val TIME_SECONDS = 3600
    val TIME_SECONDS = 30
    val DAY_MILLISECONDS = 24*60*60*1000

    val client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build()

    if(!client.tableExists(DEST_TABLE))
       createTable(DEST_TABLE, client)
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

    val parsedLineColumns = List("tag", "creationMonth", "timeToClose", "closed", "score", "userId", "answerCount")



    //soLines.addSink(new KuduSink(KUDU_MASTER, DEST_TABLE, columnNames))
    val kuduSink: KuduSink = new KuduSink(KUDU_MASTER, DEST_TABLE, aggTagColumns.toArray)

    // create a Kafka consumer
    val consumer =
      new FlinkKafkaConsumer010[String](
        "stack_topic",
        new SimpleStringSchema(),
        props)

    // create a Kafka source
    val lines: DataStream[String] = env.addSource(consumer)

    def cleanInt(input: String, default: Int) = {
      val inputToParsed = input.replaceAll("\\s", "")
      if(inputToParsed == "NA") default else inputToParsed.toInt
    }

    def getOptionDateTime(timestamp: String): Option[DateTime] = {
      val time = timestamp.replaceAll("\\s", "")
      if(time == "NA") None else Some(new DateTime(time))

    }

    def getMonthYear(date: DateTime): String = {
      val stringMonth = date.getMonthOfYear.toString
      val month =
        if(stringMonth.length == 1) "0" + stringMonth
        else stringMonth
      month + date.getYear.toString
    }
    //soLines.print
    def getParsedLine(line: SoLine): Seq[ParsedLine] = {
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

    val myFoldFunction: (Long, SoLine) => Long =
      (acc: Long, el: SoLine) =>
        acc + el.score.toLong


    /*val d = soLines.keyBy(_.id)
      .timeWindow(Time.hours(1))
      .fold(0L: Long, (acc: Long, el: SoLine) =>
        acc + el.score.toLong)
    d.print()*/

      val cc: DataStream[RowSerializable] = soLines
        .flatMap(getParsedLine(_))
        .keyBy(_.tag)
        .timeWindow(Time.seconds(TIME_SECONDS))
        //.trigger(new MonthTrigger(descriptor))
        .apply { (
          tag: String,  // id, de la key
          window: TimeWindow,
          events: Iterable[ParsedLine],
          out: Collector[RowSerializable]) =>
          events.map{event =>
          }
          val group: Map[(String, String), Iterable[ParsedLine]] = events.groupBy(record => (record.tag, record.creationMonth))
          val tags: Iterable[(String, String)] = group.keys
          tags.map{ tag =>
            val parsedLineIterable: Iterable[ParsedLine] = group(tag)
            val defaultAccTagInMemory = AccTagInMemory(
              tag._1,
              tag._2,
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
            val row = new RowSerializable(8)
                row.setField(0, accTag.tag)
                row.setField(1, accTag.creationMonth)
                row.setField(2, accTag.questions.toInt)
                row.setField(3, accTag.closed_questions.toInt)
                row.setField(4, closingTimeDays)
                row.setField(5, accTag.total_score.toInt)
                row.setField(6, accTag.unique_users.distinct.length)
                row.setField(7, accTag.answerCount.toInt)



//            out.collect( ( tag, window.getEnd, row ) )
            out.collect( row )
          }

          //out.collect( ( tag, window.getEnd, events.map( _.score ).sum ) )
        }
      //cc.print()


    def aggregateData(
      acc: Map[String, AccTagInMemory],
      element: Map[String, AccTagInMemory]): Map[String, AccTagInMemory] = {
      val tag = element.keys.head
      val el = element(tag)
      val x = if (acc.keys.toList.contains(tag)) {
        val accTag: AccTagInMemory = acc(tag)
        acc ++: Map(tag -> AccTagInMemory(
          tag,
          accTag.creationMonth,
          accTag.questions,
          accTag.closed_questions + el.closed_questions,
          accTag.total_time_closing + el.total_time_closing,
          accTag.total_score + el.total_score,
          (accTag.unique_users ++ el.unique_users).distinct,
          accTag.answerCount + el.answerCount
        ))
      } else {
        acc ++: Map(el.tag -> AccTagInMemory(
          el.tag,
          el.creationMonth,
          1L,
          el.closed_questions,
          el.total_time_closing,
          el.total_score,
          el.unique_users,
          el.answerCount
        ))
      }
      //println(acc)
      x
    }
/*
    val parsedLines: DataStream[ParsedLine] =  soLines
      .flatMap(getParsedLine(_))

    val aux: DataStream[Map[String, AccTagInMemory]] =
      parsedLines.map(el => Map(el.tag -> AccTagInMemory(
        el.tag,
        el.creationMonth,
        1L,
        if (el.closed) 1L else 0L,
        if (el.closed) el.timeToClose else 0L,
        el.score,
        List(el.userId),
        el.answerCount
      )))
        .keyBy(_.keys)
        .timeWindow(Time.seconds(1000))
          .trigger(new TestTrigger(descriptor))
        .reduce{(v1, v2) =>
          aggregateData(v1, v2)
        }
*/



/*
    val x: DataStream[RowSerializable] = aux.flatMap{accTag =>
      val lines = accTag.values
      lines.map{line =>
        val row = new RowSerializable(8)
        row.setField(0, line.tag)
        row.setField(1, line.creationMonth.toString)
        row.setField(2, line.questions.toString)
        row.setField(3, line.closed_questions.toString)
        row.setField(4, line.total_time_closing.toString)
        row.setField(5, line.total_score.toString)
        row.setField(6, line.unique_users.distinct.length.toString)
        row.setField(7, line.answerCount.toString)
        row
      }
    }
    //aux.print()
     x.addSink(kuduSink)*/



    cc.addSink(kuduSink)
    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}