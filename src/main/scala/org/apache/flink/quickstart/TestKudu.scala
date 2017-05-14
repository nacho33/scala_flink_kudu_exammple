package org.apache.flink.quickstart

/**
  * Created by nacho on 5/05/17.
  */

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
import org.apache.flink.streaming.api.datastream.{DataStream}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kudu.client.{CreateTableOptions, KuduClient}
import org.joda.time.DateTime
import org.apache.kudu.{ColumnSchema, Type}
import org.apache.kudu.Schema;

//import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}

import scala.runtime.ScalaRunTime

object TestKudu {
  val descriptor = new ValueStateDescriptor[Integer]("month", Integer.TYPE)

  def createTable(tableName: String, client: KuduClient) = {
    try{
      val columns2: List[(String, String)] =
        /*List(
          ("id", "INT"),
          ("creationDate", "STRING"),
          ("closedDate", "STRING"),
          ("deletionDate", "STRING"),
          ("score", "INT"),
          ("userId", "STRING"),
          ("answerCount", "INT"),
          ("tags", "STRING"))*/
        List(
          ("id", "STRING"),
          ("userId", "STRING"))
      val columns: List[ColumnSchema] = columns2.map{ column =>
        if(column._2 == "INT") {
          new ColumnSchema.ColumnSchemaBuilder(column._1, Type.INT32)
            .key(true)
            .build()
        }else{
          new ColumnSchema.ColumnSchemaBuilder(column._1, Type.STRING)
            .key(true)
            .build()
        }
      }


      // Para particionado
      val rangeKeys: List[String] = List("id")

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
    val KUDU_MASTER = "172.17.0.2"
    val DEST_TABLE = "test_7"
    val client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build()


    // if(!client.tableExists(DEST_TABLE))
    //   client.createTable(DEST_TABLE)
    /*if(!client.tableExists(DEST_TABLE)) {
      println(s"Creating table: $DEST_TABLE")
      createTable(DEST_TABLE, client)
    }
*/
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val columns2: List[String] = List("id", "userId")
    val stream: DataStream[String] = env.fromElements("data1 data2");
   // val a = new TestUtils
    //a.doJob(stream, KUDU_MASTER, DEST_TABLE, columns2.toArray)
    /*val stream2: DataStream[RowSerializable] = stream.map{ line =>
      val row = new RowSerializable(3)
      row.setField(0, "a")
      row.setField(1, "b")
      row.setField(2, "c")
      row
    }*/

    //val stream2: DataStream[String] = stream.map( line => line )
     /* val row = new RowSerializable(3)
      row.setField(0, "a")
      row.setField(1, "b")
      row.setField(2, "c")
      row
    }*/

   // stream2.addSink(new KuduSink(KUDU_MASTER, DEST_TABLE, columns2.toArray))
    env.execute();

    // configure event-time characteristics
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // generate a Watermark every second
    //env.getConfig.setAutoWatermarkInterval(1000);

    /*val props: Properties = new Properties
    props.setProperty("zookeeper.connect", "localhost:2181"); // Zookeeper default host:port
    props.setProperty("bootstrap.servers", "localhost:9092"); // Broker default host:port
    props.setProperty("group.id", "myGroup");                 // Consumer group ID
    props.setProperty("auto.offset.reset", "earliest");       // Always read topic from start
*/
    // create a Kafka consumer
    /*val consumer =
      new FlinkKafkaConsumer010[String](
        "stack_topic",
        new SimpleStringSchema(),
        props)*/

    // create a Kafka source
   // val lines: DataStream[String] = env.addSource(consumer)
    // val formatter: DateTimeFormatter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.getDefault())

    /*def cleanInt(input: String, default: Int) = {
      val inputToParsed = input.replaceAll("\\s", "")
      if(inputToParsed == "NA") default else inputToParsed.toInt
    }

    def getOptionDateTime(timestamp: String): Option[DateTime] = {
      val time = timestamp.replaceAll("\\s", "")
      if(time == "NA") None else Some(new DateTime(time))

    }*/
    /*val soLines: DataStream[SoLine] = lines.map{ line =>
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
    }*/

    /*def getMonthYear(date: DateTime): String = {
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

*/


   /* val parsedLines: WindowedStream[ParsedLine, String, TimeWindow] =
      soLines
        .flatMap(getParsedLine(_))
        .keyBy(_.tag)
        .timeWindow(Time.seconds(5))
        //.timeWindow(Time.days(32))
        .trigger(new MonthTrigger(descriptor))


*/


    //val columnNames = List("1", "2")
    //val columns: Array[String] = Array[String]("idd", "creationDate", "closedDate", "deletionDate", "score", "userId", "answerCount", "tags")
    //val columns2: List[String] = List("id", "creationDate", "closedDate", "deletionDate", "score", "userId", "answerCount", "tags")


    //soLines.print

    //soLines.addSink(new KuduSink(KUDU_MASTER, DEST_TABLE, columnNames))
    /*val kuduSink: KuduSink = new KuduSink(KUDU_MASTER, DEST_TABLE, columns2.toArray)
    //lines.addSink(new KuduSink(KUDU_MASTER, DEST_TABLE, columns))
    var a = 0
    val b: DataStream[RowSerializable] = lines.map{line =>
      val row = new RowSerializable(2)
      row.setField(0, a)
      row.setField(1, "a")
      a = a + 1
      row
    }
    b.print
    b.addSink(kuduSink)*/





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
    //env.execute("Flink Streaming Scala API Skeleton")
  }
}