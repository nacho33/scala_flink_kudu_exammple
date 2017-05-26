package org.apache.flink.quickstart.utils

import org.apache.kudu.client.{CreateTableOptions, KuduClient}
import org.apache.kudu.{ColumnSchema, Schema, Type}

/**
  * Created by nacho on 26/05/17.
  */
object CustomKuduUtils {

  val aggTagColumns = List(
    "tag",
    "creationMonth",
    "questions",
    "closed_questions",
    "total_time_closing",
    "total_score",
    "unique_users",
    "answerCount")

  val tupleKuduColumns: List[(String, String)] =
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

  /*
   * Create Kudu table. We specify the type of each column
   */
  def createTable(tableName: String, client: KuduClient) = {
    try{
      val columns: List[ColumnSchema] = tupleKuduColumns.map { column =>
        if (column._2 == "INT") {
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

        false
    }
  }

}
