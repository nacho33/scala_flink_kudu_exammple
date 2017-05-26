package org.apache.flink.quickstart.utils

import org.joda.time.DateTime

/**
  * Created by nacho on 26/05/17.
  */
object ProcessingUtils {

  /*
   * @param input the integer field as String
   * @param the default output in case of NA
   *
   * The int format is given with white spaces and we receive null
   * as NA
   * Return the input as Int
   */
  def cleanInt(input: String, default: Int) = {
    val inputToParsed = input.replaceAll("\\s", "")
    if(inputToParsed == "NA") default else inputToParsed.toInt
  }

  /*
   * @param timestamp the timestamp
   * Return the option of the datetime
   */
  def getOptionDateTime(timestamp: String): Option[DateTime] = {
    val time = timestamp.replaceAll("\\s", "")
    if(time == "NA") None else Some(new DateTime(time))
  }

  /*
   * Return the month following the yyyymm format
   * this way we can sort by month
   */
  def getMonthYear(date: DateTime): String = {
    val stringMonth = date.getMonthOfYear.toString
    val month =
      if(stringMonth.length == 1) "0" + stringMonth
      else stringMonth

    date.getYear.toString + month
  }
}
