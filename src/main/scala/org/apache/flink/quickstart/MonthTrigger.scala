package org.apache.flink.quickstart

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.quickstart.utils.ParsedLine
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}

object MonthState {
  var month = "200807"
  val r = scala.util.Random
}
/**
  * Created by nacho on 2/05/17.
  */
class MonthTrigger[W <: TimeWindow](descriptor2: ValueStateDescriptor[Integer]) extends Trigger[ParsedLine, TimeWindow] {
  var m = ""


  override def onElement(
    event: ParsedLine,
    timestamp: Long,
    window: TimeWindow,
    ctx: TriggerContext): TriggerResult = {

    //val state = ctx.getPartitionedState(StreamingJob.descriptor)
//    println("stringMonth: " + state.value)
    val month: String = event.creationMonth
    /*val monthState: String = {
      try{
        val stringMonth = state.value.toString
        if(stringMonth.length == 5) "0" + stringMonth
        else stringMonth
      } catch {
        case e =>
          println("PRIMER: month=" + month)
          state.update(month.toInt)
          MonthState.month = month
          month
      }

    }
*/
  //  println("month: " + month + " -- monthState: " + MonthState.month)

    if(m == ""){
      m = month
      TriggerResult.CONTINUE
    }else if(m == month) {
      //if(monthState == month) {
      TriggerResult.CONTINUE
    }else{
      println("fire and purge: " + month)
      m = month
      TriggerResult.FIRE_AND_PURGE
    }
    /*
    if(MonthState.r.nextInt(1000) > 3) {
      //if(monthState == month) {
      TriggerResult.CONTINUE
    }else{
      println("fire and purge: " + month)
      m = month
      TriggerResult.FIRE_AND_PURGE
    }
*/

  }

  override def onEventTime(
    time: Long,
    window: TimeWindow,
    ctx: TriggerContext): TriggerResult = {
    println("onEventTime")
    // trigger final computation
//    TriggerResult.FIRE_AND_PURGE
    TriggerResult.CONTINUE
    //throw new UnsupportedOperationException("I am not a processing time trigger")
  }

  override def onProcessingTime(
    time: Long,
    window: TimeWindow,
    ctx: TriggerContext): TriggerResult = {
    println("onProcessingTime")
    TriggerResult.CONTINUE
    //throw new UnsupportedOperationException("I am not a processing time trigger")
  }

  override def clear(
    window: TimeWindow,
    ctx: TriggerContext
  ) = {
    println("clear")
    TriggerResult.CONTINUE
    //throw new UnsupportedOperationException("I am not a processing clear")
  }

}
