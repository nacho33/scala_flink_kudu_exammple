package org.apache.flink.quickstart

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.quickstart.utils.ParsedLine
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}

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

    val month: String = event.creationMonth

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

  }

  override def onEventTime(
    time: Long,
    window: TimeWindow,
    ctx: TriggerContext): TriggerResult = {
    println("onEventTime")
    // trigger final computation
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
