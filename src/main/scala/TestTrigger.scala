package org.apache.flink.quickstart

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.quickstart.utils.AccTagInMemory
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}

object TestState {
  var month = "072008"
}
/**
  * Created by nacho on 2/05/17.
  */
class TestTrigger[W <: TimeWindow](descriptor2: ValueStateDescriptor[Integer]) extends Trigger[Map[String, AccTagInMemory], TimeWindow] {
  var m = ""
  override def onElement(
    event: Map[String, AccTagInMemory],
    timestamp: Long,
    window: TimeWindow,
    ctx: TriggerContext): TriggerResult = {

    val state = ctx.getPartitionedState(StackOverflowStreamingJob.descriptor)
    //    println("stringMonth: " + state.value)
    val month: Long = event.values.head.total_score
    println("int: " + month)


    if(month % 10 == 0) {
      //if(monthState == month) {
      println("fire and purg")
      TriggerResult.FIRE_AND_PURGE
    }else{
      println("continue")

      TriggerResult.CONTINUE
    }



  }

  override def onEventTime(
    time: Long,
    window: TimeWindow,
    ctx: TriggerContext): TriggerResult = {
    println("onEventTime")
    // trigger final computation
    //    TriggerResult.FIRE_AND_PURGE
    throw new UnsupportedOperationException("I am not a processing time trigger")
  }

  override def onProcessingTime(
    time: Long,
    window: TimeWindow,
    ctx: TriggerContext): TriggerResult = {
    println("onProcessingTime")

    throw new UnsupportedOperationException("I am not a processing time trigger")
  }

  override def clear(
    window: TimeWindow,
    ctx: TriggerContext
  ) = {
    println("clear")

    throw new UnsupportedOperationException("I am not a processing clear")
  }

}
