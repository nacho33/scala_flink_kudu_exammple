package org.apache.flink.quickstart

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}

/**
  * Created by nacho on 2/05/17.
  */
class MonthTrigger[W <: TimeWindow](descriptor2: ValueStateDescriptor[Integer]) extends Trigger[ParsedLine, TimeWindow] {

  override def onElement(
    event: ParsedLine,
    timestamp: Long,
    window: TimeWindow,
    ctx: TriggerContext): TriggerResult = {

    val state = ctx.getPartitionedState(StreamingJob.descriptor)

    val month = event.creationMonth
    val monthState: String = {
      try{
        val stringMonth = state.value.toString
        if(stringMonth.length == 5) "0" + stringMonth
        else stringMonth
      } catch {
        case e =>
          println("PRIMER")
          state.update(month.toInt)
          month
      }

    }




    if(monthState == month)
      TriggerResult.CONTINUE
    else{

      TriggerResult.FIRE_AND_PURGE
    }


  }

  override def onEventTime(
    time: Long,
    window: TimeWindow,
    ctx: TriggerContext): TriggerResult = {

    // trigger final computation
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(
    time: Long,
    window: TimeWindow,
    ctx: TriggerContext): TriggerResult = {

    throw new UnsupportedOperationException("I am not a processing time trigger")
  }

  override def clear(
    window: TimeWindow,
    ctx: TriggerContext
  ) = {
    throw new UnsupportedOperationException("I am not a processing clear")
  }

}
