package org.apache.flink.quickstart.utils

import org.joda.time.DateTime

/**
  * Created by nacho on 26/05/17.
  */

case class SoLine(
  id: String,
  creationDate: DateTime,
  closedDate: Option[DateTime],
  deletionDate: Option[DateTime],
  score: Int,
  userId: Int,
  answerCount: Int,
  tags: List[String]
)

case class ParsedLine(
  environment: String,
  tag: String,
  creationMonth: String,
  timeToClose: Long,
  closed: Boolean,
  score: Int,
  userId: Int,
  answerCount: Int
) {
  override def toString = tag
}

case class AccTagInMemory(
  tag: String,
  creationMonth: String,
  questions: Long,
  closed_questions: Long,
  total_time_closing: Long,
  total_score: Long,
  unique_users: List[Int],
  answerCount: Long
)


