package org.apache.flink.quickstart.utils

import org.joda.time.DateTime

/**
  * Created by nacho on 26/05/17.
  */


case class DirtyLine(
  id: String,
  creationDate: String,
  closedDate: String,
  deletionDate: String,
  score: String,
  userId: String,
  answerCount: String,
  tags: List[String]
)
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

case class AccTag(
  tag: String,
  timestamp: DateTime,
  questions: Long,
  closed_questions: Long,
  total_time_closing: Long,
  total_score: Long,
  unique_users: Long,
  answerCount: Long
)

