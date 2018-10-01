/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.spark.sql.execution.streaming.state

import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, JoinedRow, Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.streaming.{StatefulOperatorStateInfo, StateStoreWriter}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper._
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.util.{CompletionIterator, SerializableConfiguration}

case class newStreamJoin (
                           leftKeys: Seq[Expression],
                           rightKeys: Seq[Expression],
                           joinType: JoinType,
                           condition: JoinConditionSplitPredicates,
                           stateInfo: Option[StatefulOperatorStateInfo],
                           //  eventTimeWatermark: Option[Long],
                           //  stateWatermarkPredicates: JoinStateWatermarkPredicates,
                           left: SparkPlan,
                           right: SparkPlan)
  extends SparkPlan with BinaryExecNode with StateStoreWriter{



  //  override def stateInfo: Option[StatefulOperatorStateInfo] = ???


  def this(leftKeys: Seq[Expression],
           rightKeys: Seq[Expression],
           joinType: JoinType,
           condition: Option[Expression],
           left: SparkPlan,
           right: SparkPlan) = {
    this(leftKeys, rightKeys, joinType, JoinConditionSplitPredicates(condition, left, right),
      stateInfo = None, left, right)
  }
  require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
    "\n\n\ndatatypes are not of the same type!\n\n\n")

  val nullLeft = new GenericInternalRow(left.output.map(_.withNullability(true)).length)
  val nullRight = new GenericInternalRow(right.output.map(_.withNullability(true)).length)
  private val storeConf = new StateStoreConf(sqlContext.conf)
  private val hadoopConfBcast = sparkContext.broadcast(
    new SerializableConfiguration(SessionState.newHadoopConf(
      sparkContext.hadoopConfiguration, sqlContext.conf)))

  // TODO - not sture if this is correct for this join.
  override def requiredChildDistribution: Seq[Distribution] =
    HashClusteredDistribution(leftKeys, stateInfo.map(_.numPartitions)) ::
      HashClusteredDistribution(rightKeys, stateInfo.map(_.numPartitions)) :: Nil

  override def output: Seq[Attribute] = {
    left.output ++ right.output
    //    case LeftOuter => left.output ++ right.output.map(_.withNullability(true))
    //    case RightOuter => left.output.map(_.withNullability(true)) ++ right.output
    //    case _ => throwBadJoinTypeException()
  }


  protected override def doExecute(): RDD[InternalRow] = {
    val stateStoreCoord = sqlContext.sessionState.streamingQueryManager.stateStoreCoordinator
    val stateStoreNames = SymmetricHashJoinStateManager.allStateStoreNames(LeftSide, RightSide)
    left.execute().stateStoreAwareZipPartitions(
      right.execute(), stateInfo.get, stateStoreNames, stateStoreCoord)(processPartitions)
  }

  private def processPartitions(
                                 leftInputIter: Iterator[InternalRow],
                                 rightInputIter: Iterator[InternalRow]): Iterator[InternalRow] = {
    if (stateInfo.isEmpty) {
      throw new IllegalStateException(s"Cannot execute join as state info was not specified\n$this")
    }

    val numOutputRows = longMetric("numOutputRows")
    val numUpdatedStateRows = longMetric("numUpdatedStateRows")
    val numTotalStateRows = longMetric("numTotalStateRows")
    val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
    val allRemovalsTimeMs = longMetric("allRemovalsTimeMs")
    val commitTimeMs = longMetric("commitTimeMs")
    val stateMemory = longMetric("stateMemory")

    val updateStartTimeNs = System.nanoTime
    val joinedRow = new JoinedRow

    // scalastyle:off println
//    println("\n\n\nIMPRIMINDO CONDITIONS : \n")
//    println(condition.toString() + "\n\n\n")

    // scalastyle:on println

    val postJoinFilter =
      newPredicate(condition.bothSides.getOrElse(Literal(true)), left.output ++ right.output).eval _
    val leftSideJoiner = new OneSideHashJoiner(
      LeftSide, left.output, leftKeys, leftInputIter,
      condition.leftSideOnly, postJoinFilter)
    val rightSideJoiner = new OneSideHashJoiner(
      RightSide, right.output, rightKeys, rightInputIter,
      condition.rightSideOnly, postJoinFilter)

    //  Join one side input using the other side's buffered/state rows. Here is how it is done.
    //
    //  - `leftJoiner.joinWith(rightJoiner)` generates all rows from matching new left input with
    //    stored right input, and also stores all the left input
    //
    //  - `rightJoiner.joinWith(leftJoiner)` generates all rows from matching new right input with
    //    stored left input, and also stores all the right input. It also generates all rows from
    //    matching new left input with new right input, since the new left input has become stored
    //    by that point. This tiny asymmetry is necessary to avoid duplication.
    val leftOutputIter = leftSideJoiner.storeAndJoinWithOtherSide(rightSideJoiner) {
      (input: InternalRow, matched: InternalRow) => joinedRow.withLeft(input).withRight(matched)
    }
    val rightOutputIter = rightSideJoiner.storeAndJoinWithOtherSide(leftSideJoiner) {
      (input: InternalRow, matched: InternalRow) => joinedRow.withLeft(matched).withRight(input)
    }

    // We need to save the time that the inner join output iterator completes, since outer join
    // output counts as both update and removal time.
    var innerOutputCompletionTimeNs: Long = 0
    def onInnerOutputCompletion = {
      innerOutputCompletionTimeNs = System.nanoTime
    }
    // This is the iterator which produces the inner join rows. For outer joins, this will be
    // prepended to a second iterator producing outer join rows; for inner joins, this is the full
    // output.
    val innerOutputIter = CompletionIterator[InternalRow, Iterator[InternalRow]](
      (leftOutputIter ++ rightOutputIter), onInnerOutputCompletion)


    val outputIter: Iterator[InternalRow] = innerOutputIter


    val outputProjection = UnsafeProjection.create(left.output ++ right.output, output)
    val outputIterWithMetrics = outputIter.map { row =>
      numOutputRows += 1
      outputProjection(row)
    }

    // Function to remove old state after all the input has been consumed and output generated
    def onOutputCompletion = {
      // All processing time counts as update time.
      allUpdatesTimeMs += math.max(NANOSECONDS.toMillis(System.nanoTime - updateStartTimeNs), 0)

      // Processing time between inner output completion and here comes from the outer portion of a
      // join, and thus counts as removal time as we remove old state from one side while iterating.
      if (innerOutputCompletionTimeNs != 0) {
        allRemovalsTimeMs +=
          math.max(NANOSECONDS.toMillis(System.nanoTime - innerOutputCompletionTimeNs), 0)
      }

      allRemovalsTimeMs += timeTakenMs {
        // Remove any remaining state rows which aren't needed because they're below the watermark.
        //
        // For inner joins, we have to remove unnecessary state rows from both sides if possible.
        // For outer joins, we have already removed unnecessary state rows from the outer side
        // (e.g., left side for left outer join) while generating the outer "null" outputs. Now, we
        // have to remove unnecessary state rows from the other side (e.g., right side for the left
        // outer join) if possible. In all cases, nothing needs to be outputted, hence the removal
        // needs to be done greedily by immediately consuming the returned iterator.
        val cleanupIter = leftSideJoiner.removeOldState() ++ rightSideJoiner.removeOldState()
        while (cleanupIter.hasNext) {
          cleanupIter.next()
        }
      }

      // Commit all state changes and update state store metrics
      commitTimeMs += timeTakenMs {
        val leftSideMetrics = leftSideJoiner.commitStateAndGetMetrics()
        val rightSideMetrics = rightSideJoiner.commitStateAndGetMetrics()
        val combinedMetrics = StateStoreMetrics.combine(Seq(leftSideMetrics, rightSideMetrics))

        // Update SQL metrics
        numUpdatedStateRows +=
          (leftSideJoiner.numUpdatedStateRows + rightSideJoiner.numUpdatedStateRows)
        numTotalStateRows += combinedMetrics.numKeys
        stateMemory += combinedMetrics.memoryUsedBytes
        combinedMetrics.customMetrics.foreach { case (metric, value) =>
          longMetric(metric.name) += value
        }
      }
    }

    CompletionIterator[InternalRow, Iterator[InternalRow]](
      outputIterWithMetrics, onOutputCompletion)
  }

  private class OneSideHashJoiner(
                                   joinSide: JoinSide,
                                   inputAttributes: Seq[Attribute],
                                   joinKeys: Seq[Expression],
                                   inputIter: Iterator[InternalRow],
                                   preJoinFilterExpr: Option[Expression],
                                   postJoinFilter: (InternalRow) => Boolean
                                   //  stateWatermarkPredicate: Option[JoinStateWatermarkPredicate]
                                 )
  {

    // Filter the joined rows based on the given condition.
    val preJoinFilter =
      newPredicate(preJoinFilterExpr.getOrElse(Literal(true)), inputAttributes).eval _

    private val joinStateManager = new SymmetricHashJoinStateManager(
      joinSide, inputAttributes, joinKeys, stateInfo, storeConf, hadoopConfBcast.value.value)
    private[this] val keyGenerator = UnsafeProjection.create(joinKeys, inputAttributes)

    private[this] var updatedStateRowsCount = 0


    def storeAndJoinWithOtherSide(
                                   otherSideJoiner: OneSideHashJoiner)(
                                   generateJoinedRow: (InternalRow, InternalRow) => JoinedRow):
    Iterator[InternalRow] = {

      val nonLateRows = inputIter
      // scalastyle:off println

      nonLateRows.flatMap { row =>
        val thisRow = row.asInstanceOf[UnsafeRow]
        // If this row fails the pre join filter, that means it can never satisfy the full join
        // condition no matter what other side row it's matched with. This allows us to avoid
        // adding it to the state, and generate an outer join row immediately (or do nothing in
        // the case of inner join).
//        if (preJoinFilter(thisRow)) {//TODO - COLOCAR ESSE FILTRO NOVAMENTE ???
          val key = keyGenerator(thisRow)
          val outputIter = otherSideJoiner.joinStateManager
             .getAllKeys.map { thatRow =>
//            println("This Row " + thisRow.toString + "\t\tThat row:" + thatRow.toString)
            // scalastyle:on println

            generateJoinedRow(thisRow, thatRow)
          }.filter(postJoinFilter) // TODO - APLICAR O FILTRO NOVAMENTE ???
          joinStateManager.append(key, thisRow)
          updatedStateRowsCount += 1

          outputIter
//        } else {
//          joinSide match {
//            case LeftSide if joinType == LeftOuter =>
//              Iterator(generateJoinedRow(thisRow, nullRight))
//            case RightSide if joinType == RightOuter =>
//              Iterator(generateJoinedRow(thisRow, nullLeft))
//            case _ => Iterator()
//          }
//        }
      }
    }

    def get(key: UnsafeRow): Iterator[UnsafeRow] = {
      joinStateManager.get(key)
    }


    def commitStateAndGetMetrics(): StateStoreMetrics = {
      joinStateManager.commit()
      joinStateManager.metrics
    }

    def numUpdatedStateRows: Long = updatedStateRowsCount

    def removeOldState(): Iterator[UnsafeRowPair] = {
      Iterator.empty

    }

  }
}

