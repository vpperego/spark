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

package org.apache.spark.sql.streaming

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.StateStore


class NewStreamingJoinSuite extends StreamTest with StateStoreMetricsTest with BeforeAndAfter {

  before {
    SparkSession.setActiveSession(spark)  // set this before force initializing 'joinExec'
    spark.streams.stateStoreCoordinator   // initialize the lazy coordinator
  }

  import testImplicits._


//  test("stream stream inner join on non-time column") {
//    val input1 = MemoryStream[Int]
//    val input2 = MemoryStream[Int]
//
//    val df1 = input1.toDF.select('value as "key", ('value * 2) as "leftValue")
//    val df2 = input2.toDF.select('value as "key", ('value * 3) as "rightValue")
//    val joined = df1.join(df2, "key")
//
//    testStream(joined)(
//      AddData(input1, 1),
//      CheckAnswer(),
//      AddData(input2, 1, 10),       // 1 arrived on input1 first, then input2, should join
//      CheckNewAnswer((1, 2, 3)),
//      AddData(input1, 10),          // 10 arrived on input2 first, then input1, should join
//      CheckNewAnswer((10, 20, 30)),
//      AddData(input2, 1),           // another 1 in input2 should join with 1 input1
//      CheckNewAnswer((1, 2, 3)),
//      StopStream,
//      StartStream(),
//      AddData(input1, 1), // multiple 1s should be kept in state causing multiple (1, 2, 3)
//      CheckNewAnswer((1, 2, 3), (1, 2, 3)),
//      StopStream,
//      StartStream(),
//      AddData(input1, 100),
//      AddData(input2, 100),
//      CheckNewAnswer((100, 200, 300))
//    )
//  }


  test("first test") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    val df1 = input1.toDF.select('value as "leftKey")
    val df2 = input2.toDF.select('value as "rightKey")

    // scalastyle:off
    println("\n\n\n printing schema \n\n\n")
    // scalastyle:on

    df1.printSchema()
    df2.printSchema()

    val joined = df1.join(df2, $"leftKey" < $"rightKey")

    testStream(joined)(
      AddData(input1, 1),
      CheckAnswer(),
      AddData(input2, 3),       // 1 arrived on input1 first, then input2, should join
      CheckNewAnswer((1, 3))

    )
  }

  after {
    StateStore.stop()
  }

}
