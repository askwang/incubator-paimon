/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.utils.SnapshotManager
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest

import java.sql.Timestamp

class ExpireSnapshotsProcedureTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon Procedure: expire snapshots") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // define a change-log table and test `forEachBatch` api
          spark.sql(
            s"""
               |CREATE TABLE T (a INT, b STRING)
               |TBLPROPERTIES ('primary-key'='a', 'bucket'='3')
               |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, String)]
          val stream = inputData
            .toDS()
            .toDF("a", "b")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T ORDER BY a")

          try {
            // snapshot-1
            inputData.addData((1, "a"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Nil)

            // snapshot-2
            inputData.addData((2, "b"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Nil)

            // snapshot-3
            inputData.addData((2, "b2"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b2") :: Nil)

            // expire
            checkAnswer(
              spark.sql("CALL paimon.sys.expire_snapshots(table => 'test.T', retain_max => 2)"),
              Row(1) :: Nil)

            checkAnswer(
              spark.sql("SELECT snapshot_id FROM paimon.test.`T$snapshots`"),
              Row(2L) :: Row(3L) :: Nil)
          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: expire snapshots retainMax retainMin value check") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // define a change-log table and test `forEachBatch` api
          spark.sql(
            s"""
               |CREATE TABLE T (a INT, b STRING)
               |TBLPROPERTIES ('primary-key'='a', 'bucket'='3')
               |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, String)]
          val stream = inputData
            .toDS()
            .toDF("a", "b")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T ORDER BY a")

          try {
            // snapshot-1
            inputData.addData((1, "a"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Nil)

            // snapshot-2
            inputData.addData((2, "b"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Nil)

            // snapshot-3
            inputData.addData((2, "b2"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b2") :: Nil)

            // expire assert throw exception
            assertThrows[IllegalArgumentException] {
              spark.sql(
                "CALL paimon.sys.expire_snapshots(table => 'test.T', retain_max => 2, retain_min => 3)")
            }
          } finally {
            stream.stop()
          }
      }
    }
  }

  // min maybe large than maxExclusive
  test("Paimon Procedure: max delete") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          // define a change-log table and test `forEachBatch` api
          spark.sql(
            s"""
               |CREATE TABLE T (a INT, b STRING)
               |TBLPROPERTIES ('primary-key'='a', 'bucket'='3', 'write-only'='true')
               |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(Int, String)]
          val stream = inputData
            .toDS()
            .toDF("a", "b")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T ORDER BY a")

          try {
            // snapshot-1
            inputData.addData((1, "a"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Nil)

            // snapshot-2
            inputData.addData((2, "b"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b") :: Nil)

            // snapshot-3
            inputData.addData((2, "b2"))
            stream.processAllAvailable()
            checkAnswer(query(), Row(1, "a") :: Row(2, "b2") :: Nil)

            // snapshot-4
            inputData.addData((4, "b"))
            stream.processAllAvailable()

            // snapshot-5
            inputData.addData((5, "b"))
            stream.processAllAvailable()

            // snapshot-6
            inputData.addData((6, "b"))
            stream.processAllAvailable()

            // snapshot-7
            inputData.addData((7, "b"))
            stream.processAllAvailable()

            // snapshot-8
            inputData.addData((8, "b"))
            stream.processAllAvailable()

            // snapshot-9
            inputData.addData((9, "b"))
            stream.processAllAvailable()

            // snapshot-10
            inputData.addData((10, "b"))
            stream.processAllAvailable()

            spark.sql(
              "CALL paimon.sys.expire_snapshots(table => 'test.T', retain_max => 5, retain_min => 2, max_deletes => 4)")
          } finally {
            stream.stop()
          }
      }
    }
  }

  // ExpireSnapshotsProcedureITCase. test没通过.
  test("copy ExpireSnapshotsProcedureITCase") {
    withTable("word_count") {
      withSQLConf() {
        // spark.sql("SET TIME ZONE 'Asia/Shanghai';")
        spark.sql(
          s"CREATE TABLE word_count ( word STRING, cnt INT) using paimon " +
            s"TBLPROPERTIES ('primary-key' = 'word', 'file.format'='parquet', 'num-sorted-run.compaction-trigger' = '9999')")

        val table = loadTable("word_count")
        val snapshotManager = table.snapshotManager()

        // initially prepare 6 snapshots, expected snapshots (1, 2, 3, 4, 5, 6)
        for (i <- 0 until 6) {
          sql("INSERT INTO word_count VALUES ('" + String.valueOf(i) + "', " + i + ")")
        }

        sql("select * from word_count").show

        checkSnapshots(snapshotManager, 1, 6)

        // retain_max => 5, expected snapshots (2, 3, 4, 5, 6)
        sql("CALL sys.expire_snapshots(`table` => 'word_count', retain_max => 5)")
        checkSnapshots(snapshotManager, 2, 6)

        // askwang-todo: ts6 作为 procedure 参数一直不通过（如何传递 timestampType 类型）
        val ts6 = new Timestamp(snapshotManager.latestSnapshot.timeMillis)
        println("current time: " + System.currentTimeMillis())
        println("ts6 : " + ts6)
        // older_than => timestamp of snapshot 6, max_deletes => 1, expected snapshots (3, 4, 5, 6)
        // earliest=2, min=min(max, 2)=2, max=max(6-1+1, 2+1)=3，所以删除snapshot-2
        sql("CALL sys.expire_snapshots(`table` => 'word_count', older_than => '" + ts6 + "', max_deletes => 1)")
        checkSnapshots(snapshotManager, 3, 6)

        // older_than => timestamp of snapshot 6, retain_min => 3, expected snapshots (4, 5, 6)
        // earliest=3, min=min(max,3)=3, max=min(6-3+1,max)=4，所以删除 snapshot-3
        sql("CALL sys.expire_snapshots(`table` => 'word_count', older_than => '" + ts6 + "', retain_min => 3)")
        checkSnapshots(snapshotManager, 4, 6)

        // older_than => timestamp of snapshot 6, expected snapshots (6)
        // min=min(max,4)=4, max=min(6-1+1, max)=6，所以删除 snapshot-4和snapshot-5
        sql("CALL sys.expire_snapshots(`table`  => 'word_count', older_than => '" + ts6 + "')")
        checkSnapshots(snapshotManager, 6, 6)
      }
    }
  }

  def checkSnapshots(sm: SnapshotManager, earliest: Int, lastest: Int): Unit = {
    assert(sm.snapshotCount() == (lastest - earliest + 1))
    assert(sm.earliestSnapshotId() == earliest)
    assert(sm.latestSnapshotId() == lastest)
  }
}
