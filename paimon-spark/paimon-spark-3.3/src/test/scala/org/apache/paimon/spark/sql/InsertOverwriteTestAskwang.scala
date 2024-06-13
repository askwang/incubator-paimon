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

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

class InsertOverwriteTestAskwang extends PaimonSparkTestBase {

  val hasPk = true
  val bucket = 1

  test(s"insert overwrite non-partitioned table: hasPk: $hasPk, bucket: $bucket") {
    val prop = if (hasPk) {
      s"'primary-key'='a,b', 'bucket' = '$bucket' "
    } else if (bucket != -1) {
      s"'bucket-key'='a,b', 'bucket' = '$bucket' "
    } else {
      "'write-only'='true'"
    }

    spark.sql(s"""
                 |CREATE TABLE T (a INT, b INT, c STRING)
                 |TBLPROPERTIES ($prop)
                 |""".stripMargin)

    spark.sql("INSERT INTO T values (1, 1, '1'), (2, 2, '2')")
    checkAnswer(spark.sql("SELECT * FROM T ORDER BY a, b"), Row(1, 1, "1") :: Row(2, 2, "2") :: Nil)

    spark.sql("INSERT OVERWRITE T VALUES (1, 3, '3'), (2, 4, '4')");
    checkAnswer(spark.sql("SELECT * FROM T ORDER BY a, b"), Row(1, 3, "3") :: Row(2, 4, "4") :: Nil)
  }

}
