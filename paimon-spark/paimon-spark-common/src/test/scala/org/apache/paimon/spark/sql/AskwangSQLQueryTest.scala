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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.internal.SQLConf

/**
 * @author
 *   askwang
 * @date
 *   2024/7/6
 */
class AskwangSQLQueryTest extends PaimonSparkTestBase {

  test("sql query with filter timestamp") {
    withTable("tb") {
      spark.sql("SET TIME ZONE 'Asia/Shanghai';")
      // spark.conf.set("spark.sql.planChangeLog.level", "INFO")
      spark.conf.set("spark.sql.datetime.java8API.enabled", "true")
      println("version: " + sparkVersion)
      spark.sql(
        s"CREATE TABLE tb (id INT, dt TIMESTAMP) using paimon TBLPROPERTIES ('file.format'='parquet')")
      val ds = sql("INSERT INTO `tb` VALUES (1,cast(\"2024-04-11 11:01:00\" as Timestamp))")
      val data = sql("SELECT * FROM `tb` where dt ='2024-04-11 11:01:00' ")
      println(spark.conf.get("spark.sql.session.timeZone"))
      println(data.show())
      println(data.explain(true))
    }
  }

  // int/long type pk field insert null failed. string is ok.
  // new version fix this, pk filed check should not null.
  test("writ pk table with pk null long/int type") {
    withTable("tb") {
      withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "false") {
        spark.sql(
          s"CREATE TABLE tb (id long, dt string) " +
            s"using paimon " +
            s"TBLPROPERTIES ('file.format'='parquet', 'primary-key'='id', 'bucket'='1')")
        //      val ds = sql("INSERT INTO `tb` VALUES (cast(NULL as long),cast(NULL as string))")

        val query2 = "INSERT INTO `tb` VALUES (cast(NULL as long),cast(NULL as string))"
        val query = "INSERT INTO `tb` VALUES (NULL, NULL)"

        explainPlan(query, spark)
      }
    }
  }

  def explainPlan(query: String, spark: SparkSession) = {
    val (parser, analyzer, optimizer, planner) = analysisEntry(spark)
    val parsedPlan = parser.parsePlan(query)
    val analyzedPlan = analyzer.execute(parsedPlan)
    val optimizedPlan = optimizer.execute(analyzedPlan)
    val sparkPlan = planner.plan(optimizedPlan).next()
    println("[askwang] ================parsedPlan===================")
    println(parsedPlan)
    println("[askwang] ================analyzedPlan===================")
    println(analyzedPlan)
    println("[askwang] ================optimizedPlan===================")
    println(optimizedPlan)
    println("[askwang] ================sparkPlan===================")
    println(sparkPlan)
  }

  def analysisEntry(spark: SparkSession): (ParserInterface, Analyzer, Optimizer, SparkPlanner) = {
    val parser = spark.sessionState.sqlParser
    val analyzer = spark.sessionState.analyzer
    val optimizer = spark.sessionState.optimizer
    val planner = spark.sessionState.planner
    (parser, analyzer, optimizer, planner)
  }

  def showQueryExecutionPlanInfo(analyzedDF: DataFrame): Unit = {
    val ana = analyzedDF.queryExecution.analyzed
    println("== Analyzed Logical Plan ==")
    println(ana)
    // println( ana.prettyJson)
    println("== Optimized Logical Plan ==")
    val opt = analyzedDF.queryExecution.optimizedPlan
    println(opt)
    // println( opt.prettyJson)
    println("== Physical Plan ==")
    println(analyzedDF.queryExecution.sparkPlan)
    println("== executedPlan ==")
    println(analyzedDF.queryExecution.executedPlan)
  }

}
