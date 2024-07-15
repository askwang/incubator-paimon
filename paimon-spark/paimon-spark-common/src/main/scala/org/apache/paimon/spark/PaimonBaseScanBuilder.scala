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

package org.apache.paimon.spark

import org.apache.paimon.predicate.{PartitionPredicateVisitor, Predicate}
import org.apache.paimon.table.Table

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

abstract class PaimonBaseScanBuilder(table: Table)
  extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with Logging {

  protected var requiredSchema: StructType = SparkTypeUtils.fromPaimonRowType(table.rowType())

  protected var pushed: Array[(Filter, Predicate)] = Array.empty

  protected var reservedFilters: Array[Filter] = Array.empty

  protected var pushDownLimit: Option[Int] = None

  override def build(): Scan = {
    PaimonScan(table, requiredSchema, pushed.map(_._2), reservedFilters, pushDownLimit)
  }

  /**
   * Pushes down filters, and returns filters that need to be evaluated after scanning. <p> Rows
   * should be returned from the data source if and only if all of the filters match. That is,
   * filters must be interpreted as ANDed together.
   *
   * <p>spark-sql INFO V2ScanRelationPushDown: Pushing operators to z_paimon_pk_table Pushed
   * Filters: IsNotNull(dt), EqualTo(dt,2024-04-11T03:01:00Z) Post-Scan Filters:
   * isnotnull(dt#3),(dt#3 = 2024-04-11 11:01:00) spark 侧输出的 Post-scan Filter 没问题是 spark 进行了 filter
   * 的 translateFilterWithMapping， 之后又进行了 rebuildExpressionFromFilter 还原
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // spark 传进来的 filter: Array[Filter] 就是转换为 utc 类型的.  EqualTo(dt,2024-04-11T03:01:00Z)
    // 相当于 paimon 是基于错误的 filter 进行转换的
    val pushable = mutable.ArrayBuffer.empty[(Filter, Predicate)]
    val postScan = mutable.ArrayBuffer.empty[Filter]
    val reserved = mutable.ArrayBuffer.empty[Filter]

    val converter = new SparkFilterConverter(table.rowType)
    val visitor = new PartitionPredicateVisitor(table.partitionKeys())
    filters.foreach {
      filter =>
        val predicate = converter.convertIgnoreFailure(filter)
        if (predicate == null) {
          postScan.append(filter)
        } else {
          pushable.append((filter, predicate))
          if (predicate.visit(visitor)) {
            reserved.append(filter)
          } else {
            postScan.append(filter)
          }
        }
    }

    if (pushable.nonEmpty) {
      this.pushed = pushable.toArray
    }
    if (reserved.nonEmpty) {
      this.reservedFilters = reserved.toArray
    }
    postScan.toArray
  }

  // 这里只是接口实现，将结果返回给 Spark Pushed Filters
  override def pushedFilters(): Array[Filter] = {
    pushed.map(_._1)
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }
}
