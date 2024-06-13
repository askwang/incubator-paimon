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

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD

/** scala use. */
class AskwangScalaITCase extends PaimonSparkTestBase {

  /** 读取分区内元素 */
  test("spark partitionBy function") {

    val sc = spark.sparkContext
    val rdd = sc.makeRDD(Seq((1, "A"), (2, "B"), (3, "C"), (4, "D")), 2)

    println("------init data-----")
    getMapPartitionsResult(rdd).foreach(println)

    println("-----after partitionBy-----")
    val partitionedRdd = rdd.partitionBy(new HashPartitioner(4))
    getMapPartitionsResult(partitionedRdd).foreach(println)
  }

  class CustomPartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = key.asInstanceOf[Int] % partitions
  }

  // 读取rdd每个partition数据
  def getMapPartitionsResult(rdd: RDD[(Int, String)]): Array[(String, List[(Int, String)])] = {
    val part_map = scala.collection.mutable.Map[String, List[(Int, String)]]()
    rdd
      .mapPartitionsWithIndex {
        (partIdx, iter) =>
          {
            while (iter.hasNext) {
              val part_name = "part_" + partIdx
              val elem = iter.next()
              if (part_map.contains(part_name)) {
                var elems = part_map(part_name)
                elems ::= elem
                part_map(part_name) = elems
              } else {
                part_map(part_name) = List[(Int, String)](elem)
              }
            }
            part_map.iterator
          }
      }
      .collect()
  }
}
