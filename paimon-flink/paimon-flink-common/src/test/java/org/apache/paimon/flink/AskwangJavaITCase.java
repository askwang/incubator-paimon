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

package org.apache.paimon.flink;

import org.apache.paimon.fs.Path;

import org.junit.jupiter.api.Test;

import java.util.function.BinaryOperator;

/** java use. */
public class AskwangJavaITCase {

    @Test
    public void testBinaryOperator() {
        BinaryOperator<Integer> binaryOperator1 = (m, n) -> m + n;
        BinaryOperator<Integer> binaryOperator2 = Integer::sum;
        Integer sum = binaryOperator2.apply(3, 5);
        assert (sum == 8);
    }

    @Test
    public void testPathGetName() {
        Path path = new Path("hdfs://ns/warehouse/db/tb/snapshot/snapshot-1");
        String name = path.getName();
        System.out.println(name);
        String index = name.substring("snapshot-".length());
        System.out.println(index);

        Path path1 = new Path("hdfs://ns/warehouse/db/tb/snapshot/EARLIEST");
        // path_length < begin_index => throw StringIndexOutOfBoundsException
        String pos = path1.getName().substring("snapshot-".length());
        System.out.println(pos);
    }
}
