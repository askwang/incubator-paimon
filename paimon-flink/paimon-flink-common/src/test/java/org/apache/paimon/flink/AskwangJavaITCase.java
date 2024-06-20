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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;

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

    @Test
    public void testPredicate() {
        List<String> tags = new ArrayList<String>(Arrays.asList("tag-tag1", "tag-tag2", "tag-tag3"));
        Predicate<String> filter = str -> true;
        System.out.println(filter.test("a"));
        for (String tag : tags) {
            String tagName = tag.substring("tag-".length());
            System.out.println(tagName + ":" + filter.test(tagName));;
        }
    }

    /**
     * computeIfAbsent 和 computeIfPresent 都是 Map 接口的方法.
     * V computeIfAbsent(K key, Function mappingFunction).
     *  - key 不存在，触发 mappingFunction
     *  - key 存在，直接返回该 key 对应的值
     * V computeIfPresent(K key, BiFunction remappingFunction).
     *  - 与 computeIfAbsent 刚好相反
     *  - key 存在，触发 remappingFunction
     *  - key 不存在，不做任务操作
     */
    @Test
    public void testComputeIfAbsent() {
        List<String> tagNames = new ArrayList<>(Arrays.asList("tag-tag1", "tag-tag2", "tag-tag3"));
        TreeMap<Integer, List<String>> tags = new TreeMap<>();
        for (String tagName : tagNames) {
            tags.computeIfAbsent(1,  i -> new ArrayList<>()).add(tagName);
        }
        System.out.println(tagNames);
    }

    @Test
    public void testComputeIfPresent() {
        List<String> tagNames = new ArrayList<>(Arrays.asList("tag-tag1", "tag-tag2", "tag-tag3"));
        TreeMap<Integer, List<String>> tags = new TreeMap<>();
        for (String tagName : tagNames) {
            tags.computeIfPresent(1, (k, v) -> v);
        }
        System.out.println(tagNames);
    }

    @Test
    public void testMidValue() {
        int left = 1;
        int right = 5;
        int mid = left + ((right - left) >> 2);
        System.out.println(mid);
    }
}
