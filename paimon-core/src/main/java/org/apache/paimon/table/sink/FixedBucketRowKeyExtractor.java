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

package org.apache.paimon.table.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;

/** {@link KeyAndBucketExtractor} for {@link InternalRow}. */
public class FixedBucketRowKeyExtractor extends RowKeyExtractor {

    private final int numBuckets;
    private final boolean sameBucketKeyAndTrimmedPrimaryKey;
    private final Projection bucketKeyProjection;

    private BinaryRow reuseBucketKey;
    private Integer reuseBucket;

    public FixedBucketRowKeyExtractor(TableSchema schema) {
        // projection构建：调用 apply 方法，将 RowData 选取部分列转换成 BinaryRowData。类似于列裁剪）
        // partitionProjection、trimmedPrimaryKeyProjection （由 partitionKeyExtractor 封装）你
        // logPrimaryKeyProjection、bucketKeyProjection
        super(schema);
        numBuckets = new CoreOptions(schema.options()).bucket();
        // bucketKeys() 如果 bucketKeys 为空，则返回 trimmedPrimaryKeys() 调用结果
        sameBucketKeyAndTrimmedPrimaryKey = schema.bucketKeys().equals(schema.trimmedPrimaryKeys());
        bucketKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.bucketKeys()));
    }

    @Override
    public void setRecord(InternalRow record) {
        super.setRecord(record);
        this.reuseBucketKey = null;
        this.reuseBucket = null;
    }

    private BinaryRow bucketKey() {
        if (sameBucketKeyAndTrimmedPrimaryKey) {
            return trimmedPrimaryKey();
        }

        // askwang-todo: 什么情况下会用到 reuseBucketKey ？
        if (reuseBucketKey == null) {
            reuseBucketKey = bucketKeyProjection.apply(record);
        }
        return reuseBucketKey;
    }

    @Override
    public int bucket() {
        // 计算 record 的 bucketKey，根据 bucketKey 的 hashcode 分发
        BinaryRow bucketKey = bucketKey();
        if (reuseBucket == null) {
            reuseBucket =
                    KeyAndBucketExtractor.bucket(
                            KeyAndBucketExtractor.bucketKeyHashCode(bucketKey), numBuckets);
        }
        return reuseBucket;
    }
}
