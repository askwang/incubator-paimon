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

package org.apache.paimon.spark.sql;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author askwang
 * @date 2024/7/27
 */
public class AskwangCommitCallback implements CommitCallback {
    private String appid;

    public AskwangCommitCallback() {}

    public AskwangCommitCallback(String appid) {
        this.appid = appid;
    }

    @Override
    public void call(List<ManifestCommittable> committables) {
        Map<Integer, List<String>> bucketToFiles = new ConcurrentHashMap<>();

        System.out.println("=====enter commit callback");
        System.out.println("appid: " + appid);
        for (ManifestCommittable committable : committables) {
            for (CommitMessage message : committable.fileCommittables()) {
                CommitMessageImpl msg = (CommitMessageImpl) message;
                List<String> files = bucketToFiles.computeIfAbsent(msg.bucket(), f -> new ArrayList<>());
                msg.newFilesIncrement().newFiles().stream()
                        .map(DataFileMeta::fileName).forEach(files::add);
            }
        }

        bucketToFiles.forEach((bucket, files) -> {
            System.out.println("bucket: " + bucket + "; files: " + files);
        });
    }

    @Override
    public void close() throws Exception {}
}
