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

package org.apache.paimon.table;

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/** An implementation for {@link ExpireSnapshots}. */
public class ExpireSnapshotsImpl implements ExpireSnapshots {

    private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsImpl.class);

    private final SnapshotManager snapshotManager;
    private final ConsumerManager consumerManager;
    private final SnapshotDeletion snapshotDeletion;
    private final TagManager tagManager;
    private final boolean cleanEmptyDirectories;

    private ExpireConfig expireConfig;

    public ExpireSnapshotsImpl(
            SnapshotManager snapshotManager,
            SnapshotDeletion snapshotDeletion,
            TagManager tagManager,
            boolean cleanEmptyDirectories) {
        this.snapshotManager = snapshotManager;
        this.consumerManager =
                new ConsumerManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        this.snapshotDeletion = snapshotDeletion;
        this.tagManager = tagManager;
        this.cleanEmptyDirectories = cleanEmptyDirectories;
        this.expireConfig = ExpireConfig.builder().build();
    }

    @Override
    public ExpireSnapshots config(ExpireConfig expireConfig) {
        this.expireConfig = expireConfig;
        return this;
    }

    @Override
    public int expire() {
        snapshotDeletion.setChangelogDecoupled(expireConfig.isChangelogDecoupled());
        int retainMax = expireConfig.getSnapshotRetainMax();
        int retainMin = expireConfig.getSnapshotRetainMin();
        int maxDeletes = expireConfig.getSnapshotMaxDeletes();
        long olderThanMills =
                System.currentTimeMillis() - expireConfig.getSnapshotTimeRetain().toMillis();

        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            // no snapshot, nothing to expire
            return 0;
        }

        Long earliest = snapshotManager.earliestSnapshotId();
        if (earliest == null) {
            return 0;
        }

        Preconditions.checkArgument(
                retainMax >= retainMin, "retainMax must greater than retainMin.");

        // example, snapshot[1,2,3,4,5,6,7,8,9,10]
        // minRetain = 2, maxRetain = 5, maxDeletes=4
        // min=max(10-5+1,1)=6
        // maxExclusive=10-2+1=9
        // maxExclusive = min(9, 1+4)=5

        // the min snapshot to retain from 'snapshot.num-retained.max'
        // (the maximum number of snapshots to retain)
        long min = Math.max(latestSnapshotId - retainMax + 1, earliest);

        // the max exclusive snapshot to expire until
        // protected by 'snapshot.num-retained.min'
        // (the minimum number of completed snapshots to retain)
        long maxExclusive = latestSnapshotId - retainMin + 1;

        // Consumer which contains next snapshot.
        // 最早的 consumer snapshot id 是最早需要读取的 snapshot，不可被删除
        // the snapshot being read by the consumer cannot be deleted
        maxExclusive =
                Math.min(maxExclusive, consumerManager.minNextSnapshot().orElse(Long.MAX_VALUE));

        // protected by 'snapshot.expire.limit'
        // (the maximum number of snapshots allowed to expire at a time)
        // askwang-todo: 考虑 maxDelete 后，min 就有可能大于 maxExclusive，则直接 expireUtil(1,5)
        // A:好像同时这么retainMin和retainMax和 maxDelete 的可能性能小。
        maxExclusive = Math.min(maxExclusive, earliest + maxDeletes);

        // snapshot(i).timeMillis + snapshot.time-retained >= system.currentTime
        // 则认定为该 snapshot 没有过期。比如 10点创建，保留1小时，当前是 10:30，表示没有过期
        for (long id = min; id < maxExclusive; id++) {
            // Early exit the loop for 'snapshot.time-retained'
            // (the maximum time of snapshots to retain)
            if (snapshotManager.snapshotExists(id)
                    && olderThanMills <= snapshotManager.snapshot(id).timeMillis()) {
                // 如果 id 没有过期，则过期清理的 snapshot id 范围为 [earliest, id)
                return expireUntil(earliest, id);
            }
        }
        // [min, maxExclusive) 之间的 snapshot 都已过期
        return expireUntil(earliest, maxExclusive);
    }

    @VisibleForTesting
    public int expireUntil(long earliestId, long endExclusiveId) {
        if (endExclusiveId <= earliestId) {
            // No expire happens:
            // write the hint file in order to see the earliest snapshot directly next time
            // should avoid duplicate writes when the file exists
            if (snapshotManager.readHint(SnapshotManager.EARLIEST) == null) {
                // askwang-done: 这种条件下应该写入 earliestId 为 EARLIEST 值
                // pr: https://github.com/apache/paimon/pull/3398
                writeEarliestHint(endExclusiveId);
            }

            // fast exit
            return 0;
        }

        // find first snapshot to expire
        long beginInclusiveId = earliestId;
        for (long id = endExclusiveId - 1; id >= earliestId; id--) {
            if (!snapshotManager.snapshotExists(id)) {
                // only latest snapshots are retained, as we cannot find this snapshot, we can
                // assume that all snapshots preceding it have been removed
                beginInclusiveId = id + 1;
                break;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Snapshot expire range is [" + beginInclusiveId + ", " + endExclusiveId + ")");
        }

        // tag 依赖的 snapshot
        List<Snapshot> taggedSnapshots = tagManager.taggedSnapshots();

        // 删除文件：删除一个 snapshot 下的文件，前提条件是下一个 snapshot 没有使用该文件
        // askwang-done: 为什么要这么遍历？
        // A：下一个 snapshot 的 deltaManifestList 记录决定是否有删除当前 snapshot 文件的操作。
        // delete merge tree files
        // deleted merge tree files in a snapshot are not used by the next snapshot, so the range of
        // id should be (beginInclusiveId, endExclusiveId]
        for (long id = beginInclusiveId + 1; id <= endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete merge tree files not used by snapshot #" + id);
            }
            Snapshot snapshot = snapshotManager.snapshot(id);
            // expire merge tree files and collect changed buckets
            Predicate<ManifestEntry> skipper;
            try {
                // 没有 tag 的情况，skipper 始终为 false
                skipper = snapshotDeletion.dataFileSkipper(taggedSnapshots, id);
            } catch (Exception e) {
                LOG.info(
                        String.format(
                                "Skip cleaning data files of snapshot '%s' due to failed to build skipping set.",
                                id),
                        e);
                continue;
            }

            snapshotDeletion.cleanUnusedDataFiles(snapshot, skipper);
        }

        // delete changelog files
        if (!expireConfig.isChangelogDecoupled()) {
            for (long id = beginInclusiveId; id < endExclusiveId; id++) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ready to delete changelog files from snapshot #" + id);
                }
                Snapshot snapshot = snapshotManager.snapshot(id);
                if (snapshot.changelogManifestList() != null) {
                    snapshotDeletion.deleteAddedDataFiles(snapshot.changelogManifestList());
                }
            }
        }

        // data files and changelog files in bucket directories has been deleted
        // then delete changed bucket directories if they are empty
        // 从 bucket=-1 -> bucket=2，清理snapshot/file 时会清理之前动态 bucket 生成的文件和空的 bucket 目录
        if (cleanEmptyDirectories) {
            snapshotDeletion.cleanDataDirectories();
        }

        // 过滤出不应该被删除的 snapshot manifest 和文件，因为 taggedSnapshot 会依赖
        // delete manifests and indexFiles
        List<Snapshot> skippingSnapshots =
                TagManager.findOverlappedSnapshots(
                        taggedSnapshots, beginInclusiveId, endExclusiveId);
        skippingSnapshots.add(snapshotManager.snapshot(endExclusiveId));
        // 记录 baseManifestList、deltaManifestList、manifest 文件名
        Set<String> skippingSet = snapshotDeletion.manifestSkippingSet(skippingSnapshots);

        // 删除过期 snapshot 的 manifest 文件、snapshot 目录文件
        for (long id = beginInclusiveId; id < endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete manifests in snapshot #" + id);
            }

            Snapshot snapshot = snapshotManager.snapshot(id);
            // clean manifest and indexFile
            snapshotDeletion.cleanUnusedManifests(snapshot, skippingSet);
            if (expireConfig.isChangelogDecoupled()) {
                commitChangelog(new Changelog(snapshot));
            }
            // snapshot id 删除的范围是 [bengin, end)
            snapshotManager.fileIO().deleteQuietly(snapshotManager.snapshotPath(id));
        }

        // 更新 snapshot/EARLIEST 文件
        writeEarliestHint(endExclusiveId);
        return (int) (endExclusiveId - beginInclusiveId);
    }

    private void commitChangelog(Changelog changelog) {
        try {
            snapshotManager.commitChangelog(changelog, changelog.id());
            snapshotManager.commitLongLivedChangelogLatestHint(changelog.id());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeEarliestHint(long earliest) {
        try {
            snapshotManager.commitEarliestHint(earliest);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @VisibleForTesting
    public SnapshotDeletion snapshotDeletion() {
        return snapshotDeletion;
    }
}
