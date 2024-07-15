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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Universal Compaction Style is a compaction style, targeting the use cases requiring lower write
 * amplification, trading off read amplification and space amplification.
 *
 * <p>See RocksDb Universal-Compaction:
 * https://github.com/facebook/rocksdb/wiki/Universal-Compaction.
 */
public class UniversalCompaction implements CompactStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(UniversalCompaction.class);

    private final int maxSizeAmp;
    private final int sizeRatio;
    private final int numRunCompactionTrigger;

    @Nullable private final Long opCompactionInterval;
    @Nullable private Long lastOptimizedCompaction;

    public UniversalCompaction(int maxSizeAmp, int sizeRatio, int numRunCompactionTrigger) {
        this(maxSizeAmp, sizeRatio, numRunCompactionTrigger, null);
    }

    public UniversalCompaction(
            int maxSizeAmp,
            int sizeRatio,
            int numRunCompactionTrigger,
            @Nullable Duration opCompactionInterval) {
        this.maxSizeAmp = maxSizeAmp;
        this.sizeRatio = sizeRatio;
        this.numRunCompactionTrigger = numRunCompactionTrigger;
        this.opCompactionInterval =
                opCompactionInterval == null ? null : opCompactionInterval.toMillis();
    }

    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        int maxLevel = numLevels - 1;

        if (opCompactionInterval != null) {
            if (lastOptimizedCompaction == null
                    || currentTimeMillis() - lastOptimizedCompaction > opCompactionInterval) {
                LOG.debug("Universal compaction due to optimized compaction interval");
                updateLastOptimizedCompaction();
                return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
            }
        }

        // https://www.cnblogs.com/Aitozi/p/17506198.html 介绍 size Amplification 和 size radio
        // 1 checking for reducing size amplification
        CompactUnit unit = pickForSizeAmp(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size amplification");
            }
            return Optional.of(unit);
        }

        // 2 checking for size ratio
        unit = pickForSizeRatio(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size ratio");
            }
            return Optional.of(unit);
        }

        // 3 checking for file num
        if (runs.size() > numRunCompactionTrigger) {
            // numRunCompactionTrigger = 5
            // 比如 runs = 10，则直接从第 6 个文件开始处理
            // 即直接比较 size(R7) / size(R1~R6) 与 size_ratio_trigger
            // compacting for file num
            int candidateCount = runs.size() - numRunCompactionTrigger + 1;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to file num");
            }
            return Optional.ofNullable(pickForSizeRatio(maxLevel, runs, candidateCount));
        }

        return Optional.empty();
    }

    @VisibleForTesting
    CompactUnit pickForSizeAmp(int maxLevel, List<LevelSortedRun> runs) {
        if (runs.size() < numRunCompactionTrigger) {
            return null;
        }
        // runs 的顺序
        long candidateSize =
                runs.subList(0, runs.size() - 1).stream()
                        .map(LevelSortedRun::run)
                        .mapToLong(SortedRun::totalSize)
                        .sum();

        long earliestRunSize = runs.get(runs.size() - 1).run().totalSize();

        // maxSizeAmp: compaction.max-size-amplification-percent = 200
        // 判断R1～R(n-1) sorted run大小有没有超过 最高层(最老数据)的两倍, 超过了那就触发一次full compaction.
        // size amplification = percentage of additional size
        if (candidateSize * 100 > maxSizeAmp * earliestRunSize) {
            // askwang-todo：更新 lastOptimizedCompaction 值作用是什么
            updateLastOptimizedCompaction();
            return CompactUnit.fromLevelRuns(maxLevel, runs);
        }

        return null;
    }

    CompactUnit pickForSizeAmpAskwang(int maxLevel, List<LevelSortedRun> runs) {
        if (runs.size() < numRunCompactionTrigger) {
            return null;
        }
        // 前 n-1 个 SortedRun size 大小
        long candidateSize =
                runs.subList(0, runs.size() - 1).stream()
                        .map(levelSortedRun -> levelSortedRun.run().totalSize())
                        .mapToLong(Long::longValue)
                        .sum();

        long earliestRunSize = runs.get(runs.size() - 1).run().totalSize();

        if (candidateSize * 100 > maxSizeAmp * earliestRunSize) {
            CompactUnit compactUnit = fromLevelRunsAskwang(maxLevel, runs);
            return compactUnit;
        }
        return null;
    }

    public CompactUnit fromLevelRunsAskwang(int maxLevel, List<LevelSortedRun> runs) {
        List<DataFileMeta> list = new ArrayList<>();
        for (LevelSortedRun run : runs) {
            list.addAll(run.run().files());
        }
        return new CompactUnit() {
            @Override
            public int outputLevel() {
                return maxLevel;
            }

            @Override
            public List<DataFileMeta> files() {
                return list;
            }
        };
    }

    @VisibleForTesting
    CompactUnit pickForSizeRatio(int maxLevel, List<LevelSortedRun> runs) {
        if (runs.size() < numRunCompactionTrigger) {
            return null;
        }

        return pickForSizeRatio(maxLevel, runs, 1);
    }

    private CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount) {
        return pickForSizeRatio(maxLevel, runs, candidateCount, false);
    }

    /*
     * sizeRatio: compaction.size-ratio = 1.
     * size_ratio_trigger = (100 + options.compaction_options_universal.size_ratio) / 100.
     * size(R2) / size(R1) <= size_ratio_trigger, 那么（R1，R2）被合并到一起 size(R3) / size(R1+r2) <= size_ratio_trigger，R3应该被包含，得到(R1,R2,R3).
     * 这个策略的效果有点类似于是除了最高层之外, 把各个sorted run的大小尽可能靠近对齐.
     * 1 1 1 1 1 => 5.
     * 1 5 (no compaction triggered).
     * 1 1 5 (no compaction triggered).
     * 1 1 1 5 (no compaction triggered).
     * 1 1 1 1 5 => 4 5.
     * 1 4 5 (no compaction triggered).
     * 1 1 4 5 (no compaction triggered).
     * 1 1 1 4 5 => 3 4 5.
     * 1 3 4 5 (no compaction triggered).
     * 1 1 3 4 5 => 2 3 4 5.
     *
     * boolean forcePick 参数是用于 ForceUpLevel0Compaction 策略
     */
    public CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount, boolean forcePick) {
        long candidateSize = candidateSize(runs, candidateCount);
        for (int i = candidateCount; i < runs.size(); i++) {
            LevelSortedRun next = runs.get(i);
            if (candidateSize * (100.0 + sizeRatio) / 100.0 < next.run().totalSize()) {
                break;
            }

            candidateSize += next.run().totalSize();
            candidateCount++;
        }

        if (forcePick || candidateCount > 1) {
            // 创建 outputLevel 可能为非 maxLevel 的 CompactUnit
            return createUnit(runs, maxLevel, candidateCount);
        }

        return null;
    }

    public CompactUnit pickForSizeRatioAskwang(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount, boolean forcePick) {
        long candidateSize = 0;
        for (int i = 0; i < candidateCount; i++) {
            candidateSize += runs.get(i).run().totalSize();
        }
        for (int i = candidateCount; i < runs.size(); i++) {
            long nextTotalSize = runs.get(i).run().totalSize();
            // 当下一个 SortedRun 不满足 SizeRatio 条件，则 candidate 选取完毕
            if (candidateSize * 1.01 < nextTotalSize) {
                break;
            }
            candidateCount++;
            candidateSize += nextTotalSize;
        }

        // 如果 candidateCount = 1，
        // runs.subList(0, runCount) 取一个 SortedRun，outputLevel = 0，没意义
        if (forcePick || candidateCount > 1) {
            return createUnit(runs, maxLevel, candidateCount);
        }
        return null;
    }

    private long candidateSize(List<LevelSortedRun> runs, int candidateCount) {
        long size = 0;
        for (int i = 0; i < candidateCount; i++) {
            size += runs.get(i).run().totalSize();
        }
        return size;
    }

    @VisibleForTesting
    CompactUnit createUnit(List<LevelSortedRun> runs, int maxLevel, int runCount) {
        // 确定 compact 后的文件放到哪一个 level
        int outputLevel;
        // 1. runCount 包含所有 runs，则 outputLevel 为 maxLevel
        // 否则，放到 next level 的上一个 level （这个处理比较巧妙，不是直接取第 runCount LevelSortedRun 的 level)
        if (runCount == runs.size()) {
            outputLevel = maxLevel;
        } else {
            // level of next run - 1
            outputLevel = Math.max(0, runs.get(runCount).level() - 1);
        }

        // 2. 如果 outputLevel = 0，则将下一个 LevelSortedRun 纳入到 runCount 中，
        // 直到下一个 LevelSortedRun 的 level 不为 0
        if (outputLevel == 0) {
            // do not output level 0
            for (int i = runCount; i < runs.size(); i++) {
                LevelSortedRun next = runs.get(i);
                runCount++;
                // level0 的合并至少下层到一个非 level0 的 level
                // runs 存在都有有数据的 SortedRun，所以 next.level 一般不会出现空值
                // 如果 next.level 一直等于 0，则 runCount 一直自增，直到等于 runs.size，然后下面更新 outputLevle
                if (next.level() != 0) {
                    outputLevel = next.level();
                    break;
                }
            }
        }

        // 3. 经过第 2 步的 outputLevel 非 0 处理，runCount 又可能包含所有 runs
        if (runCount == runs.size()) {
            updateLastOptimizedCompaction();
            outputLevel = maxLevel;
        }

        return CompactUnit.fromLevelRuns(outputLevel, runs.subList(0, runCount));
    }

    private void updateLastOptimizedCompaction() {
        lastOptimizedCompaction = currentTimeMillis();
    }

    long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
