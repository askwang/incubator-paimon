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

package org.apache.paimon.utils;

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.tag.Tag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.BranchManager.getBranchPath;
import static org.apache.paimon.utils.FileUtils.listVersionedFileStatus;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Manager for {@code Tag}. */
public class TagManager {

    private static final Logger LOG = LoggerFactory.getLogger(TagManager.class);

    private static final String TAG_PREFIX = "tag-";

    private final FileIO fileIO;
    private final Path tablePath;
    private final String branch;

    public TagManager(FileIO fileIO, Path tablePath) {
        this(fileIO, tablePath, DEFAULT_MAIN_BRANCH);
    }

    /** Specify the default branch for data writing. */
    public TagManager(FileIO fileIO, Path tablePath, String branch) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.branch = StringUtils.isBlank(branch) ? DEFAULT_MAIN_BRANCH : branch;
    }

    public TagManager copyWithBranch(String branchName) {
        return new TagManager(fileIO, tablePath, branchName);
    }

    /** Return the root Directory of tags. */
    public Path tagDirectory() {
        return new Path(getBranchPath(fileIO, tablePath, branch) + "/tag");
    }

    /** Return the path of a tag. */
    public Path tagPath(String tagName) {
        return new Path(getBranchPath(fileIO, tablePath, branch) + "/tag/" + TAG_PREFIX + tagName);
    }

    /** Create a tag from given snapshot and save it in the storage. */
    public void createTag(
            Snapshot snapshot,
            String tagName,
            @Nullable Duration timeRetained,
            List<TagCallback> callbacks) {
        checkArgument(!StringUtils.isBlank(tagName), "Tag name '%s' is blank.", tagName);

        // skip create tag for the same snapshot of the same name.
        if (tagExists(tagName)) {
            // tag 存在则判断 tag 对应的 snapshot id 是否和要创建的 snapshot id 一致
            Snapshot tagged = taggedSnapshot(tagName);
            Preconditions.checkArgument(
                    tagged.id() == snapshot.id(), "Tag name '%s' already exists.", tagName);
        } else {
            // tag 创建的本质就是把 snapshot 的 json 拿过来
            // 如果指定了 timeRetained，则添加 timeRetained 和 tagCreateTime 写入到 json
            // snapshot json -> tag json，snapshot 的 watermark 和 statistics 为空，不添加到 json
            Path newTagPath = tagPath(tagName);
            try {
                fileIO.writeFileUtf8(
                        newTagPath,
                        timeRetained != null
                                ? Tag.fromSnapshotAndTagTtl(
                                                snapshot, timeRetained, LocalDateTime.now())
                                        .toJson()
                                : snapshot.toJson());
            } catch (IOException e) {
                throw new RuntimeException(
                        String.format(
                                "Exception occurs when committing tag '%s' (path %s). "
                                        + "Cannot clean up because we can't determine the success.",
                                tagName, newTagPath),
                        e);
            }
        }

        try {
            // 将 tagName 作为分区结果添加到分区中，参考 HiveCatalogITCaseBase#testAddPartitionsForTag
            // 比如设置 'metastore.tag-to-partition' = 'dt'
            // CALL sys.create_tag('t', '2023-10-16', 1)
            // SELECT * FROM t WHERE dt='2023-10-16'")
            callbacks.forEach(callback -> callback.notifyCreation(tagName));
        } finally {
            for (TagCallback tagCallback : callbacks) {
                IOUtils.closeQuietly(tagCallback);
            }
        }
    }

    /** Make sure the tagNames are ALL tags of one snapshot. */
    public void deleteAllTagsOfOneSnapshot(
            List<String> tagNames, TagDeletion tagDeletion, SnapshotManager snapshotManager) {
        Snapshot taggedSnapshot = taggedSnapshot(tagNames.get(0));
        List<Snapshot> taggedSnapshots;

        // skip file deletion if snapshot exists
        if (snapshotManager.snapshotExists(taggedSnapshot.id())) {
            tagNames.forEach(tagName -> fileIO.deleteQuietly(tagPath(tagName)));
            return;
        } else {
            // FileIO discovers tags by tag file, so we should read all tags before we delete tag
            taggedSnapshots = taggedSnapshots();
            tagNames.forEach(tagName -> fileIO.deleteQuietly(tagPath(tagName)));
        }

        doClean(taggedSnapshot, taggedSnapshots, snapshotManager, tagDeletion);
    }

    public void deleteTag(
            String tagName,
            TagDeletion tagDeletion,
            SnapshotManager snapshotManager,
            List<TagCallback> callbacks) {
        checkArgument(!StringUtils.isBlank(tagName), "Tag name '%s' is blank.", tagName);
        checkArgument(tagExists(tagName), "Tag '%s' doesn't exist.", tagName);

        Snapshot taggedSnapshot = taggedSnapshot(tagName);
        List<Snapshot> taggedSnapshots;

        // tag 对应的 snapshot id 还存在，则不需要 delete file
        // skip file deletion if snapshot exists
        if (snapshotManager.copyWithBranch(branch).snapshotExists(taggedSnapshot.id())) {
            deleteTagMetaFile(tagName, callbacks);
            return;
        } else {
            // 找到所有 tags 对应的 snapshot，按 snapshot id 升序排序存储
            // FileIO discovers tags by tag file, so we should read all tags before we delete tag
            SortedMap<Snapshot, List<String>> tags = tags();
            deleteTagMetaFile(tagName, callbacks);
            // 如果当前 taggedSnapshot id 对应多个 tag，则删除 tag metafile 即可，不继续处理
            // skip data file clean if more than 1 tags are created based on this snapshot
            if (tags.get(taggedSnapshot).size() > 1) {
                return;
            }
            taggedSnapshots = new ArrayList<>(tags.keySet());
        }

        doClean(taggedSnapshot, taggedSnapshots, snapshotManager, tagDeletion);
    }

    private void deleteTagMetaFile(String tagName, List<TagCallback> callbacks) {
        fileIO.deleteQuietly(tagPath(tagName));
        try {
            // 处理 callback，即删除 tagName 对应的 分区
            callbacks.forEach(callback -> callback.notifyDeletion(tagName));
        } finally {
            for (TagCallback tagCallback : callbacks) {
                IOUtils.closeQuietly(tagCallback);
            }
        }
    }

    private void doClean(
            Snapshot taggedSnapshot,
            List<Snapshot> taggedSnapshots,
            SnapshotManager snapshotManager,
            TagDeletion tagDeletion) {
        // collect skipping sets from the left neighbor tag and the nearest right neighbor (either
        // the earliest snapshot or right neighbor tag)
        List<Snapshot> skippedSnapshots = new ArrayList<>();

        // taggedSnapshots 是按 snapshot id 升序的所有 tags 对应的 snapshot
        // taggedSnapshot 是当前 tag 对应的 snapshot
        // findIndex 的作用是找到 taggedSnapshot 在 taggedSnapshots 的位置，对其之前（包括自身）的 snapshot 进行清理
        int index = findIndexAskwang(taggedSnapshot, taggedSnapshots);

        // 处理 index 位置的 snapshot 的左右边界
        // askwang-todo: 没看懂
        // the left neighbor tag
        if (index - 1 >= 0) {
            skippedSnapshots.add(taggedSnapshots.get(index - 1));
        }
        // the nearest right neighbor
        Snapshot right = snapshotManager.copyWithBranch(branch).earliestSnapshot();
        if (index + 1 < taggedSnapshots.size()) {
            Snapshot rightTag = taggedSnapshots.get(index + 1);
            right = right.id() < rightTag.id() ? right : rightTag;
        }
        skippedSnapshots.add(right);

        // delete data files and empty directories
        Predicate<ManifestEntry> dataFileSkipper = null;
        boolean success = true;
        try {
            dataFileSkipper = tagDeletion.dataFileSkipper(skippedSnapshots);
        } catch (Exception e) {
            LOG.info(
                    String.format(
                            "Skip cleaning data files for tag of snapshot %s due to failed to build skipping set.",
                            taggedSnapshot.id()),
                    e);
            success = false;
        }
        if (success) {
            tagDeletion.cleanUnusedDataFiles(taggedSnapshot, dataFileSkipper);
            tagDeletion.cleanDataDirectories();
        }

        // delete manifests
        tagDeletion.cleanUnusedManifests(
                taggedSnapshot, tagDeletion.manifestSkippingSet(skippedSnapshots));
    }

    /** Check if a tag exists. */
    public boolean tagExists(String tagName) {
        Path path = tagPath(tagName);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to determine if tag '%s' exists in path %s.", tagName, path),
                    e);
        }
    }

    /** Get the tagged snapshot by name. */
    public Snapshot taggedSnapshot(String tagName) {
        checkArgument(tagExists(tagName), "Tag '%s' doesn't exist.", tagName);
        return Tag.fromPath(fileIO, tagPath(tagName)).trimToSnapshot();
    }

    public long tagCount() {
        try {
            return listVersionedFileStatus(fileIO, tagDirectory(), TAG_PREFIX).count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Get all tagged snapshots sorted by snapshot id. */
    public List<Snapshot> taggedSnapshots() {
        return new ArrayList<>(tags().keySet());
    }

    /** Get all tagged snapshots with names sorted by snapshot id. */
    public SortedMap<Snapshot, List<String>> tags() {
        return tags(tagName -> true);
    }

    /**
     * Retrieves a sorted map of snapshots filtered based on a provided predicate. The predicate
     * determines which tag names should be included in the result. Only snapshots with tag names
     * that pass the predicate test are included.
     *
     * @param filter A Predicate that tests each tag name. Snapshots with tag names that fail the
     *     test are excluded from the result.
     * @return A sorted map of filtered snapshots keyed by their IDs, each associated with its tag
     *     name.
     * @throws RuntimeException if an IOException occurs during retrieval of snapshots.
     */
    public SortedMap<Snapshot, List<String>> tags(Predicate<String> filter) {
        TreeMap<Snapshot, List<String>> tags =
                new TreeMap<>(Comparator.comparingLong(Snapshot::id));
        try {
            List<Path> paths =
                    listVersionedFileStatus(fileIO, tagDirectory(), TAG_PREFIX)
                            .map(FileStatus::getPath)
                            .collect(Collectors.toList());

            for (Path path : paths) {
                String tagName = path.getName().substring(TAG_PREFIX.length());

                // Predicate<String> filter = s -> true. 只要是字符串就返回 true，if条件不满足
                if (!filter.test(tagName)) {
                    continue;
                }
                // If the tag file is not found, it might be deleted by
                // other processes, so just skip this tag
                // askwang-done: tag 中包含 snapshot + tagCreateTime + tagTimeRetained，直接读 tag path
                // 内容是否有问题？
                // 没问题，json 解析时会根据匹配字段进行解析，不匹配的字段会丢掉。
                // tag json -> snapshot json, 解析时去掉 tagCreateTime 和 tagTimeRetained
                Snapshot snapshot = Snapshot.safelyFromPath(fileIO, path);
                if (snapshot != null) {
                    tags.computeIfAbsent(snapshot, s -> new ArrayList<>()).add(tagName);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return tags;
    }

    /** Get all {@link Tag}s. */
    public List<Pair<Tag, String>> tagObjects() {
        try {
            List<Path> paths =
                    listVersionedFileStatus(fileIO, tagDirectory(), TAG_PREFIX)
                            .map(FileStatus::getPath)
                            .collect(Collectors.toList());
            List<Pair<Tag, String>> tags = new ArrayList<>();
            for (Path path : paths) {
                String tagName = path.getName().substring(TAG_PREFIX.length());
                Tag tag = Tag.safelyFromPath(fileIO, path);
                if (tag != null) {
                    tags.add(Pair.of(tag, tagName));
                }
            }
            return tags;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> sortTagsOfOneSnapshot(List<String> tagNames) {
        return tagNames.stream()
                .map(
                        name -> {
                            try {
                                return fileIO.getFileStatus(tagPath(name));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .sorted(Comparator.comparingLong(FileStatus::getModificationTime))
                .map(fileStatus -> fileStatus.getPath().getName().substring(TAG_PREFIX.length()))
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    public List<String> allTagNames() {
        return tags().values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    /** askwang-done: 二分查找加速 taggedSnapshot 查询速度. A: 内存查找很快，无需优化. */
    private int findIndex(Snapshot taggedSnapshot, List<Snapshot> taggedSnapshots) {
        for (int i = 0; i < taggedSnapshots.size(); i++) {
            if (taggedSnapshot.id() == taggedSnapshots.get(i).id()) {
                return i;
            }
        }
        throw new RuntimeException(
                String.format(
                        "Didn't find tag with snapshot id '%s'.This is unexpected.",
                        taggedSnapshot.id()));
    }

    private int findIndexAskwang(Snapshot taggedSnapshot, List<Snapshot> taggedSnapshots) {
        if (taggedSnapshots.size() == 1 && taggedSnapshots.get(0).id() == taggedSnapshot.id()) {
            return 0;
        }
        int left = 0;
        int right = taggedSnapshots.size() - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (taggedSnapshots.get(mid).id() == taggedSnapshot.id()) {
                return mid;
            } else if (taggedSnapshots.get(mid).id() < taggedSnapshot.id()) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        throw new RuntimeException(
                String.format(
                        "Didn't find tag with snapshot id '%s'.This is unexpected.",
                        taggedSnapshot.id()));
    }

    public static List<Snapshot> findOverlappedSnapshots(
            List<Snapshot> taggedSnapshots, long beginInclusive, long endExclusive) {
        List<Snapshot> snapshots = new ArrayList<>();
        int right = findPreviousTag(taggedSnapshots, endExclusive);
        if (right >= 0) {
            // askwang-todo: left 会多一个无效的值，参考 findNextOrEqualTagAskwang
            int left = Math.max(findPreviousOrEqualTag(taggedSnapshots, beginInclusive), 0);
            for (int i = left; i <= right; i++) {
                snapshots.add(taggedSnapshots.get(i));
            }
        }
        return snapshots;
    }

    /**
     * 找到 taggedSnapshots 和 snapshot [begin, end) id 直接重叠的 snapshot 缩小 taggedSnapshots 中的 snapshot.
     * id 的范围 [left, right); 第一次小于或等于 begin 的 id 为 left，（优化：第一次大于或等于 begin 的 id 为 left） 第一次小于 end 的.
     * id 为 right.
     */
    private static List<Snapshot> findOverlappedSnapshotsAskwang(
            List<Snapshot> taggedSnapshot, long beginInclusive, long endExclusive) {
        List<Snapshot> snapshots = new ArrayList<>();
        int right = findPreviousTag(taggedSnapshot, endExclusive);
        if (right >= 0) {
            int left = Math.max(findNextOrEqualTagAskwang(taggedSnapshot, beginInclusive), 0);
            for (int i = left; i <= right; i++) {
                snapshots.add(taggedSnapshot.get(i));
            }
        }

        return snapshots;
    }

    public static int findPreviousTag(List<Snapshot> taggedSnapshots, long targetSnapshotId) {
        for (int i = taggedSnapshots.size() - 1; i >= 0; i--) {
            if (taggedSnapshots.get(i).id() < targetSnapshotId) {
                return i;
            }
        }
        return -1;
    }

    /** 减少不重叠的 snapshot 范围 [begin, end): [10, 15) taggedSnapshots: [7, 9, 11, 12]. */
    private static int findNextOrEqualTagAskwang(
            List<Snapshot> taggedSnapshots, long targetSnapshotId) {
        for (int i = 0; i < taggedSnapshots.size(); i++) {
            if (taggedSnapshots.get(i).id() >= targetSnapshotId) {
                return i;
            }
        }
        return -1;
    }

    private static int findPreviousOrEqualTag(
            List<Snapshot> taggedSnapshots, long targetSnapshotId) {
        for (int i = taggedSnapshots.size() - 1; i >= 0; i--) {
            if (taggedSnapshots.get(i).id() <= targetSnapshotId) {
                return i;
            }
        }
        return -1;
    }
}
