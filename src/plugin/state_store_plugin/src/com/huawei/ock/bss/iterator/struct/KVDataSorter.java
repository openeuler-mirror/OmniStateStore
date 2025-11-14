/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.huawei.ock.bss.iterator.struct;

import com.huawei.ock.bss.common.exception.BSSRuntimeException;
import com.huawei.ock.bss.iterator.CloseableIterator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * KVDataSorter
 *
 * @since BeiMing 25.0.T1
 */
public class KVDataSorter implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(KVDataSorter.class);

    private final int blockSize;

    private final int maxOutputStream;

    private final long maxFileSize;

    private final boolean compressionEnabled;

    private final boolean checksumEnabled;

    private final Path basePath;

    private final Stat sortedStat;

    private final KeyGroupRange totalKeyGroupRange;

    private final TreeMap<Integer, Meta> startKeyGroupToMeta;

    private int lastKeyGroupToIterates;

    private boolean isInitialized;

    public KVDataSorter(KeyGroupRange totalKeyGroupRange, Path basePath, int maxOutputStream, long maxFileSize,
        int blockSize, boolean compressionEnabled, boolean checksumEnabled) {
        this.totalKeyGroupRange = totalKeyGroupRange;
        this.basePath = basePath;
        this.maxOutputStream = maxOutputStream;
        this.maxFileSize = maxFileSize;
        this.blockSize = blockSize;
        this.compressionEnabled = compressionEnabled;
        this.checksumEnabled = checksumEnabled;
        this.startKeyGroupToMeta = new TreeMap<>();
        this.isInitialized = false;
        this.lastKeyGroupToIterates = totalKeyGroupRange.getStartKeyGroup() - 1;
        this.sortedStat = new Stat();
    }

    /**
     * init
     *
     * @param initKeyValueItemIterator initKeyValueItemIterator
     */
    public void init(Iterator<SingleKeyGroupKVState.KeyValueItem> initKeyValueItemIterator) {
        if (this.isInitialized) {
            return;
        }
        LOG.info("splitMetas begin");
        List<Meta> splitMetas =
            splitData(initKeyValueItemIterator, this.totalKeyGroupRange, this.maxOutputStream, true);
        LOG.info("splitMetas end");
        updateMetas(Collections.emptyList(), splitMetas);
        this.isInitialized = true;
    }

    /**
     * iterator
     *
     * @param keyGroup keyGroup
     * @return CloseableIterator<SingleKeyGroupKVState.KeyValueItem>
     * @throws IOException IOException
     */
    public CloseableIterator<SingleKeyGroupKVState.KeyValueItem> iterator(int keyGroup) throws IOException {
        if (this.lastKeyGroupToIterates + 1 != keyGroup) {
            String errMsg = String.format(
                "Key groups in %s should be iterated one by one, but the previous is %s, and the current is %s",
                this.totalKeyGroupRange, this.lastKeyGroupToIterates, keyGroup);
            LOG.error(errMsg);
            throw new BSSRuntimeException(errMsg);
        }
        this.lastKeyGroupToIterates = keyGroup;

        LOG.debug("Create iterator for key group {}", keyGroup);

        cleanupKeyGroup(keyGroup);
        prepareKeyGroup(keyGroup);

        Meta meta = findKeyGroupMeta(keyGroup);
        if (meta == null || !meta.getValidKeyGroups().contains(keyGroup)) {
            return CloseableIterator.empty();
        }

        Preconditions.checkState((meta.getValidKeyGroups().size() == 1),
            "Set of valid key groups should only contains %s, but is %s, and data file group is %s", keyGroup,
            meta.getValidKeyGroups(), meta.getKVStatesGroup());
        final KVStatesGroup.Reader reader = new KVStatesGroup.Reader(meta.getKVStatesGroup());
        LOG.info("Before return iterator from sorter.");
        return new CloseableIterator<SingleKeyGroupKVState.KeyValueItem>() {
            private final Iterator<SingleKeyGroupKVState.KeyValueItem> iterator = reader.iterator();

            /**
             * hasNext
             *
             * @return boolean
             */
            public boolean hasNext() {
                return this.iterator.hasNext();
            }

            /**
             * next
             *
             * @return SingleKeyGroupKVState.KeyValueItem
             */
            public SingleKeyGroupKVState.KeyValueItem next() {
                return this.iterator.next();
            }

            /**
             * close
             *
             * @throws Exception Exception
             */
            public void close() throws Exception {
                reader.close();
            }
        };
    }

    private List<Meta> splitData(Iterator<SingleKeyGroupKVState.KeyValueItem> keyValueItemIterator,
        KeyGroupRange keyGroupRange, int numSplits, boolean allowOneKeyGroup) {
        if (!(allowOneKeyGroup || keyGroupRange.getNumberOfKeyGroups() > 1)) {
            throw new BSSRuntimeException("Can't split key group range %s containing only one group");
        }

        // 重新分配keyGroup，确定numSplits为什么和最大输出流数量配置项一致
        List<KeyGroupRange> splitKeyGroupRanges = SortUtils.splitKeyGroups(keyGroupRange, numSplits);

        // splitMetaBuilders中存入MetaBuilder
        try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            List<Meta.MetaBuilder> splitMetaBuilders = new ArrayList<>();
            LOG.info("splitKeyGroupRanges size: {}", splitKeyGroupRanges.size());
            for (KeyGroupRange range : splitKeyGroupRanges) {
                KVStatesGroup.Builder builder =
                    new KVStatesGroup.Builder(range, this.basePath, this.maxFileSize, this.blockSize,
                        this.compressionEnabled, this.checksumEnabled);

                closeableRegistry.registerCloseable(builder);
                splitMetaBuilders.add(new Meta.MetaBuilder(range, builder));
            }

            int startGroup = keyGroupRange.getStartKeyGroup();
            Meta.MetaBuilder[] metaBuilderForEachKeyGroup = new Meta.MetaBuilder[keyGroupRange.getNumberOfKeyGroups()];
            LOG.info("splitMetaBuilders size: {}", splitMetaBuilders.size());
            for (Meta.MetaBuilder builder : splitMetaBuilders) {
                for (int keyGroup : builder.getKeyGroupRange()) {
                    metaBuilderForEachKeyGroup[keyGroup - startGroup] = builder;
                }
            }
            LOG.info("keyValueItemIterator begin");
            int num = 0;
            while (keyValueItemIterator.hasNext()) {
                num++;
                SingleKeyGroupKVState.KeyValueItem keyValueItem = keyValueItemIterator.next();
                int keyGroup = keyValueItem.getKeyGroup();
                Preconditions.checkState(keyGroupRange.contains(keyGroup),
                    "There is a key with key group %s not in the key group range", keyGroup, keyGroupRange);
                metaBuilderForEachKeyGroup[keyGroup - startGroup].add(keyValueItem);
            }
            LOG.info("keyValueItemIterator end, num: {}", num);

            List<Meta> splitMetas = new ArrayList<>();
            LOG.info("splitMetaBuilders begin");
            for (Meta.MetaBuilder builder : splitMetaBuilders) {
                splitMetas.add(builder.build());
            }
            LOG.info("splitMetaBuilders end");
            this.sortedStat.addSplit();
            return splitMetas;
        } catch (IOException e) {
            throw new BSSRuntimeException("Failed to splitData", e);
        }
    }

    private void updateMetas(Collection<Meta> oldMetas, Collection<Meta> newMetas) {
        for (Meta meta : oldMetas) {
            KVStatesGroup kvStatesGroup = meta.getKVStatesGroup();
            this.startKeyGroupToMeta.remove(kvStatesGroup.getKeyGroupRange().getStartKeyGroup());

            SortUtils.deleteFileQuietly(
                kvStatesGroup.getDataFiles().stream().map(SingleKeyGroupKVState::getPath).collect(Collectors.toList()),
                LOG);
        }

        LOG.info("newMetas size: {}", newMetas.size());
        for (Meta meta : newMetas) {
            if (meta.getKVStatesGroup().getDataFiles().isEmpty()) {
                LOG.info("Skip to add empty meta {}", meta.getKVStatesGroup());
                continue;
            }
            Meta oldMeta =
                this.startKeyGroupToMeta.put(meta.getKVStatesGroup().getKeyGroupRange().getStartKeyGroup(), meta);
            Preconditions.checkState((oldMeta == null), "There should not be old meta for %s", meta.getKVStatesGroup());
        }

        this.sortedStat.updateMeta(oldMetas, newMetas);
    }

    private void cleanupKeyGroup(int keyGroup) {
        List<Meta> cleanupMetas = this.startKeyGroupToMeta.headMap(keyGroup)
            .values()
            .stream()
            .filter(meta -> (meta.getKVStatesGroup().getKeyGroupRange().getEndKeyGroup() < keyGroup))
            .collect(Collectors.toList());

        if (cleanupMetas.isEmpty()) {
            return;
        }

        updateMetas(cleanupMetas, Collections.emptyList());

        LOG.info("Cleanup data triggered key group {}, {}", keyGroup,
            cleanupMetas.stream().map(Meta::getKVStatesGroup).collect(Collectors.toList()));
    }

    private void prepareKeyGroup(int keyGroup) throws IOException {
        LOG.debug("Prepare data for key group {}", keyGroup);
        Meta meta = findKeyGroupMeta(keyGroup);
        while (meta != null) {
            List<Meta> splitMetas;

            if (!meta.getValidKeyGroups().contains(keyGroup) || meta.getValidKeyGroups().size() == 1) {
                return;
            }

            KVStatesGroup kvStatesGroup = meta.getKVStatesGroup();

            LOG.info("Splitting data in {} to {} splits to prepare key group {} zzz", kvStatesGroup,
                this.maxOutputStream, keyGroup);
            long startTime = System.currentTimeMillis();
            try (KVStatesGroup.Reader reader = new KVStatesGroup.Reader(kvStatesGroup)) {
                Iterator<SingleKeyGroupKVState.KeyValueItem> keyValueItemIterator = reader.iterator();
                splitMetas =
                    splitData(keyValueItemIterator, kvStatesGroup.getKeyGroupRange(), this.maxOutputStream, false);
            }
            updateMetas(Collections.singleton(meta), splitMetas);
            LOG.info("Data in {} is split into {} to prepare key group {} in {} ms xxx", kvStatesGroup,
                splitMetas.stream().map(Meta::getKVStatesGroup).collect(Collectors.toList()), keyGroup,
                System.currentTimeMillis() - startTime);
            meta = findKeyGroupMeta(keyGroup);
        }
    }

    private Meta findKeyGroupMeta(int keyGroup) {
        Map.Entry<Integer, Meta> floorEntry = this.startKeyGroupToMeta.floorEntry(keyGroup);
        if (floorEntry == null) {
            return null;
        }

        Meta meta = floorEntry.getValue();
        KeyGroupRange keyGroupRange = meta.getKVStatesGroup().getKeyGroupRange();
        return keyGroupRange.contains(keyGroup) ? meta : null;
    }

    /**
     * close
     *
     * @throws IOException IOException
     */
    @Override
    public void close() throws IOException {
        LOG.info("Closing savepoint iterator: KVDataSorter. basePath: {}", basePath.getName());
        try {
            this.basePath.getFileSystem().delete(this.basePath, true);
        } catch (IOException e) {
            LOG.warn("Failed to delete base path {}", this.basePath.getName(), e);
        }

        LOG.info("Close data sorter with {}, {}", this.totalKeyGroupRange, this.sortedStat);
    }

    static class Stat {
        private final AtomicLong splitCount = new AtomicLong(0L);

        private final AtomicLong createdFileCount = new AtomicLong(0L);

        private final AtomicLong createdFileSize = new AtomicLong(0L);

        private final AtomicLong currentFileCount = new AtomicLong(0L);

        private final AtomicLong currentFileSize = new AtomicLong(0L);

        private final AtomicLong maxFileCount = new AtomicLong(0L);

        private final AtomicLong maxFileSize = new AtomicLong(0L);

        /**
         * addSplit
         */
        public void addSplit() {
            this.splitCount.incrementAndGet();
        }

        /**
         * updateMeta
         *
         * @param oldMetas oldMetas
         * @param newMetas newMetas
         */
        public void updateMeta(Collection<Meta> oldMetas, Collection<Meta> newMetas) {
            Tuple2<Long, Long> newFileTuple = getFileCountAndFileSize(newMetas);
            this.createdFileCount.addAndGet(newFileTuple.f0);
            this.createdFileSize.addAndGet(newFileTuple.f1);
            this.currentFileCount.addAndGet(newFileTuple.f0);
            this.currentFileSize.addAndGet(newFileTuple.f1);
            this.maxFileCount.set(Math.max(this.maxFileCount.get(), this.currentFileCount.get()));
            this.maxFileSize.set(Math.max(this.maxFileSize.get(), this.currentFileSize.get()));

            Tuple2<Long, Long> oldFileTuple = getFileCountAndFileSize(oldMetas);
            this.currentFileCount.addAndGet(-oldFileTuple.f0);
            this.currentFileSize.addAndGet(-oldFileTuple.f1);
        }

        private Tuple2<Long, Long> getFileCountAndFileSize(Collection<Meta> oldMetas) {
            long fileCount = 0L;
            long fileSize = 0L;
            for (Meta meta : oldMetas) {
                KVStatesGroup kvStatesGroup = meta.getKVStatesGroup();
                fileCount += kvStatesGroup.getDataFiles().size();
                for (SingleKeyGroupKVState singleKeyGroupKVState : kvStatesGroup.getDataFiles()) {
                    fileSize += singleKeyGroupKVState.getFileSize();
                }
            }

            return Tuple2.of(fileCount, fileSize);
        }

        /**
         * toString
         *
         * @return String
         */
        public String toString() {
            return "Stat{splitCount=" + this.splitCount + ", createdFileCount=" + this.createdFileCount
                + ", createdFileSize=" + this.createdFileSize + ", currentFileCount=" + this.currentFileCount
                + ", currentFileSize=" + this.currentFileSize + ", maxFileCount=" + this.maxFileCount + ", maxFileSize="
                + this.maxFileSize + '}';
        }
    }
}