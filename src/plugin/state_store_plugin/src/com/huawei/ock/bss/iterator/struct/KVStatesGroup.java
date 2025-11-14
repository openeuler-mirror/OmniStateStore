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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

/**
 * KVStatesGroup
 *
 * @since BeiMing 25.0.T1
 */
public class KVStatesGroup {
    private final KeyGroupRange keyGroupRange;

    private final Path basePath;

    private final List<SingleKeyGroupKVState> kvStates;

    public KVStatesGroup(KeyGroupRange keyGroupRange, Path basePath, List<SingleKeyGroupKVState> kvStates) {
        this.keyGroupRange = keyGroupRange;
        this.basePath = basePath;
        this.kvStates = kvStates;
    }

    /**
     * getKeyGroupRange
     *
     * @return KeyGroupRange
     */
    public KeyGroupRange getKeyGroupRange() {
        return this.keyGroupRange;
    }

    /**
     * getBasePath
     *
     * @return Path
     */
    public Path getBasePath() {
        return this.basePath;
    }

    /**
     * List<SingleKeyGroupKVState>
     *
     * @return getDataFiles
     */
    public List<SingleKeyGroupKVState> getDataFiles() {
        return this.kvStates;
    }

    /**
     * toString
     *
     * @return String
     */
    public String toString() {
        return "DataFileGroup{keyGroupRange=" + this.keyGroupRange + ", basePath=" + this.basePath.getName()
            + ", dataFiles="
            + this.kvStates + '}';
    }

    /**
     * Builder
     */
    public static class Builder implements Closeable {
        private static final Logger LOG = LoggerFactory.getLogger(Builder.class);

        private final int blockSize;

        private final long maxFileSize;

        private final boolean compressionEnabled;

        private final boolean checksumEnabled;

        private final Path basePath;

        private final KeyGroupRange keyGroupRange;

        private final List<SingleKeyGroupKVState> singleKeyGroupKVStates;

        private boolean finished;

        @Nullable
        private SingleKeyGroupKVState.Builder currentBuilder;

        public Builder(KeyGroupRange keyGroupRange, Path basePath, long maxFileSize, int blockSize,
            boolean compressionEnabled, boolean checksumEnabled) {
            this.keyGroupRange = keyGroupRange;
            this.basePath = basePath;
            this.maxFileSize = maxFileSize;
            this.blockSize = blockSize;
            this.compressionEnabled = compressionEnabled;
            this.checksumEnabled = checksumEnabled;
            this.singleKeyGroupKVStates = new ArrayList<>();
            this.finished = false;
            this.currentBuilder = null;
            LOG.info("Create builder for data file group {} under {}", keyGroupRange, basePath.getName());
        }

        /**
         * add
         *
         * @param item SingleKeyGroupKVState.KeyValueItem
         * @throws IOException IOException
         */
        public void add(SingleKeyGroupKVState.KeyValueItem item) throws IOException {
            getBuilder().addKeyValue(item);
        }

        /**
         * finish
         *
         * @return KVStatesGroup
         * @throws IOException IOException
         */
        public KVStatesGroup finish() throws IOException {
            if (this.currentBuilder != null) {
                finishBuilder();
            }

            KVStatesGroup kvStatesGroup =
                new KVStatesGroup(this.keyGroupRange, this.basePath, this.singleKeyGroupKVStates);
            this.finished = true;
            LOG.info("Finished build KVStatesGroup, singleKeyGroupKVStates size: {}",
                this.singleKeyGroupKVStates.size());
            return kvStatesGroup;
        }

        /**
         * close
         *
         * @throws IOException IOException
         */
        public void close() throws IOException {
            if (!this.finished) {
                this.singleKeyGroupKVStates.forEach(
                    singleKeyGroupKVState -> SortUtils.deleteFileQuietly(singleKeyGroupKVState.getPath(), LOG));
                this.singleKeyGroupKVStates.clear();

                if (this.currentBuilder != null) {
                    IOUtils.closeQuietly(this.currentBuilder);
                    this.currentBuilder = null;
                }
                LOG.info("Close {} after build failure", this);
            } else {
                LOG.info("Close {} after build success", this);
            }
        }

        private SingleKeyGroupKVState.Builder getBuilder() throws IOException {
            if (this.currentBuilder != null && this.currentBuilder.getFileSize() >= this.maxFileSize) {
                finishBuilder();
            }

            if (this.currentBuilder == null) {
                Path filePath = getNewFilePath();
                LOG.info("Create new data file builder {} for {}", filePath.getName(), this);
                this.currentBuilder =
                    new SingleKeyGroupKVState.Builder(filePath, this.blockSize, this.compressionEnabled,
                        this.checksumEnabled);
            }

            return this.currentBuilder;
        }

        // 整个完成刷一次，获取builder刷一次
        private void finishBuilder() throws IOException {
            if (this.currentBuilder != null) {
                SingleKeyGroupKVState singleKeyGroupKVState = this.currentBuilder.finish();
                LOG.info("SingleKeyGroupKVState path: {}", singleKeyGroupKVState.getPath().getName());
                this.currentBuilder.close();
                this.singleKeyGroupKVStates.add(singleKeyGroupKVState);
                LOG.info("singleKeyGroupKVStates size: {}", this.singleKeyGroupKVStates.size());
                this.currentBuilder = null;
            }
        }

        private Path getNewFilePath() {
            return new Path(this.basePath, UUID.randomUUID().toString());
        }

        /**
         * toString
         *
         * @return String
         */
        public String toString() {
            return "Builder{keyGroupRange=" + this.keyGroupRange + ", basePath=" + this.basePath.getName() + '}';
        }
    }

    /**
     * Reader
     */
    public static class Reader implements Closeable {
        private static final Logger LOG = LoggerFactory.getLogger(Reader.class);

        private final KVStatesGroup kvStatesGroup;

        private final Set<SingleKeyGroupKVState.Reader> openedReaders;

        private final AtomicBoolean iteratorCreated;

        public Reader(KVStatesGroup kvStatesGroup) {
            this.kvStatesGroup = kvStatesGroup;
            this.openedReaders = new HashSet<>();
            this.iteratorCreated = new AtomicBoolean(false);
            LOG.info("Create reader for {}", kvStatesGroup);
        }

        /**
         * iterator
         *
         * @return Iterator<SingleKeyGroupKVState.KeyValueItem>
         * @throws IOException IOException
         */
        public Iterator<SingleKeyGroupKVState.KeyValueItem> iterator() throws IOException {
            if (!this.iteratorCreated.compareAndSet(false, true)) {
                throw new FlinkRuntimeException("Iterator can only be created once for " + this.kvStatesGroup);
            }

            return new Iterator<SingleKeyGroupKVState.KeyValueItem>() {
                private int nextDataFileIndex = 0;

                private SingleKeyGroupKVState.Reader currentReader;

                private Iterator<SingleKeyGroupKVState.KeyValueItem> dataFileIterator = Collections.emptyIterator();

                private void advance() throws IOException {
                    if (this.dataFileIterator.hasNext()) {
                        return;
                    }

                    while (this.nextDataFileIndex < KVStatesGroup.Reader.this.kvStatesGroup.getDataFiles().size()
                        && !this.dataFileIterator.hasNext()) {
                        if (this.currentReader != null) {
                            KVStatesGroup.Reader.this.openedReaders.remove(this.currentReader);
                            this.currentReader.close();
                            this.currentReader = null;
                        }

                        SingleKeyGroupKVState singleKeyGroupKVState =
                            KVStatesGroup.Reader.this.kvStatesGroup.getDataFiles().get(this.nextDataFileIndex);
                        this.currentReader = new SingleKeyGroupKVState.Reader(singleKeyGroupKVState);
                        KVStatesGroup.Reader.this.openedReaders.add(this.currentReader);
                        this.dataFileIterator = this.currentReader.iterator();

                        this.nextDataFileIndex++;
                    }

                    if (!this.dataFileIterator.hasNext()) {
                        this.dataFileIterator = Collections.emptyIterator();
                    }
                }

                /**
                 * hasNext
                 *
                 * @return boolean
                 */
                public boolean hasNext() {
                    try {
                        advance();
                    } catch (IOException e) {
                        throw new BSSRuntimeException(e);
                    }

                    return this.dataFileIterator.hasNext();
                }

                /**
                 * next
                 *
                 * @return SingleKeyGroupKVState.KeyValueItem
                 */
                public SingleKeyGroupKVState.KeyValueItem next() {
                    return this.dataFileIterator.next();
                }
            };
        }

        /**
         * close
         *
         * @throws IOException IOException
         */
        public void close() throws IOException {
            IOUtils.closeAllQuietly(this.openedReaders);
            LOG.info("Close reader for {}", this.kvStatesGroup);
        }
    }
}
