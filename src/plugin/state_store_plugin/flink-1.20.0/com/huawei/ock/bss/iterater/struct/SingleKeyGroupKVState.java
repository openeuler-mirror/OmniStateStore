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

import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.compression.BlockCompressor;
import org.apache.flink.runtime.io.compression.BlockDecompressor;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import javax.annotation.Nullable;

/**
 * SingleKeyGroupKVState
 *
 * @since BeiMing 25.0.T1
 */
public class SingleKeyGroupKVState {
    private final int metaSize;

    private final int numberBlocks;

    private final long metaOffset;

    private final long fileSize;

    private final boolean compressionEnabled;

    private final boolean checksumEnabled;

    private final Path path;

    private final KeyValueState keyValueState;

    public SingleKeyGroupKVState(Path path, boolean compressionEnabled, boolean checksumEnabled, long metaOffset,
        int metaSize, int numberBlocks, long fileSize, KeyValueState keyValueState) {
        this.path = path;
        this.compressionEnabled = compressionEnabled;
        this.checksumEnabled = checksumEnabled;
        this.metaOffset = metaOffset;
        this.metaSize = metaSize;
        this.numberBlocks = numberBlocks;
        this.fileSize = fileSize;
        this.keyValueState = keyValueState;
    }

    /**
     * getPath
     *
     * @return Path
     */
    public Path getPath() {
        return this.path;
    }

    /**
     * isCompressionEnabled
     *
     * @return boolean
     */
    public boolean isCompressionEnabled() {
        return this.compressionEnabled;
    }

    /**
     * isChecksumEnabled
     *
     * @return boolean
     */
    public boolean isChecksumEnabled() {
        return this.checksumEnabled;
    }

    /**
     * getMetaOffset
     *
     * @return long
     */
    public long getMetaOffset() {
        return this.metaOffset;
    }

    /**
     * getMetaSize
     *
     * @return int
     */
    public int getMetaSize() {
        return this.metaSize;
    }

    /**
     * getNumberBlocks
     *
     * @return int
     */
    public int getNumberBlocks() {
        return this.numberBlocks;
    }

    /**
     * getFileSize
     *
     * @return long
     */
    public long getFileSize() {
        return this.fileSize;
    }

    /**
     * getKeyValueStat
     *
     * @return KeyValueState
     */
    public KeyValueState getKeyValueStat() {
        return this.keyValueState;
    }

    /**
     * toString
     *
     * @return String
     */
    public String toString() {
        return "DataFile{path=" + this.path.getName() + ", compressionEnabled=" + this.compressionEnabled
            + ", checksumEnabled=" + this.checksumEnabled + ", metaOffset=" + this.metaOffset + ", metaSize="
            + this.metaSize + ", numberBlocks=" + this.numberBlocks + ", fileSize=" + this.fileSize
            + ", " + this.keyValueState + '}';
    }

    /**
     * Builder
     */
    public static class Builder implements Closeable {
        private static final Logger LOG = LoggerFactory.getLogger(Builder.class);

        private final int blockSize;

        private final boolean checksumEnabled;

        private final boolean compressionEnabled;

        private final Path path;

        @Nullable
        private final CRC32 crc32;

        @Nullable
        private final BlockCompressor blockCompressor;

        private final KeyValueState keyValueState;

        private final ByteArrayOutputStreamWithPos currentBlock;

        private final DataOutputViewStreamWrapper blockOutputView;

        private final DataOutputViewStreamWrapper fileOutputView;

        private final List<SingleKeyGroupKVState.BlockHandle> blockHandles;

        private int currentKeyValueNumber;

        private long fileSize;

        private byte[] reuseCompressedBlock;

        private FSDataOutputStream fileOutputStream;

        public Builder(Path path, int blockSize, boolean compressionEnabled, boolean checksumEnabled) {
            this.path = path;
            this.blockSize = blockSize;
            this.compressionEnabled = compressionEnabled;

            if (compressionEnabled) {
                BlockCompressionFactory compressionFactory = BlockCompressionFactory.createBlockCompressionFactory(
                    NettyShuffleEnvironmentOptions.CompressionCodec.LZ4);
                this.blockCompressor = compressionFactory.getCompressor();
            } else {
                this.blockCompressor = null;
            }

            this.reuseCompressedBlock = new byte[0];
            this.checksumEnabled = checksumEnabled;
            this.crc32 = checksumEnabled ? new CRC32() : null;
            this.currentBlock = new ByteArrayOutputStreamWithPos(blockSize);
            this.blockOutputView = new DataOutputViewStreamWrapper(this.currentBlock);
            this.currentKeyValueNumber = 0;

            try {
                this.fileOutputStream = path.getFileSystem().create(path, FileSystem.WriteMode.NO_OVERWRITE);
            } catch (IOException e) {
                String errMsg = "Failed to create file " + path.getName()
                    + ", make sure current user has permission to read and write in " + path.getName()
                    + ", this error may be caused by config 'state.backend.ockdb.savepoint.sort.local.dir'"
                    + ", if you want to do savepoint"
                    + ", configure a path where current user has permission to read and write"
                    + ", if you did not configure this config, default path is /usr/local/flink/savepoint/tmp";
                LOG.error(errMsg, e);
                SortUtils.deleteFileQuietly(path, LOG);
                throw new BSSRuntimeException(errMsg, e);
            }

            this.fileOutputView = new DataOutputViewStreamWrapper(this.fileOutputStream);
            this.fileSize = 0L;
            this.blockHandles = new ArrayList<>();
            this.keyValueState = new KeyValueState();

            LOG.debug("Create file {}, block size {}, compression {}, checksum {}", path.getName(), blockSize,
                compressionEnabled, checksumEnabled);
        }

        /**
         * getFileSize
         *
         * @return long
         */
        public long getFileSize() {
            return this.fileSize;
        }

        /**
         * addKeyValue
         *
         * @param keyValueItem SingleKeyGroupKVState.KeyValueItem
         * @throws IOException IOException
         */
        public void addKeyValue(SingleKeyGroupKVState.KeyValueItem keyValueItem) throws IOException {
            SingleKeyGroupKVState.KeyValueItem.serialize(keyValueItem, this.blockOutputView);
            this.currentKeyValueNumber++;
            checkAndFlushBlock(false);
            this.keyValueState.update(keyValueItem);
        }

        /**
         * finish
         *
         * @return SingleKeyGroupKVState
         * @throws IOException IOException
         */
        public SingleKeyGroupKVState finish() throws IOException {
            if (this.currentKeyValueNumber > 0) {
                checkAndFlushBlock(true);
            }
            long currMetaOffset = this.fileSize;
            int currMetaSize = writeMeta();
            this.fileSize += currMetaSize;
            closeFileSafety();

            SingleKeyGroupKVState singleKeyGroupKVState =
                new SingleKeyGroupKVState(this.path, this.compressionEnabled, this.checksumEnabled, currMetaOffset,
                    currMetaSize, this.blockHandles.size(), this.fileSize, this.keyValueState);

            LOG.info("Finish file {}", singleKeyGroupKVState);

            return singleKeyGroupKVState;
        }

        /**
         * close
         *
         * @throws IOException IOException
         */
        public void close() throws IOException {
            if (this.fileOutputStream != null) {
                closeFileSafety();
                LOG.info("Close {} after build failure", this.path.getName());
            } else {
                LOG.info("Close {} after build success", this.path.getName());
            }
        }

        private void closeFileSafety() {
            try {
                this.fileOutputStream.close();
            } catch (IOException e) {
                LOG.error("Failed to close file {}", this.path.getName(), e);
                SortUtils.deleteFileQuietly(this.path, LOG);
                throw new BSSRuntimeException("Failed to close file " + this.path.getName(), e);
            } finally {
                this.fileOutputStream = null;
            }
        }

        private void checkAndFlushBlock(boolean force) throws IOException {
            if (force || this.currentBlock.getPosition() >= this.blockSize) {
                LOG.debug("------Flush------");
                LOG.debug("currentBlock pos: {}, blockSize config: {}", this.currentBlock.getPosition(),
                    this.blockSize);
                long blockOffset = this.fileSize;
                int currBlockSize = writeBlock();
                SingleKeyGroupKVState.BlockHandle blockHandle =
                    new SingleKeyGroupKVState.BlockHandle(blockOffset, currBlockSize, this.currentKeyValueNumber);
                this.blockHandles.add(blockHandle);

                this.currentKeyValueNumber = 0;
                this.currentBlock.setPosition(0);
                this.fileSize += currBlockSize;
            }
        }

        private int writeBlock() throws IOException {
            int outputSize;
            byte[] outputData;
            int rawDataSize = this.currentBlock.getPosition();

            if (this.blockCompressor != null) {
                int maxSize = this.blockCompressor.getMaxCompressedSize(rawDataSize);
                if (this.reuseCompressedBlock.length < maxSize) {
                    this.reuseCompressedBlock = new byte[maxSize];
                }
                outputSize =
                    this.blockCompressor.compress(this.currentBlock.getBuf(), 0, this.currentBlock.getPosition(),
                        this.reuseCompressedBlock, 0);
                outputData = this.reuseCompressedBlock;
            } else {
                outputSize = this.currentBlock.getPosition();
                outputData = this.currentBlock.getBuf();
            }

            this.fileOutputView.write(outputData, 0, outputSize);
            if (this.crc32 != null) {
                this.crc32.reset();
                this.crc32.update(outputData, 0, outputSize);
                this.fileOutputView.writeInt((int) this.crc32.getValue());
                outputSize += 4;
            }

            this.fileOutputView.writeInt(rawDataSize);
            outputSize += 4;

            return outputSize;
        }

        private int writeMeta() throws IOException {
            this.currentBlock.setPosition(0);
            this.blockOutputView.writeInt(this.blockHandles.size());
            for (SingleKeyGroupKVState.BlockHandle blockHandle : this.blockHandles) {
                SingleKeyGroupKVState.BlockHandle.serialize(blockHandle, (DataOutputView) this.blockOutputView);
            }

            if (this.crc32 != null) {
                this.crc32.reset();
                this.crc32.update(this.currentBlock.getBuf(), 0, this.currentBlock.getPosition());
                this.blockOutputView.writeInt((int) this.crc32.getValue());
            }

            this.fileOutputView.write(this.currentBlock.getBuf(), 0, this.currentBlock.getPosition());

            return this.currentBlock.getPosition();
        }
    }

    /**
     * Reader
     */
    public static class Reader implements Closeable {
        private static final Logger LOG = LoggerFactory.getLogger(Reader.class);

        @Nullable
        private final CRC32 crc32;

        private final AtomicBoolean iteratorCreated;

        @Nullable
        private final BlockDecompressor blockDecompressor;

        private final DataInputViewStreamWrapper inputView;

        private final SingleKeyGroupKVState singleKeyGroupKVState;

        private final List<SingleKeyGroupKVState.BlockHandle> blockHandles;

        private byte[] reuseBlock;

        private byte[] reuseCompressedBlock;

        private FSDataInputStream inputStream;

        public Reader(SingleKeyGroupKVState singleKeyGroupKVState) throws IOException {
            this.singleKeyGroupKVState = singleKeyGroupKVState;

            if (singleKeyGroupKVState.isCompressionEnabled()) {
                BlockCompressionFactory compressionFactory = BlockCompressionFactory.createBlockCompressionFactory(
                    NettyShuffleEnvironmentOptions.CompressionCodec.LZ4);
                this.blockDecompressor = compressionFactory.getDecompressor();
            } else {
                this.blockDecompressor = null;
            }

            if (singleKeyGroupKVState.isChecksumEnabled()) {
                this.crc32 = new CRC32();
            } else {
                this.crc32 = null;
            }

            this.reuseCompressedBlock = new byte[0];
            this.reuseBlock = new byte[0];

            try {
                this.inputStream =
                    singleKeyGroupKVState.getPath().getFileSystem().open(singleKeyGroupKVState.getPath());
            } catch (IOException e) {
                LOG.error("Failed to open file {}", singleKeyGroupKVState.getPath().getName());
                throw new IOException("Failed to open file {}" + singleKeyGroupKVState.getPath().getName(), e);
            }
            this.inputView = new DataInputViewStreamWrapper(this.inputStream);
            LOG.info("Open data file {}", singleKeyGroupKVState);

            this.blockHandles = new ArrayList<>();
            this.iteratorCreated = new AtomicBoolean(false);
            init();
        }

        private void init() throws IOException {
            try {
                this.inputStream.seek(this.singleKeyGroupKVState.getMetaOffset());
                byte[] metaData = new byte[this.singleKeyGroupKVState.getMetaSize()];
                this.inputView.readFully(metaData);

                ByteArrayInputStreamWithPos byteArrayInputStream = new ByteArrayInputStreamWithPos(metaData);
                DataInputViewStreamWrapper inputViewStreamWrapper =
                    new DataInputViewStreamWrapper(byteArrayInputStream);

                if (this.crc32 != null) {
                    byteArrayInputStream.setPosition(metaData.length - 4);
                    int expected = inputViewStreamWrapper.readInt();
                    this.crc32.reset();
                    this.crc32.update(metaData, 0, metaData.length - 4);
                    int actual = (int) this.crc32.getValue();

                    if (expected != actual) {
                        String errMsg = String.format("Checksum failed for meta in %s, expected %s, actual %s",
                            this.singleKeyGroupKVState.getPath().getName(), expected, actual);
                        LOG.error(errMsg);
                        throw new IOException(errMsg);
                    }
                }

                byteArrayInputStream.setPosition(0);
                inputViewStreamWrapper.readInt();
                for (int i = 0; i < this.singleKeyGroupKVState.getNumberBlocks(); i++) {
                    SingleKeyGroupKVState.BlockHandle blockHandle =
                        SingleKeyGroupKVState.BlockHandle.deserialize((DataInputView) inputViewStreamWrapper);
                    this.blockHandles.add(blockHandle);
                }

                LOG.info("Init reader for data file {}", this.singleKeyGroupKVState.getPath().getName());
            } catch (IOException e) {
                LOG.error("Failed to init reader for data file {}", this.singleKeyGroupKVState.getPath().getName());
                internalClose();
                throw e;
            }
        }

        /**
         * iterator
         *
         * @return Iterator<KeyValueItem>
         */
        public Iterator<KeyValueItem> iterator() {
            if (!this.iteratorCreated.compareAndSet(false, true)) {
                throw new FlinkRuntimeException(
                    "Iterator can only be created once for " + this.singleKeyGroupKVState.getPath().getName());
            }

            return new Iterator<SingleKeyGroupKVState.KeyValueItem>() {
                private int nextBlockIndex = 0;

                private Iterator<SingleKeyGroupKVState.KeyValueItem> blockIterator = Collections.emptyIterator();

                /**
                 * hasNext
                 *
                 * @return boolean
                 */
                public boolean hasNext() {
                    return (this.nextBlockIndex < SingleKeyGroupKVState.Reader.this.blockHandles.size()
                        || this.blockIterator.hasNext());
                }

                /**
                 * next
                 *
                 * @return SingleKeyGroupKVState.KeyValueItem
                 */
                public SingleKeyGroupKVState.KeyValueItem next() {
                    try {
                        while (!this.blockIterator.hasNext()) {
                            SingleKeyGroupKVState.Block block =
                                SingleKeyGroupKVState.Reader.this.readBlock(this.nextBlockIndex++);
                            this.blockIterator = block.iterator();
                        }
                    } catch (IOException e) {
                        SingleKeyGroupKVState.Reader.LOG.error("Failed to iterate key/value pairs in {}",
                            SingleKeyGroupKVState.Reader.this.singleKeyGroupKVState.getPath().getName(), e);
                        throw new BSSRuntimeException("Failed to iterate key/value pairs in "
                            + SingleKeyGroupKVState.Reader.this.singleKeyGroupKVState.getPath().getName(), e);
                    }
                    return this.blockIterator.next();
                }
            };
        }

        /**
         * close
         *
         * @throws IOException IOException
         */
        public void close() throws IOException {
            if (this.inputStream != null) {
                internalClose();
            } else {
                LOG.info("Close unopened reader {}", this.singleKeyGroupKVState.getPath().getName());
            }
        }

        private void internalClose() throws IOException {
            try {
                this.inputStream.close();
                LOG.info("Close reader for {}", this.singleKeyGroupKVState.getPath().getName());
            } catch (IOException e) {
                LOG.error("Failed to close reader for {}", this.singleKeyGroupKVState.getPath().getName());
                throw e;
            } finally {
                this.inputStream = null;
            }
        }

        private SingleKeyGroupKVState.Block readBlock(int blockIndex) throws IOException {
            byte[] rawData;
            SingleKeyGroupKVState.BlockHandle blockHandle = this.blockHandles.get(blockIndex);

            if (this.singleKeyGroupKVState.isCompressionEnabled()) {
                if (this.reuseCompressedBlock.length < blockHandle.getSize()) {
                    this.reuseCompressedBlock = new byte[blockHandle.getSize()];
                }
                rawData = this.reuseCompressedBlock;
            } else {
                if (this.reuseBlock.length < blockHandle.getSize()) {
                    this.reuseBlock = new byte[blockHandle.getSize()];
                }
                rawData = this.reuseBlock;
            }

            try {
                this.inputStream.seek(blockHandle.getOffset());
                this.inputView.readFully(rawData, 0, blockHandle.getSize());
            } catch (IOException e) {
                LOG.error("Failed to read the {} block {} in file {}", blockIndex, blockHandle,
                    this.singleKeyGroupKVState.getPath().getName(), e);
                throw new BSSRuntimeException("Failed to read the " + blockIndex + " block " + blockHandle + " in file "
                    + this.singleKeyGroupKVState.getPath().getName(), e);
            }

            ByteArrayInputStreamWithPos rawInputStream = new ByteArrayInputStreamWithPos(rawData);
            DataInputViewStreamWrapper rawInputView = new DataInputViewStreamWrapper(rawInputStream);

            rawInputStream.setPosition(blockHandle.getSize() - 4);
            int rawDataSize = rawInputView.readInt();

            if (this.crc32 != null) {
                rawInputStream.setPosition(blockHandle.getSize() - 8);
                int expected = rawInputView.readInt();
                this.crc32.reset();
                this.crc32.update(rawData, 0, blockHandle.getSize() - 8);
                int actual = (int) this.crc32.getValue();
                if (expected != actual) {
                    String errMsg =
                        String.format("Checksum failed for the %s block %s in file %s, expected %s, actual %s",
                            blockIndex, blockHandle, this.singleKeyGroupKVState.getPath().getName(), expected, actual);
                    throw new IOException(errMsg);
                }
            }

            if (this.singleKeyGroupKVState.isCompressionEnabled() && this.blockDecompressor != null) {
                if (this.reuseBlock.length < rawDataSize) {
                    this.reuseBlock = new byte[rawDataSize];
                }
                int compressedSize =
                    blockHandle.getSize() - 4 - (this.singleKeyGroupKVState.isChecksumEnabled() ? 4 : 0);
                int decompressedSize =
                    this.blockDecompressor.decompress(this.reuseCompressedBlock, 0, compressedSize, this.reuseBlock, 0);
                if (rawDataSize != decompressedSize) {
                    String errMsg = String.format(
                        "Failed to decompress the %s block %s in file %s, expected size %s, actual size %s", blockIndex,
                        blockHandle, this.singleKeyGroupKVState.getPath().getName(), rawDataSize, decompressedSize);
                    throw new IOException(errMsg);
                }
            }
            return new SingleKeyGroupKVState.Block(this.reuseBlock, blockHandle.getKeyValueNumber());
        }
    }

    /**
     * BlockHandle
     */
    static class BlockHandle {
        private final int size;

        private final int keyValueNumber;

        private final long offset;

        public BlockHandle(long offset, int size, int keyValueNumber) {
            this.offset = offset;
            this.size = size;
            this.keyValueNumber = keyValueNumber;
        }

        /**
         * getOffset
         *
         * @return long
         */
        public long getOffset() {
            return this.offset;
        }

        /**
         * getSize
         *
         * @return int
         */
        public int getSize() {
            return this.size;
        }

        /**
         * getKeyValueNumber
         *
         * @return int
         */
        public int getKeyValueNumber() {
            return this.keyValueNumber;
        }

        /**
         * toString
         *
         * @return String
         */
        public String toString() {
            return "BlockHandle{offset=" + this.offset + ", size=" + this.size + ", keyValueNumber="
                + this.keyValueNumber + '}';
        }

        /**
         * serialize
         *
         * @param blockHandle BlockHandle
         * @param outputView  DataOutputView
         * @throws IOException IOException
         */
        static void serialize(BlockHandle blockHandle, DataOutputView outputView) throws IOException {
            outputView.writeLong(blockHandle.getOffset());
            outputView.writeInt(blockHandle.getSize());
            outputView.writeInt(blockHandle.getKeyValueNumber());
        }

        /**
         * deserialize
         *
         * @param inputView DataInputView
         * @return BlockHandle
         * @throws IOException IOException
         */
        static BlockHandle deserialize(DataInputView inputView) throws IOException {
            return new BlockHandle(inputView.readLong(), inputView.readInt(), inputView.readInt());
        }
    }

    /**
     * Block
     */
    static class Block {
        private final int numberKeyValue;

        private final DataInputViewStreamWrapper inputView;

        public Block(byte[] data, int numberKeyValue) {
            this.numberKeyValue = numberKeyValue;
            this.inputView = new DataInputViewStreamWrapper((InputStream) new ByteArrayInputStreamWithPos(data));
        }

        /**
         * iterator
         *
         * @return Iterator<SingleKeyGroupKVState.KeyValueItem>
         */
        public Iterator<SingleKeyGroupKVState.KeyValueItem> iterator() {
            return new Iterator<SingleKeyGroupKVState.KeyValueItem>() {
                private int nextKeyValueIndex = 0;

                private final SingleKeyGroupKVState.KeyValueItem reuseItem = new SingleKeyGroupKVState.KeyValueItem();

                /**
                 * hasNext
                 *
                 * @return boolean
                 */
                public boolean hasNext() {
                    return (this.nextKeyValueIndex < SingleKeyGroupKVState.Block.this.numberKeyValue);
                }

                /**
                 * next
                 *
                 * @return SingleKeyGroupKVState.KeyValueItem
                 */
                public SingleKeyGroupKVState.KeyValueItem next() {
                    try {
                        this.nextKeyValueIndex++;
                        return SingleKeyGroupKVState.KeyValueItem.deserialize(this.reuseItem,
                            SingleKeyGroupKVState.Block.this.inputView);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            };
        }
    }

    /**
     * KeyValueItem
     */
    public static class KeyValueItem {
        private int keyGroup;

        private int stateId;

        private byte[] key;

        private byte[] value;

        public KeyValueItem() {
        }

        public KeyValueItem(int keyGroup, int stateId, byte[] key, byte[] value) {
            this.keyGroup = keyGroup;
            this.stateId = stateId;
            this.key = key;
            this.value = value;
        }

        /**
         * getKeyGroup
         *
         * @return int
         */
        public int getKeyGroup() {
            return this.keyGroup;
        }

        /**
         * getStateId
         *
         * @return int
         */
        public int getStateId() {
            return this.stateId;
        }

        /**
         * getKey
         *
         * @return byte[]
         */
        public byte[] getKey() {
            return this.key;
        }

        /**
         * getValue
         *
         * @return byte[]
         */
        public byte[] getValue() {
            return this.value;
        }

        /**
         * reset
         *
         * @param keyGroup keyGroup
         * @param stateId  stateId
         * @param key      key
         * @param value    value
         */
        public void reset(int keyGroup, int stateId, byte[] key, byte[] value) {
            this.keyGroup = keyGroup;
            this.stateId = stateId;
            this.key = key;
            this.value = value;
        }

        /**
         * serialize
         *
         * @param item       KeyValueItem
         * @param outputView DataOutputView
         * @throws IOException IOException
         */
        static void serialize(KeyValueItem item, DataOutputView outputView) throws IOException {
            outputView.writeInt(item.getKeyGroup());
            outputView.writeInt(item.getStateId());
            outputView.writeInt((item.getKey()).length);
            outputView.write(item.getKey());
            outputView.writeInt((item.getValue()).length);
            outputView.write(item.getValue());
        }

        /**
         * deserialize
         *
         * @param reuseItem KeyValueItem
         * @param inputView DataInputView
         * @return KeyValueItem
         * @throws IOException IOException
         */
        static KeyValueItem deserialize(@Nullable KeyValueItem reuseItem, DataInputView inputView) throws IOException {
            KeyValueItem item = (reuseItem == null) ? new KeyValueItem() : reuseItem;
            int keyGroup = inputView.readInt();
            int stateId = inputView.readInt();
            int keyLen = inputView.readInt();
            byte[] key = new byte[keyLen];
            inputView.readFully(key);
            int valueLen = inputView.readInt();
            byte[] value = new byte[valueLen];
            inputView.readFully(value);

            item.reset(keyGroup, stateId, key, value);
            return item;
        }

        /**
         * of
         *
         * @param keyGroup keyGroup
         * @param stateId  stateId
         * @param key      key
         * @param value    value
         * @return KeyValueItem
         */
        public static KeyValueItem of(int keyGroup, int stateId, byte[] key, byte[] value) {
            return new KeyValueItem(keyGroup, stateId, key, value);
        }
    }

    /**
     * KeyValueState
     */
    static class KeyValueState {
        private int numKeyValue = 0;

        private long totalKeySize = 0L;

        private long totalValueSize = 0L;

        private long totalKeyValueSize = 0L;

        /**
         * update
         *
         * @param item SingleKeyGroupKVState.KeyValueItem
         */
        public void update(SingleKeyGroupKVState.KeyValueItem item) {
            this.numKeyValue++;
            this.totalKeySize += (item.getKey()).length;
            this.totalValueSize += (item.getValue()).length;
            this.totalKeyValueSize += ((item.getKey()).length + (item.getValue()).length);
        }

        /**
         * toString
         *
         * @return String
         */
        public String toString() {
            return "KeyValueStat{numKeyValue=" + this.numKeyValue + ", totalKeySize=" + this.totalKeySize
                + ", avgKeySize=" + ((this.numKeyValue == 0) ? 0L : (this.totalKeySize / this.numKeyValue))
                + ", totalValueSize=" + this.totalValueSize + ", avgValueSize=" + ((this.numKeyValue == 0)
                ? 0L
                : (this.totalValueSize / this.numKeyValue)) + ", totalKeyValueSize=" + this.totalKeyValueSize
                + ", avgKeyValueSize=" + ((this.numKeyValue == 0) ? 0L : (this.totalKeyValueSize / this.numKeyValue))
                + '}';
        }
    }
}
