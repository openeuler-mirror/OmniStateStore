package com.huawei.ock.bss.iterator;

import com.huawei.ock.bss.common.BinaryKeyValueItem;
import com.huawei.ock.bss.common.exception.BSSRuntimeException;
import com.huawei.ock.bss.iterator.serializer.KeyValueBuilder;
import com.huawei.ock.bss.snapshot.SavepointDBResult;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.Nullable;

public final class BoostSortedKeyValueIterator<K> implements SingleStateIterator {
    private static final Logger LOG = LoggerFactory.getLogger(BoostSortedKeyValueIterator.class);

    private final SavepointDBResult savepointDBResult;

    private CloseableIterator<BinaryKeyValueItem> iterator;

    private KeyValueItem currentItem;

    private KeyValueBuilder<K> keyValueBuilder;

    private boolean isValid;

    private KeyGroupRange keyGroupRange;

    public BoostSortedKeyValueIterator(SavepointDBResult savepointDBResult, KeyValueBuilder<K> keyValueBuilder,
        KeyGroupRange keyGroupRange) throws IOException {
        this.savepointDBResult = savepointDBResult;
        this.keyValueBuilder = keyValueBuilder;
        this.isValid = false;
        this.iterator = savepointDBResult.iterator();

        this.keyGroupRange = keyGroupRange;
        next();
    }

    @Override
    public void next() throws IOException {
        currentItem = null;
        BinaryKeyValueItem binaryKeyValueItem = null;
        while (currentItem == null && iterator.hasNext()) {
            binaryKeyValueItem = iterator.next();
            currentItem = keyValueBuilder.buildKeyValueItem(binaryKeyValueItem);
        }
        if (currentItem == null) {
            isValid = false;
        } else {
            isValid = true;
        }
    }

    private boolean checkKeyGroup(int keyGroup) {
        return this.keyGroupRange.contains(keyGroup);
    }

    @Override
    public boolean isValid() {
        return isValid;
    }

    @Override
    public byte[] key() {
        return currentItem.getKey();
    }

    @Override
    public byte[] value() {
        return currentItem.getValue();
    }

    @Override
    public int kvStateId() {
        return currentItem.getStateId();
    }

    @Override
    public void seek(final int keyGroup) throws Exception {

    }

    @Override
    public int keyGroup() {
        return currentItem.getKeyGroup();
    }

    @Override
    public boolean isHeapPQState() {
        return false;
    }

    @Override
    public void close() {
        try {
            iterator.close();
        } catch (Exception e) {
            throw new BSSRuntimeException("Failed to close iterator.", e);
        }
    }

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
}