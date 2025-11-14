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

import com.huawei.ock.bss.common.BinaryKeyValueItem;
import com.huawei.ock.bss.common.BoostStateType;
import com.huawei.ock.bss.common.exception.BSSRuntimeException;
import com.huawei.ock.bss.iterator.CloseableIterator;
import com.huawei.ock.bss.iterator.struct.serializer.BinaryDataBuilder;
import com.huawei.ock.bss.snapshot.FullBoostSnapshotResources;
import com.huawei.ock.bss.snapshot.SavepointConfiguration;
import com.huawei.ock.bss.snapshot.SavepointDBResult;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * KVDataSorterBuilder
 *
 * @param <K> key
 * @since BeiMing 25.0.T1
 */
public class KVDataSorterBuilder<K> {
    private static final Logger LOG = LoggerFactory.getLogger(KVDataSorterBuilder.class);

    private final KeyGroupRange keyGroupRange;

    private final SavepointConfiguration savepointConfiguration;

    private final SavepointDBResult savepointDBResult;

    private final BinaryDataBuilder<K> binaryDataBuilder;

    private final Map<String, FullBoostSnapshotResources.KvMetaData> kvStateMetaDataMap;

    public KVDataSorterBuilder(int totalKeyGroups, KeyGroupRange keyGroupRange, TypeSerializer<K> keySerializer,
        SavepointConfiguration savepointConfiguration,
        Map<String, FullBoostSnapshotResources.KvMetaData> kvStateMetaDataMap, SavepointDBResult savepointDBResult) {
        this.savepointConfiguration = savepointConfiguration;
        this.keyGroupRange = keyGroupRange;
        this.kvStateMetaDataMap = kvStateMetaDataMap;
        this.savepointDBResult = savepointDBResult;
        this.binaryDataBuilder = new BinaryDataBuilder<>(keySerializer, totalKeyGroups);
    }

    /**
     * build
     *
     * @return KVDataSorter
     * @throws Exception Exception
     */
    public KVDataSorter build() throws Exception {
        try (CloseableIterator<BinaryKeyValueItem> binaryIterator = this.savepointDBResult.iterator()) {
            Path basePath = SortUtils.buildSavepointTemporaryPath(
                SavepointConfiguration.decideExternalSortTemporaryPath(this.savepointConfiguration),
                this.savepointDBResult.getSnapshotId());
            LOG.info("Savepoint {} uses temporary path {}", this.savepointDBResult.getSnapshotId(), basePath.getName());

            KVDataSorter keyGroupDataSorter = new KVDataSorter(this.keyGroupRange, basePath,
                this.savepointConfiguration.getExternalSortMaxOutputStream(),
                this.savepointConfiguration.getExternalSortFileSize(),
                this.savepointConfiguration.getExternalSortBlockSize(),
                this.savepointConfiguration.isExternalSortCompressionEnabled(),
                this.savepointConfiguration.isExternalSortChecksumEnabled());

            try {
                KeyValueItemIteratorAdapter itemIteratorAdapter = new KeyValueItemIteratorAdapter(binaryIterator);
                keyGroupDataSorter.init(itemIteratorAdapter);
            } catch (Exception e) {
                LOG.error("Failed to build data sorter for savepoint {}", this.savepointDBResult.getSnapshotId(), e);
                keyGroupDataSorter.close();
                throw new BSSRuntimeException(
                    "Failed to build data sorter for savepoint " + this.savepointDBResult.getSnapshotId(), e);
            }
            LOG.info("Successful to build data sorter for savepoint {}", this.savepointDBResult.getSnapshotId());
            return keyGroupDataSorter;
        }
    }

    /**
     * buildKeyValueItem
     *
     * @param binaryKeyValueItem BinaryKeyValueItem
     * @param reuseKeyValueItem  SingleKeyGroupKVState.KeyValueItem
     * @return SingleKeyGroupKVState.KeyValueItem
     * @throws IOException IOException
     */
    @Nullable
    private SingleKeyGroupKVState.KeyValueItem buildKeyValueItem(BinaryKeyValueItem binaryKeyValueItem,
        @Nullable SingleKeyGroupKVState.KeyValueItem reuseKeyValueItem) throws IOException {
        if (binaryKeyValueItem == null) {
            throw new BSSRuntimeException("binaryKeyValueItem is null.");
        }
        FullBoostSnapshotResources.KvMetaData kvMetaData =
            this.kvStateMetaDataMap.get(binaryKeyValueItem.getStateName());
        if (kvMetaData == null) {
            throw new BSSRuntimeException("kvMetaData is null.");
        }
        Preconditions.checkNotNull(kvMetaData, "Failed to find meta for state %s", binaryKeyValueItem.getStateName());

        byte[] binaryValue = binaryKeyValueItem.getValue();

        StateSnapshotTransformer<byte[]> stateSnapshotTransformer = kvMetaData.stateSnapshotTransformer;
        if (stateSnapshotTransformer != null) {
            binaryValue = stateSnapshotTransformer.filterOrTransform(binaryValue);
            if (binaryValue == null) {
                return null;
            }
        }

        BoostStateType stateType = binaryKeyValueItem.getStateType();
        RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo = kvMetaData.metaInfo;

        byte[] key = buildKey(binaryKeyValueItem, stateType, metaInfo);
        binaryValue = buildValue(binaryValue, stateType, metaInfo);

        SingleKeyGroupKVState.KeyValueItem keyValueItem =
            (reuseKeyValueItem == null) ? new SingleKeyGroupKVState.KeyValueItem() : reuseKeyValueItem;

        keyValueItem.reset(binaryKeyValueItem.getKeyGroup(), kvMetaData.stateId, key, binaryValue);

        return keyValueItem;
    }

    private byte[] buildKey(BinaryKeyValueItem binaryKeyValueItem, BoostStateType stateType,
        RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo) throws IOException {
        int keyGroup = binaryKeyValueItem.getKeyGroup();
        byte[] binaryKey = binaryKeyValueItem.getKey();

        switch (stateType) {
            case VALUE:
            case LIST:
                return this.binaryDataBuilder.formatValueAndListKey(keyGroup, binaryKey);
            case MAP:
                return this.binaryDataBuilder.formatMapKey(keyGroup, binaryKey, getUserKeySerializer(metaInfo),
                    binaryKeyValueItem.getMapKey());

            case SUB_VALUE:
            case SUB_LIST:
                return this.binaryDataBuilder.formatValueAndListKey(keyGroup, binaryKey, metaInfo

                    .getNamespaceSerializer(), binaryKeyValueItem.getNamespace());
            case SUB_MAP:
                return this.binaryDataBuilder.formatNSMapKey(keyGroup, binaryKey, metaInfo

                        .getNamespaceSerializer(), binaryKeyValueItem.getNamespace(), getUserKeySerializer(metaInfo),
                    binaryKeyValueItem.getMapKey());
        }

        throw new UnsupportedOperationException("Unsupported state type " + stateType);
    }

    private byte[] buildValue(byte[] value, BoostStateType stateType,
        RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo) throws IOException {
        switch (stateType) {
            case VALUE:
            case SUB_VALUE:
                return value;
            case LIST:
            case SUB_LIST:
                return this.binaryDataBuilder.formatListValue(getElementSerializer(metaInfo), value);
            case MAP:
            case SUB_MAP:
                return this.binaryDataBuilder.formatUserValueForMapState(getUserValueSerializer(metaInfo), value);
        }
        throw new UnsupportedOperationException("Unsupported state type " + stateType);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private TypeSerializer<Object> getUserKeySerializer(RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo) {
        return ((MapSerializer) metaInfo.getStateSerializer()).getKeySerializer();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private TypeSerializer<Object> getUserValueSerializer(RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo) {
        return ((MapSerializer) metaInfo.getStateSerializer()).getValueSerializer();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private TypeSerializer<Object> getElementSerializer(RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo) {
        return ((ListSerializer) metaInfo.getStateSerializer()).getElementSerializer();
    }

    class KeyValueItemIteratorAdapter implements Iterator<SingleKeyGroupKVState.KeyValueItem> {
        private final CloseableIterator<BinaryKeyValueItem> binaryItemIterator;

        private SingleKeyGroupKVState.KeyValueItem currentItem;

        public KeyValueItemIteratorAdapter(CloseableIterator<BinaryKeyValueItem> binaryItemIterator) {
            this.binaryItemIterator = binaryItemIterator;
            this.currentItem = new SingleKeyGroupKVState.KeyValueItem();
        }

        private void advance() {
            SingleKeyGroupKVState.KeyValueItem reuseItem = this.currentItem;
            this.currentItem = null;
            while (this.currentItem == null && this.binaryItemIterator.hasNext()) {
                try {
                    this.currentItem =
                        KVDataSorterBuilder.this.buildKeyValueItem(this.binaryItemIterator.next(), reuseItem);
                } catch (IOException e) {
                    throw new BSSRuntimeException("Failed to buildKeyValueItem", e);
                }
            }
        }

        /**
         * hasNext
         *
         * @return boolean
         */
        public boolean hasNext() {
            advance();
            return (this.currentItem != null);
        }

        /**
         * next
         *
         * @return SingleKeyGroupKVState.KeyValueItem
         */
        public SingleKeyGroupKVState.KeyValueItem next() {
            return this.currentItem;
        }
    }
}