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

package com.huawei.ock.bss.iterator;

import com.huawei.ock.bss.common.exception.BSSRuntimeException;
import com.huawei.ock.bss.snapshot.FullBoostSnapshotResources;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * pqState迭代器
 *
 * @since BeiMing 25.0.T1
 */
public final class BoostQueueIterator implements SingleStateIterator {
    private static final Logger LOG = LoggerFactory.getLogger(BoostQueueIterator.class);

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final int keyGroupPrefixBytes;

    private final DataOutputSerializer keyOutput;

    private final Map<String, FullBoostSnapshotResources.PqMetaData> pqMetaDataMap;

    private int afterKeyMark;

    private int currentKeyGroup;

    private byte[] currentKey;

    private boolean isValid;

    private Iterator<?> stateIterator;

    private Iterator<FullBoostSnapshotResources.PqMetaData> metaDataIterator;

    private FullBoostSnapshotResources.PqMetaData currentMetaData;

    public BoostQueueIterator(Map<String, FullBoostSnapshotResources.PqMetaData> pqMetaDataMap,
        int totalKeyGroups) {
        this.pqMetaDataMap = pqMetaDataMap;
        this.currentKeyGroup = -1;
        this.keyOutput = new DataOutputSerializer(128);
        this.keyGroupPrefixBytes = CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(totalKeyGroups);
        reset();
        LOG.info("Succeed to build BoostQueueIterator.");
    }

    @Override
    public void next() throws IOException {
        while (!this.stateIterator.hasNext() && this.metaDataIterator.hasNext()) {
            this.currentMetaData = this.metaDataIterator.next();
            this.stateIterator = this.currentMetaData.stateSnapshot.getIteratorForKeyGroup(this.currentKeyGroup);
        }

        if (!this.stateIterator.hasNext()) {
            reset();
            return;
        }

        this.keyOutput.setPosition(this.afterKeyMark);
        castToType(this.currentMetaData.stateSnapshot.getMetaInfo().getElementSerializer()).serialize(
            this.stateIterator.next(), this.keyOutput);
        this.currentKey = this.keyOutput.getCopyOfBuffer();
        this.isValid = true;
    }

    @Override
    public boolean isValid() {
        return this.isValid;
    }

    @Override
    public byte[] key() {
        return this.currentKey;
    }

    @Override
    public byte[] value() {
        return EMPTY_BYTE_ARRAY;
    }

    @Override
    public int kvStateId() {
        return this.currentMetaData.stateId;
    }

    /**
     * seek
     *
     * @param keyGroup keyGroup
     * @throws IOException IOException
     */
    @Override
    public void seek(int keyGroup) throws IOException {
        if (this.currentKeyGroup >= keyGroup) {
            throw new BSSRuntimeException("Failed to iterate state because not in the upper order of key groups.");
        }

        this.currentKeyGroup = keyGroup;
        this.keyOutput.setPosition(0);
        CompositeKeySerializationUtils.writeKeyGroup(this.currentKeyGroup, this.keyGroupPrefixBytes, this.keyOutput);
        this.afterKeyMark = this.keyOutput.length();

        this.metaDataIterator = this.pqMetaDataMap.values().iterator();
        this.stateIterator = Collections.emptyIterator();
        next();
    }

    /**
     * close
     */
    @Override
    public void close() {
        LOG.info("Closing BoostQueueIterator.");
    }

    private void reset() {
        this.isValid = false;
        this.currentKey = null;
        this.currentMetaData = null;
        this.metaDataIterator = Collections.emptyIterator();
        this.stateIterator = Collections.emptyIterator();
    }

    @SuppressWarnings("unchecked")
    private static <T> TypeSerializer<T> castToType(TypeSerializer<?> typeSerializer) {
        return (TypeSerializer<T>) typeSerializer;
    }
}
