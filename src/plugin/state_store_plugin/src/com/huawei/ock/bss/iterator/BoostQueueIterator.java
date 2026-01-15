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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueStateSnapshot;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Iterator;

/**
 * pqState迭代器
 *
 * @since BeiMing 25.0.T1
 */
public final class BoostQueueIterator implements SingleStateIterator {
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final DataOutputSerializer keyOut = new DataOutputSerializer(128);

    private final HeapPriorityQueueStateSnapshot<?> queueSnapshot;

    private final Iterator<Integer> keyGroupRangeIterator;

    private final int kvStateId;

    private final int keyGroupPrefixBytes;

    private final TypeSerializer<Object> elementSerializer;

    private Iterator<Object> elementsForKeyGroup;

    private int afterKeyMark = 0;

    private boolean isValid;

    private byte[] currentKey;

    private int currentKeyGroup;

    public BoostQueueIterator(
        HeapPriorityQueueStateSnapshot<?> queuesSnapshot,
        KeyGroupRange keyGroupRange,
        int keyGroupPrefixBytes,
        int kvStateId) {
        this.queueSnapshot = queuesSnapshot;
        this.elementSerializer = castToType(queuesSnapshot.getMetaInfo().getElementSerializer());
        this.keyGroupRangeIterator = keyGroupRange.iterator();
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.kvStateId = kvStateId;
        if (keyGroupRangeIterator.hasNext()) {
            try {
                if (moveToNextNonEmptyKeyGroup()) {
                    isValid = true;
                    next();
                } else {
                    isValid = false;
                }
            } catch (IOException e) {
                throw new FlinkRuntimeException(e);
            }
        }
    }

    @Override
    public void next() {
        try {
            if (!elementsForKeyGroup.hasNext()) {
                boolean hasElement = moveToNextNonEmptyKeyGroup();
                if (!hasElement) {
                    isValid = false;
                    return;
                }
            }
            keyOut.setPosition(afterKeyMark);
            elementSerializer.serialize(elementsForKeyGroup.next(), keyOut);
            this.currentKey = keyOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    private boolean moveToNextNonEmptyKeyGroup() throws IOException {
        while (keyGroupRangeIterator.hasNext()) {
            Integer keyGroupId = keyGroupRangeIterator.next();
            currentKeyGroup = keyGroupId;
            elementsForKeyGroup = castToType(queueSnapshot.getIteratorForKeyGroup(keyGroupId));
            if (elementsForKeyGroup.hasNext()) {
                writeKeyGroupId(keyGroupId);
                return true;
            }
        }
        return false;
    }

    private void writeKeyGroupId(Integer keyGroupId) throws IOException {
        keyOut.clear();
        CompositeKeySerializationUtils.writeKeyGroup(keyGroupId, keyGroupPrefixBytes, keyOut);
        afterKeyMark = keyOut.length();
    }

    @SuppressWarnings("unchecked")
    private static <T> TypeSerializer<T> castToType(TypeSerializer<?> typeSerializer) {
        return (TypeSerializer<T>) typeSerializer;
    }

    @SuppressWarnings("unchecked")
    private static <T> Iterator<T> castToType(Iterator<?> iterator) {
        return (Iterator<T>) iterator;
    }

    @Override
    public boolean isValid() {
        return isValid;
    }

    @Override
    public byte[] key() {
        return currentKey;
    }

    @Override
    public byte[] value() {
        return EMPTY_BYTE_ARRAY;
    }

    @Override
    public int kvStateId() {
        return kvStateId;
    }

    @Override
    public void seek(int keyGroup) throws Exception {

    }

    @Override
    public int keyGroup() {
        return currentKeyGroup;
    }

    @Override
    public boolean isHeapPQState() {
        return true;
    }

    @Override
    public void close() {}
}
