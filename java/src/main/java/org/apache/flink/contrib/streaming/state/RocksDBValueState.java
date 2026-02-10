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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.util.FlinkRuntimeException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;

import com.huawei.falcon.state.cache.FalconException;
import com.huawei.falcon.state.cache.FalconValueState;

/**
 * {@link ValueState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
public class RocksDBValueState<K, N, V> extends AbstractRocksDBState<K, N, V>
        implements InternalValueState<K, N, V> {

    // -------------------------------- FALCON Implementation --------------------------------
    FalconValueState falcon;  // falcon backend, which is a native object
    // ---------------------------------------------------------------------------------------

    /**
     * Creates a new {@code RocksDBValueState}.
     *
     * @param columnFamily The RocksDB column family that this state is associated to.
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    private RocksDBValueState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue,
            RocksDBKeyedStateBackend<K> backend) {

        super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
        this.falcon = new FalconValueState(backend.db);
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public V value() {
        try {
            byte[] valueBytes;
            if (falcon.isFalconCacheOpen()) {
                valueBytes = falcon.get(columnFamily, writeOptions, serializeCurrentKeyWithGroupAndNamespace());
            } else {
                valueBytes = backend.db.get(columnFamily, serializeCurrentKeyWithGroupAndNamespace());
            }

            if (valueBytes == null) {
                return getDefaultValue();
            }
            dataInputView.setBuffer(valueBytes);
            return valueSerializer.deserialize(dataInputView);
        } catch (IOException | RocksDBException | FalconException e) {
            throw new FlinkRuntimeException("Error while retrieving data from Falcon and RocksDB.", e);
        }
    }

    @Override
    public void update(V value) {
        if (value == null) {
            clear();
            return;
        }

        try {
            if (falcon.isFalconCacheOpen()) {
                falcon.put(
                        columnFamily,
                        writeOptions,
                        serializeCurrentKeyWithGroupAndNamespace(),
                        serializeValue(value));
            } else {
                backend.db.put(
                        columnFamily,
                        writeOptions,
                        serializeCurrentKeyWithGroupAndNamespace(),
                        serializeValue(value));
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Error while adding data to RocksDB", e);
        }
    }

    public void clear() {
        try {
            if (falcon.isFalconCacheOpen()) {
                falcon.delete(columnFamily, writeOptions, serializeCurrentKeyWithGroupAndNamespace());
            } else {
                backend.db.delete(columnFamily, writeOptions, serializeCurrentKeyWithGroupAndNamespace());
            }

        } catch (RocksDBException | FalconException e) {
            throw new FlinkRuntimeException("Error while removing entry from RocksDB", e);
        }
    }

    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<V> safeValueSerializer)
            throws Exception {

        // TODO make KvStateSerializer key-group aware to save this round trip and key-group
        // computation
        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

        int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(
                        keyAndNamespace.f0, backend.getNumberOfKeyGroups());

        SerializedCompositeKeyBuilder<K> keyBuilder =
                new SerializedCompositeKeyBuilder<>(
                        safeKeySerializer, backend.getKeyGroupPrefixBytes(), 32);
        keyBuilder.setKeyAndKeyGroup(keyAndNamespace.f0, keyGroup);
        byte[] key = keyBuilder.buildCompositeKeyNamespace(keyAndNamespace.f1, namespaceSerializer);
        if (falcon.isFalconCacheOpen()) {
            return falcon.get(columnFamily, writeOptions, key);
        } else {
            return backend.db.get(columnFamily, key);
        }
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            RocksDBKeyedStateBackend<K> backend) {
        return (IS)
                new RocksDBValueState<>(
                        registerResult.f0,
                        registerResult.f1.getNamespaceSerializer(),
                        registerResult.f1.getStateSerializer(),
                        stateDesc.getDefaultValue(),
                        backend);
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            IS existingState) {
        return (IS)
                ((RocksDBValueState<K, N, SV>) existingState)
                        .setNamespaceSerializer(registerResult.f1.getNamespaceSerializer())
                        .setValueSerializer(registerResult.f1.getStateSerializer())
                        .setDefaultValue(stateDesc.getDefaultValue());
    }
}
