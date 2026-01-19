/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.falcon.state.cache.utils;

import java.util.Objects;

import org.apache.flink.table.data.binary.BinaryRowData;

/**
 * Falcon key of RocksDBValueState, which is composed deserialized key and namespace.
 *
 * @brief FALCON implementation
 * @param <K> Type of deserialized key
 * @param <N> Type of deserialized namespace
 */
public class FalconKey<K, N> {
    private final K key;
    private final N namespace;

    // It is needed to be supported that K and N are immutable data type, which should be checked by user.
    // If K or N are modified during running, this.equals() and this.hash() will miscalculate.
    @SuppressWarnings("unchecked")
    public FalconKey(K key, N namespace) {
        if (key instanceof BinaryRowData) { // deep copy K when it is BinaryRowData
            this.key = (K) ((BinaryRowData)key).copy();
        } else {
            this.key = key;
        }
        this.namespace = namespace;
    }

    public K getKey() { return key; }
    public N getNamespace() { return namespace; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FalconKey<?, ?> that = (FalconKey<?, ?>) o;
        return Objects.equals(key, that.key) && Objects.equals(namespace, that.namespace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, namespace);
    }
}
