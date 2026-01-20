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

package com.huawei.falcon.state.cache.api;

import org.apache.flink.api.common.state.State;
import com.huawei.falcon.state.cache.utils.FalconKey;
import com.huawei.falcon.state.cache.utils.FalconValue;

/**
 * Falcon value state cache interface, which provide API for upper-level flink app to access falcon cache and db. The
 * {@link FalconKey}-{@link FalconValue} state can be read, added, updated and retrieved from falcon cache. Only when
 * falcon cache misses, falcon cache will then access RocksDB.
 *
 * <p> Besides, we also expose some API for falcon cache to flush all dirty Key-Value states to RocksDB or flush the
 * coldest state to RocksDB and then remove it from falcon cache.
 *
 * <p> Each falcon cache has its own size limit, which can be got or set by the exposed API.
 *
 * <p> In Flink, each columnFamily will instantiate a {@link State}. In our Falcon system, each {@link State} holds a
 * falcon cache, which only store states under this columnFamily. Thus, input parameters does not contain columnFamily.
 *
 * @brief FALCON implementation
 * @param <K> Type of ValueState's key
 * @param <N> Type of ValueState's namespace
 * @param <V> Type of ValueState's value
 */
public interface FalconValueStateAPI<K, N, V> {
    /**
     * Returns the current value of the given key and namespace (i.e., a FalconKey)
     * <p> Case1: if FalconKey has already been in falcon cache, directly get value from falcon cache;
     * <p> Case2: if falcon cache miss, get value from RocksDB and insert (value, isDirty=false) into cache;
     * <p> If we successfully get FalconValue, mark the state as hot state (done by LinkedHashMap).
     *
     * @param key key of the state to be read
     * @param namespace namespace of the state to be read
     * @return value corresponding to the given key and namespace
     */
    V get(K key, N namespace);

    /**
     * Write the given key-value pair into falcon cache, compose key and namespace as FalconKey, compose value and
     * isDirty flag as FalconValue.
     * <p> Case1: if FalconKey has already been in falcon cache, directly update FalconValue in cache and mark as dirty
     * <p> Case2: if falcon cache miss, write key-value pair to RocksDB, then insert FalconKey-FalconValue pair into
     * cache and mark as clean
     * <p> After the state has been written, mark the state as hot state.
     *
     * @param key key of the state to be written
     * @param namespace namespace of the state to be written
     * @param value value of the state to be written
     */
    void put(K key, N namespace, V value);

    /**
     * Remove the given key and namespace (FalconKey) from falcon cache and RocksDB
     *
     * @param key key of the state to be removed
     * @param namespace namespace of the state to be removed
     */
    void remove(K key, N namespace);

    /**
     * Flush all the dirty state from falcon cache to RocksDB.
     */
    void flush();

    /**
     * Clear all the state in falcon cache
     */
    void clearAll();

    /**
     * When cache size reaches upper size limit, flush the coldest state to RocksDB if needed and then remove it from
     * falcon cache. Note that we defensively check whether cache size exceeds upper size limit a lot, if so, flush and
     * clear falcon cache.
     */
    void removeEldestState();

    /**
     * Get cache size limit.
     *
     * @return cache size limit
     */
    int getCacheSizeLimit();

    /**
     * Update cache size limit.
     *
     * @param newSizeLimit the new size limit of the cache.
     */
    void updateCacheSizeLimit(int newSizeLimit);

    /**
     * Decide whether to bypass falcon cache. If falcon cache hitRatio is less than state.backend.rocksdb.falcon.
     * state-cache-bypass-hitRatio, then disable falcon cache.
     *
     * @return true means we are going to bypass falcon cache.
     */
    boolean bypassCache();
}
