/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#ifndef FALCON_FALCONCACHE_H
#define FALCON_FALCONCACHE_H

#include "FalconMap.h"
#include <jni.h>
#include <iostream>
#include <rocksdb/db.h>
#include <unordered_map>

#ifndef NN_LOG_FILENAME
#define NN_LOG_FILENAME (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#endif
#define FALCON_LOG(args)                                                                            \
    do {                                                                                            \
        std::cout << "[FALCON " << NN_LOG_FILENAME << ":" << __LINE__ << "] " << args << std::endl; \
    } while (0)                                                                                     \

/**
 * Falcon cache interface, which provides API to access falcon cache together with rocksdb. The key-value state can be
 * get, put and remove from falcon cache, only when falcon cache miss, falcon cache will then access rocksdb.
 * <p> Besides, we also expose some API for falcon cache to flush all Key-Value states to rocksdb or flush the coldest
 * state to rocksdb and then remove it from falcon cache.
 * <p> Each falcon cache has its own size limit, which can be got or set by the exposed API.
 */
class FalconCache {
public:
    FalconCache(double hitThreshold) : accessCnt(0), hitCnt(0), hitThreshold(hitThreshold), cacheSizeLimit(0) {}

    ~FalconCache()
    {
        clearAll();  // free memory and clear cache
    };

    /**
     * Return the value corresponding to the given key_slice. If cache elimination fires, writeOptionsHandle is needed
     * to flush state to rocksdb.
     * <p> If key_slice has already been in falcon cache, directly get value from falcon cache (just transfer it from
     * Slice to jByteArray); If falcon cache miss, get value from RocksDB and insert into cache (just transfer it from
     * String to slice).
     * <p> key-value state will be mark as hot after each cache get process, which is done by LinkedHashMap.
     *
     * @param env pointer of com.huawei.falcon.state.FalconValueState
     * @param rocksdbHandle JAVA rocksdb object handle
     * @param cfHandle JAVA column family handle
     * @param writeOptionsHandle JAVA rocksdb write options handle, which is used when eliminating state to rocksdb
     * @param key_slice slice of key used to access cache and rocksdb
     * @return value corresponding to the given key_slice
     */
    jbyteArray get(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
                   ROCKSDB_NAMESPACE::Slice key_slice);

    /**
     * Write the given key-value pair into falcon cache and rocksdb.
     * <p> If key_slice has already been in falcon cache, update key-value pair in falcon cache; If falcon cache miss,
     * directly insert key-value pair into falcon cache. When cache size reaches upper limit, eliminate the coldest
     * state into rocksdb (state in falcon cache is always dirty).
     * <p> key-value state will be mark as hot after each cache put process, which is done by LinkedHashMap.
     *
     * @param env pointer of com.huawei.falcon.state.FalconValueState
     * @param rocksdbHandle JAVA rocksdb object handle
     * @param cfHandle JAVA column family handle
     * @param writeOptionsHandle JAVA rocksdb write options handle
     * @param key_slice slice of key used to access cache and rocksdb
     * @param value_slice slice of value used to access cache and rocksdb
     */
    void put(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
             ROCKSDB_NAMESPACE::Slice key_slice, ROCKSDB_NAMESPACE::Slice value_slice);

    /**
     * Remove the given key_slice from falcon and rocksdb
     *
     * @param env pointer of com.huawei.falcon.state.FalconValueState
     * @param rocksdbHandle JAVA rocksdb object handle
     * @param cfHandle JAVA column family handle
     * @param writeOptionsHandle JAVA rocksdb write options handle
     * @param key_slice slice of key used to access cache and rocksdb
     */
    void remove(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
                ROCKSDB_NAMESPACE::Slice key_slice);

    /**
     * Flush all the states from falcon cache to rocksdb. Note that all the states in cache are always dirty.
     *
     * @param env pointer of com.huawei.falcon.state.FalconValueState
     * @param rocksdbHandle JAVA rocksdb object handle
     * @param cfHandle JAVA column family handle
     * @param writeOptionsHandle JAVA rocksdb write options handle
     */
    void flush(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle);

    /**
     * Clear all the states in falcon cache, and free their memory
     */
    void clearAll();

    /**
     * When cache size reach upper size limit, flush the coldest state to RocksDB and then remove it from falcon cache.
     * Note that we defensively check whether cache size exceeds upper size limit a lot, if so, flush and clear cache.
     *
     * @param env pointer of com.huawei.falcon.state.FalconValueState
     * @param rocksdbHandle JAVA rocksdb object handle
     * @param cfHandle JAVA column family handle
     * @param writeOptionsHandle JAVA rocksdb write options handle
     */
    void removeEldestState(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle);

    /**
     * Get falcon cache size limit.
     *
     * @return cache size limit
     */
    int getSizeLimit() const;

    /**
     * Update falcon cache size limit, if new cache size limit reaches upper limit, flush all the state into rocksdb and
     * then clear the cache.
     *
     * @param env pointer of com.huawei.falcon.state.FalconValueState
     * @param rocksdbHandle JAVA rocksdb object handle
     * @param cfHandle JAVA column family handle
     * @param writeOptionsHandle JAVA rocksdb write options handle
     * @param newSizeLimit the new size limit of the cache.
     */
    void updateSizeLimit(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle, jint newSizeLimit);

    /**
     * Decide whether to bypass falcon cache. If falcon cache hitRatio is less than state.backend.rocksdb.falcon.
     * state-cache-bypass-hitRatio, then disable falcon cache.
     *
     * @return true means we are going to bypass falcon cache.
     */
    bool bypassCache() const;

private:
    unsigned long long accessCnt{}; // frequency of accessing falcon cache, use this type (2^64) to avoid overflow
    unsigned long long hitCnt{}; // frequency of hitting falcon cache
    double hitThreshold{}; // if cache hit ratio is less than hitThreshold, bypass falcon cache
    int cacheSizeLimit{}; // stateCache size limit, 0 means disable falcon cache, otherwise enable falcon cache
    std::unordered_map<ROCKSDB_NAMESPACE::Slice, ROCKSDB_NAMESPACE::Slice, FalconHash<ROCKSDB_NAMESPACE::Slice>,
        FalconEqual<ROCKSDB_NAMESPACE::Slice, ROCKSDB_NAMESPACE::Slice>> cache = {};
};

#endif // FALCON_FALCONCACHE_H
