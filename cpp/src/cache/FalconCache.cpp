/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#include "FalconCache.h"
#include "FalconRocksDBHelper.h"

const int CACHE_SIZE_UPPER_LIMIT_RATIO = 2;
const int BYPASS_CHECK_PERIOD = 20000;

jbyteArray FalconCache::get(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
                            ROCKSDB_NAMESPACE::Slice key_slice)
{
    accessCnt++;
    auto state_pos = cache.find(key_slice);
    if (state_pos != cache.end()) { // falcon cache hit, get value from cache
        hitCnt++;

        // value in cache will not be null all the time
        ROCKSDB_NAMESPACE::Slice value_slice = state_pos->second;
        jbyteArray jVal = FalconUtil::JniUtil::copyBytes(env, value_slice.ToString());

        // if falcon cache hit, directly delete key_slice
        delete[] key_slice.data_;
        key_slice.data_ = nullptr;  // defensive programming

        return jVal;
    } else { // falcon cache miss, get from rocksdb and insert into falcon cache
        jbyteArray jVal = rocksdb_get(env, rocksdbHandle, cfHandle, key_slice);

        if (jVal == nullptr) {
            // falcon cache miss, but value in rocksdb is null, do not insert it into cache, and delete key_slice
            delete[] key_slice.data_;
            key_slice.data_ = nullptr;
        } else {
            // transfer jVal into Slice type, and insert into falcon cache
            jsize jValLen = env->GetArrayLength(jVal);
            jbyte* value = new jbyte[jValLen];
            env->GetByteArrayRegion(jVal, 0, jValLen, value);
            if (env->ExceptionCheck()) {  // exception thrown: ArrayIndexOutOfBoundsException
                delete[] value;
                delete[] key_slice.data_;
                value = nullptr;
                key_slice.data_ = nullptr;  // defensive programming
                return nullptr;
            }

            // insert key_slice and value_slice into cache
            ROCKSDB_NAMESPACE::Slice value_slice(reinterpret_cast<char*>(value), jValLen);
            cache.emplace(key_slice, value_slice);

            // key_slice and value_slice will be deleted in removeEldestState() when cache elimination fires
            if (cache.size() > cacheSizeLimit) {
                removeEldestState(env, rocksdbHandle, cfHandle, writeOptionsHandle);
            }
        }

        return jVal;
    }
}

void FalconCache::put(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
                      ROCKSDB_NAMESPACE::Slice key_slice, ROCKSDB_NAMESPACE::Slice value_slice)
{
    accessCnt++;
    auto state_pos = cache.find(key_slice);
    if (state_pos != cache.end()) { // falcon cache hit, update key_slice and value_slice inside falcon cache
        hitCnt++;

        delete[] state_pos->first.data_;
        delete[] state_pos->second.data_;
        state_pos->second.data_ = nullptr;  // key.data_ can not be set to null
        cache.erase(state_pos);
        cache.emplace(key_slice, value_slice);
    } else { // falcon cache hit, insert key_slice and value_slice into cache
        cache.emplace(key_slice, value_slice);
        // key_slice and value_slice will be deleted in removeEldestState() when cache elimination fires
        if (cache.size() > cacheSizeLimit) {
            removeEldestState(env, rocksdbHandle, cfHandle, writeOptionsHandle);
        }
    }
}

void FalconCache::remove(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
                         ROCKSDB_NAMESPACE::Slice key_slice)
{
    accessCnt++;
    auto state_pos = cache.find(key_slice);
    if (state_pos != cache.end()) { // falcon cache hit, remove key_slice and value_slice from cache
        hitCnt++;

        delete[] state_pos->first.data_;
        delete[] state_pos->second.data_;
        state_pos->second.data_ = nullptr;  // key.data_ can not be set to null
        cache.erase(state_pos);
    }
    // remove key_slice and value_slice from rocksdb
    rocksdb_delete(env, rocksdbHandle, cfHandle, writeOptionsHandle, key_slice);
    delete[] key_slice.data();
    key_slice.data_ = nullptr;
}

void FalconCache::flush(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle)
{
    for (auto &[key_slice, val_slice] : cache) {
        rocksdb_put(env, rocksdbHandle, cfHandle, writeOptionsHandle, key_slice, val_slice);
    }
}

void FalconCache::clearAll()
{
    for (auto &[key_slice, value_slice] : cache) {
        delete[] key_slice.data_;
        delete[] value_slice.data_;
        value_slice.data_ = nullptr;  // key.data_ can not be set to null
    }
    cache.clear();
    hitCnt = 0;
    accessCnt = 0;
}

void FalconCache::removeEldestState(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle)
{
    if (cache.size() > CACHE_SIZE_UPPER_LIMIT_RATIO * cacheSizeLimit) { // defensive programming
        flush(env, rocksdbHandle, cfHandle, writeOptionsHandle);
        clearAll();
    } else {
        // get the coldest state, put it into rocksdb, and then remove it from falcon cache
        auto &key_slice = cache.begin()->first;
        auto &val_slice = cache.begin()->second;
        rocksdb_put(env, rocksdbHandle, cfHandle, writeOptionsHandle, key_slice, val_slice);

        delete[] key_slice.data_;
        delete[] val_slice.data_;
        val_slice.data_ = nullptr;  // key.data_ can not be set to null
        cache.erase(cache.begin());
    }
}

int FalconCache::getSizeLimit() const
{
    return cacheSizeLimit;
}

void FalconCache::updateSizeLimit(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
                                  jint newSizeLimit)
{
    cacheSizeLimit = newSizeLimit;
    if (cache.size() > cacheSizeLimit) {
        flush(env, rocksdbHandle, cfHandle, writeOptionsHandle);
        clearAll();
    }
}

bool FalconCache::bypassCache() const
{
    // perform hit ratio check every 2w times (avoid too much calculating), then decide whether to bypass cache
    if (accessCnt % BYPASS_CHECK_PERIOD == 0) {
        double hitRatio = static_cast<double>(hitCnt) / static_cast<double>(accessCnt);
        if (hitRatio < hitThreshold) {
            FALCON_LOG("cache hit ratio is less than bypass threshold, bypass cache. (" << hitCnt << "/" << accessCnt <<
                       " = " << hitRatio << " < " << hitThreshold << ")");
            return true;
        } else {
            return false;
        }
    }
    return false;
}