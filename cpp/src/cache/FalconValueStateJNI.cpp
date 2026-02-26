/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#include "com_huawei_falcon_state_cache_FalconValueState.h"
#include "FalconRocksDBHelper.h"
#include "FalconCache.h"

extern "C" {

JNIEXPORT jlong JNICALL Java_com_huawei_falcon_state_cache_FalconValueState_initFalconCache(JNIEnv *env, jobject obj,
    jdouble cacheBypassThreshold)
{
    auto cache = new FalconCache(cacheBypassThreshold);
    FALCON_LOG("successfully init falcon cache object. If cache hitRatio < " << cacheBypassThreshold << ", falcon cache"
                " will be bypassed.");
    return reinterpret_cast<jlong>(cache);
}

JNIEXPORT void JNICALL Java_com_huawei_falcon_state_cache_FalconValueState_destroyFalconCache(JNIEnv *env, jobject obj,
    jlong falconHandle)
{
    auto cache = reinterpret_cast<FalconCache*>(falconHandle);
    delete cache;
    FALCON_LOG("successfully destroy falcon cache object.");
}

JNIEXPORT jbyteArray JNICALL Java_com_huawei_falcon_state_cache_FalconValueState_get(JNIEnv *env, jobject obj,
    jlong falconHandle, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
    jbyteArray jKey, jint jKeyOffset, jint jKeyLen)
{
    // construct key slice, key, i.e., key_slice.data() will be delete inside falcon_cache->get()
    jbyte* key = new jbyte[jKeyLen];
    env->GetByteArrayRegion(jKey, jKeyOffset, jKeyLen, key);
    if (env->ExceptionCheck()) {  // exception thrown: ArrayIndexOutOfBoundsException
        delete[] key;
        return nullptr;
    }
    ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jKeyLen);

    auto falcon_cache = reinterpret_cast<FalconCache*>(falconHandle);
    if (falcon_cache != nullptr) {
        jbyteArray value = falcon_cache->get(env, rocksdbHandle, cfHandle, writeOptionsHandle, key_slice);
        if (falcon_cache->bypassCache()) {
            falcon_cache->updateSizeLimit(env, rocksdbHandle, cfHandle, writeOptionsHandle, 0);
        }
        return value;
    } else {
        FalconException::FalconExceptionJni::ThrowNew(env, "[FALCON] invalid falcon cache handle when getting state.",
            ROCKSDB_NAMESPACE::Status::InvalidArgument("Invalid Falcon Cache Handle."));
        return nullptr;
    }
}

JNIEXPORT void JNICALL Java_com_huawei_falcon_state_cache_FalconValueState_put(JNIEnv *env, jobject obj,
    jlong falconHandle, jlong rocksdbHandle, jlong cfHandle, jlong writeOptHandle,
    jbyteArray jKey, jint jKeyOffset, jint jKeyLen, jbyteArray jVal, jint jValOffset, jint jValLen)
{
    // construct key and value slice, key and value will be deleted inside falcon_cache->put()
    jbyte* key = new jbyte[jKeyLen];
    env->GetByteArrayRegion(jKey, jKeyOffset, jKeyLen, key);
    if (env->ExceptionCheck()) {  // exception thrown: ArrayIndexOutOfBoundsException
        delete[] key;
    }
    jbyte* value = new jbyte[jValLen];
    env->GetByteArrayRegion(jVal, jValOffset, jValLen, value);
    if (env->ExceptionCheck()) {  // exception thrown: ArrayIndexOutOfBoundsException
        delete[] value;
        delete[] key;
    }
    ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jKeyLen);
    ROCKSDB_NAMESPACE::Slice value_slice(reinterpret_cast<char*>(value), jValLen);

    auto falcon_cache = reinterpret_cast<FalconCache*>(falconHandle);
    if (falcon_cache != nullptr) {
        falcon_cache->put(env, rocksdbHandle, cfHandle, writeOptHandle, key_slice, value_slice);
        if (falcon_cache->bypassCache()) {
            falcon_cache->updateSizeLimit(env, rocksdbHandle, cfHandle, writeOptHandle, 0);
        }
    } else {
        FalconException::FalconExceptionJni::ThrowNew(env, "[FALCON] invalid falcon cache handle when putting state.",
            ROCKSDB_NAMESPACE::Status::InvalidArgument("Invalid Falcon Cache Handle."));
    }
}

JNIEXPORT void JNICALL Java_com_huawei_falcon_state_cache_FalconValueState_delete(JNIEnv *env, jobject obj,
    jlong falconHandle, jlong rocksdbHandle, jlong cfHandle, jlong writeOptHandle,
    jbyteArray jKey, jint jKeyOffset, jint jKeyLen)
{
    // construct key slice, key, i.e., key_slice.data() will be delete inside falcon_cache->remove()
    jbyte* key = new jbyte[jKeyLen];
    env->GetByteArrayRegion(jKey, jKeyOffset, jKeyLen, key);
    if (env->ExceptionCheck()) {  // exception thrown: ArrayIndexOutOfBoundsException
        delete[] key;
    }
    ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jKeyLen);

    auto falcon_cache = reinterpret_cast<FalconCache*>(falconHandle);
    if (falcon_cache != nullptr) {
        falcon_cache->remove(env, rocksdbHandle, cfHandle, writeOptHandle, key_slice);
        if (falcon_cache->bypassCache()) {
            falcon_cache->updateSizeLimit(env, rocksdbHandle, cfHandle, writeOptHandle, 0);
        }
    } else {
        FalconException::FalconExceptionJni::ThrowNew(env, "[FALCON] invalid falcon cache handle when removing state.",
            ROCKSDB_NAMESPACE::Status::InvalidArgument("Invalid Falcon Cache Handle."));
    }
}

JNIEXPORT jint JNICALL Java_com_huawei_falcon_state_cache_FalconValueState_getCacheSizeLimit(JNIEnv *env, jobject obj,
    jlong falconHandle)
{
    auto falcon_cache = reinterpret_cast<FalconCache*>(falconHandle);
    if (falcon_cache != nullptr) {
        return falcon_cache->getSizeLimit();
    } else {
        FalconException::FalconExceptionJni::ThrowNew(env, "[FALCON] invalid falcon handle getting cacheSizeLimit.",
            ROCKSDB_NAMESPACE::Status::InvalidArgument("Invalid Falcon Cache Handle."));
        return 0;
    }
}

JNIEXPORT void JNICALL Java_com_huawei_falcon_state_cache_FalconValueState_setCacheSizeLimit(JNIEnv *env, jobject obj,
    jlong falconHandle, jlong rocksdbHandle, jlong cfHandle, jlong writeOptHandle, jint newSizeLimit)
{
    auto falcon_cache = reinterpret_cast<FalconCache*>(falconHandle);
    if (falcon_cache != nullptr) {
        falcon_cache->updateSizeLimit(env, rocksdbHandle, cfHandle, writeOptHandle, newSizeLimit);

        auto* cf_handle = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(cfHandle);
        std::string state_name;
        if (cf_handle != nullptr) {
            state_name = cf_handle->GetName();
        }
        FALCON_LOG("state [" << state_name << "] updates cache size limit to " << newSizeLimit);
    } else {
        FalconException::FalconExceptionJni::ThrowNew(env, "[FALCON] invalid falcon handle updating cacheSizeLimit.",
            ROCKSDB_NAMESPACE::Status::InvalidArgument("Invalid Falcon Cache Handle."));
    }
}

JNIEXPORT void JNICALL Java_com_huawei_falcon_state_cache_FalconValueState_flush(JNIEnv *env, jobject obj,
    jlong falconHandle, jlong rocksdbHandle, jlong cfHandle, jlong writeOptHandle)
{
    auto falcon_cache = reinterpret_cast<FalconCache*>(falconHandle);
    if (falcon_cache != nullptr) {
        falcon_cache->flush(env, rocksdbHandle, cfHandle, writeOptHandle);
        falcon_cache->clearAll();
    } else {
        FalconException::FalconExceptionJni::ThrowNew(env, "[FALCON] invalid falcon handle when checkpoint flushing.",
            ROCKSDB_NAMESPACE::Status::InvalidArgument("Invalid Falcon Cache Handle."));
    }
}

}