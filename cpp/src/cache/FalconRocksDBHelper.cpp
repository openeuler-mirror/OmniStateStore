/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#include "FalconRocksDBHelper.h"

jbyteArray rocksdb_get(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, ROCKSDB_NAMESPACE::Slice key_slice)
{
    auto cf_handle = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(cfHandle);
    auto db_handle = reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(rocksdbHandle);

    if (cf_handle != nullptr && db_handle != nullptr) {
        std::string value;
        ROCKSDB_NAMESPACE::Status s = db_handle->Get(ROCKSDB_NAMESPACE::ReadOptions(), cf_handle, key_slice, &value);

        if (s.IsNotFound()) { return nullptr; }
        if (s.ok()) {
            jbyteArray jVal = FalconUtil::JniUtil::copyBytes(env, value);
            if (jVal == nullptr) { return nullptr; }  // exception occurred
            return jVal;
        }
        FalconException::FalconExceptionJni::ThrowNew(env, "[FALCON] error occurs when get from rocksdb.", s);
        return nullptr;
    } else {
        FalconException::FalconExceptionJni::ThrowNew(env, "[FALCON] invalid handle when get from rocksdb.",
            ROCKSDB_NAMESPACE::Status::InvalidArgument("Invalid ColumnFamilyHandle or DBHandle."));
        return nullptr;
    }
}

void rocksdb_put(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
                 ROCKSDB_NAMESPACE::Slice key_slice, ROCKSDB_NAMESPACE::Slice value_slice)
{
    auto* cf_handle = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(cfHandle);
    auto* db_handle = reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(rocksdbHandle);
    auto* write_options = reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(writeOptionsHandle);

    if (cf_handle != nullptr && db_handle != nullptr && write_options != nullptr) {
        ROCKSDB_NAMESPACE::Status s = db_handle->Put(*write_options, cf_handle, key_slice, value_slice);

        if (s.ok()) {
            return;
        }
        FalconException::FalconExceptionJni::ThrowNew(env, "[FALCON] error occurs when put into rocksdb.", s);
    } else {
        FalconException::FalconExceptionJni::ThrowNew(env, "[FALCON] invalid handle when put into rocksdb.",
            ROCKSDB_NAMESPACE::Status::InvalidArgument("Invalid ColumnFamilyHandle or DBHandle or WriteOptHandle."));
    }
}

void rocksdb_delete(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
                    ROCKSDB_NAMESPACE::Slice key_slice)
{
    auto* cf_handle = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(cfHandle);
    auto* db_handle = reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(rocksdbHandle);
    auto* write_options = reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(writeOptionsHandle);

    if (cf_handle != nullptr && db_handle != nullptr && write_options != nullptr) {
        ROCKSDB_NAMESPACE::Status s = db_handle->Delete(*write_options, cf_handle, key_slice);

        if (s.ok()) {
            return;
        }
        FalconException::FalconExceptionJni::ThrowNew(env, "[FALCON] error occurs when delete from rocksdb.", s);
    } else {
        FalconException::FalconExceptionJni::ThrowNew(env, "[FALCON] invalid handle when delete rocksdb.",
            ROCKSDB_NAMESPACE::Status::InvalidArgument("Invalid ColumnFamilyHandle or DBHandle or WriteOptHandle."));
    }
}