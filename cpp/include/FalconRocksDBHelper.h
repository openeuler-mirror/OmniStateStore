/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#ifndef FALCON_FALCONROCKSDBHELPER_H
#define FALCON_FALCONROCKSDBHELPER_H

#include "FalconExceptionJni.h"
#include "FalconUtilJni.h"
#include <rocksdb/db.h>

/**
 * RocksDB native get function, get value from rocksdb using the given key. Note that memory of key will not be delete
 * inside this function, for that memory of key will be managed in falcon cache get API.
 * @param env pointer of com.huawei.falcon.state.FalconValueState
 * @param rocksdbHandle JAVA rocksdb object handle
 * @param cfHandle JAVA column family handle
 * @param key_slice slice of key used to access rocksdb, which holds view of key array and key length
 * @return value corresponding to the given key
 */
jbyteArray rocksdb_get(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, ROCKSDB_NAMESPACE::Slice key_slice);

/**
 * RocksDB native put function, put the given key-value pair into rocksdb. Note that memory of key/value will not be
 * delete inside this function, for that memory of key will be managed in falcon cache put API.
 * @param env pointer of com.huawei.falcon.state.FalconValueState
 * @param rocksdbHandle JAVA rocksdb object handle
 * @param cfHandle JAVA column family handle
 * @param writeOptionsHandle JAVA rocksdb write options handle
 * @param key_slice slice of key used to access rocksdb, which holds view of key array and key length
 * @param value_slice slice of value used to access rocksdb, which holds view of value array and value length
 */
void rocksdb_put(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
                 ROCKSDB_NAMESPACE::Slice key_slice, ROCKSDB_NAMESPACE::Slice value_slice);

/**
 * RocksDB native delete function, delete the value corresponding to the given key from rocksdb. Note that memory of key
 * will not be delete inside this function, for that memory of key will be managed in falcon cache remove API.
 * @param env pointer of com.huawei.falcon.state.FalconValueState
 * @param rocksdbHandle JAVA rocksdb object handle
 * @param cfHandle JAVA column family handle
 * @param writeOptionsHandle JAVA rocksdb write options handle
 * @param key_slice slice of key used to access rocksdb, which holds view of key array and key length
 */
void rocksdb_delete(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
                    ROCKSDB_NAMESPACE::Slice key_slice);

#endif // FALCON_FALCONROCKSDBHELPER_H
