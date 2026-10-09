/* Copyright (c) Huawei Technologies Co., Ltd. 2025. Licensed under the Mulan PSL v2. */
package com.huawei.ock.bss.table.api;

/** Restore-only entry point for bytes produced by the table's previous serializers. */
public interface SerializedMapRestoreTable<K, UK> {
    void restoreSerialized(byte[] keyBytes, int mixedKeyHash, int bloomKeyHash, byte[] userKeyBytes,
        int userKeyHash, byte[] valueBytes);
}
