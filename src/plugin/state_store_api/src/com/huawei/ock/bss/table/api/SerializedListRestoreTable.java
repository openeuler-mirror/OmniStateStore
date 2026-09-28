/* Copyright (c) Huawei Technologies Co., Ltd. 2025. Licensed under the Mulan PSL v2. */
package com.huawei.ock.bss.table.api;

import java.io.IOException;

/** Restore-only entry point for bounded chunks in the native list format. */
public interface SerializedListRestoreTable<K> {
    void restoreSerialized(byte[] keyBytes, int mixedKeyHash, int bloomKeyHash, ChunkProducer producer)
        throws IOException;

    interface ChunkProducer {
        void writeTo(ChunkConsumer consumer) throws IOException;
    }

    interface ChunkConsumer {
        void accept(byte[] chunk) throws IOException;
    }
}
