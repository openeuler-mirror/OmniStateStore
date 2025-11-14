/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.huawei.ock.bss.iterator;

import java.io.IOException;

/**
 * 空迭代器，作为初始值
 *
 * @since BeiMing 25.0.T1
 */
public class EmptySingleStateIteratorWrapper implements ISingleStateIteratorWrapper {
    /**
     * 获取空迭代器
     */
    public static final EmptySingleStateIteratorWrapper INSTANCE = new EmptySingleStateIteratorWrapper();

    @Override
    public void next() throws IOException {
    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public byte[] key() {
        throw new UnsupportedOperationException("Failed to query key, empty iterator should not be query for key.");
    }

    @Override
    public byte[] value() {
        throw new UnsupportedOperationException(
            "Failed to query value, empty iterator should not be queried for value.");
    }

    @Override
    public int kvStateId() {
        throw new UnsupportedOperationException(
            "Failed to query kvStateId, empty iterator should not be queried for kvStateId.");
    }
}
