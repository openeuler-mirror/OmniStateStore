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

/**
 * 空迭代器，作为初始值
 *
 * @since BeiMing 25.0.T1
 */
public final class EmptySingleStateIterator implements SingleStateIterator {
    /**
     * 获取空迭代器
     */
    public static final EmptySingleStateIterator INSTANCE = new EmptySingleStateIterator();

    /**
     * next
     */
    @Override
    public void next() {
    }

    /**
     * isValid
     *
     * @return boolean
     */
    @Override
    public boolean isValid() {
        return false;
    }

    /**
     * 获取key，空实现不支持
     *
     * @return key
     */
    @Override
    public byte[] key() {
        throw new UnsupportedOperationException("Failed to query key, empty iterator should not be queried for key.");
    }

    /**
     * 获取value，空实现不支持
     *
     * @return value
     */
    @Override
    public byte[] value() {
        throw new UnsupportedOperationException(
            "Failed to query value, empty iterator should not be queried for value.");
    }

    /**
     * 获取kvStateId，空实现不支持
     *
     * @return kvStateId
     */
    @Override
    public int kvStateId() {
        throw new UnsupportedOperationException(
            "Failed to query kvStateId, empty iterator should not be queried for kvStateId.");
    }

    /**
     * 迭代器指向下一个keyGroup的元素
     *
     * @param keyGroup keyGroup
     */
    @Override
    public void seek(int keyGroup) {
        throw new UnsupportedOperationException(
            "Failed to seek to target keyGroup, empty iterator should not be seeked to target keyGroup.");
    }

    @Override
    public void close() {
    }
}
