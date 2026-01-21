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

import java.io.Closeable;
import java.io.IOException;

/**
 * kvState迭代器和pqState迭代器接口
 *
 * @since BeiMing 25.0.T1
 */
public interface SingleStateIterator extends Closeable {
    /**
     * next
     *
     * @throws IOException IOException
     */
    void next() throws IOException;

    /**
     * isValid
     *
     * @return boolean
     */
    boolean isValid();

    /**
     * key
     *
     * @return byte[]
     */
    byte[] key();

    /**
     * value
     *
     * @return byte[]
     */
    byte[] value();

    /**
     * kvStateId
     *
     * @return int
     */
    int kvStateId();

    /**
     * close
     */
    @Override
    void close();

    int keyGroup();
}
