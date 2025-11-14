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

import com.huawei.ock.bss.common.exception.BSSRuntimeException;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * 迭代所有SingleStateIterator中指定KeyGroup位置的元素
 *
 * @since BeiMing 25.0.T1
 */
public class SingleStateIteratorWrapper implements ISingleStateIteratorWrapper {
    private final Iterator<SingleStateIterator> iteratorForSingleStateIterator;

    private SingleStateIterator currentSingleStateIterator;

    private SingleStateIteratorWrapper(Iterator<SingleStateIterator> iteratorForSingleStateIterator,
        SingleStateIterator currentSingleStateIterator) throws IOException {
        this.iteratorForSingleStateIterator = iteratorForSingleStateIterator;
        this.currentSingleStateIterator = currentSingleStateIterator;
        next();
    }

    /**
     * create，创建本类的实例
     *
     * @param keyGroupStateIterators keyGroupStateIterators
     * @param keyGroup               keyGroup
     * @return SingleStateIteratorWrapper
     * @throws IOException IOException
     */
    public static SingleStateIteratorWrapper create(List<SingleStateIterator> keyGroupStateIterators, int keyGroup)
        throws IOException {
        for (SingleStateIterator iterator : keyGroupStateIterators) {
            try {
                iterator.seek(keyGroup);
            } catch (Exception e) {
                throw new BSSRuntimeException("Failed to create SingleStateIteratorWrapper,"
                    + " error while trying to seek all iterator to target keyGroup.");
            }
        }
        return new SingleStateIteratorWrapper(keyGroupStateIterators.iterator(), EmptySingleStateIterator.INSTANCE);
    }

    /**
     * next
     *
     * @throws IOException IOException
     */
    public void next() throws IOException {
        // 先迭代当前SingleStateIterator（以BoostKeyValueIterator为例）中的key和value等信息，因为有多对kv数据的keyGroup可以相同
        this.currentSingleStateIterator.next();
        while (!this.currentSingleStateIterator.isValid() && this.iteratorForSingleStateIterator.hasNext()) {
            this.currentSingleStateIterator = this.iteratorForSingleStateIterator.next();
        }
        if (!this.currentSingleStateIterator.isValid()) {
            this.currentSingleStateIterator = EmptySingleStateIterator.INSTANCE;
        }
    }

    /**
     * isValid
     *
     * @return boolean
     */
    public boolean isValid() {
        return this.currentSingleStateIterator.isValid();
    }

    /**
     * key
     *
     * @return byte[]
     */
    public byte[] key() {
        return this.currentSingleStateIterator.key();
    }

    /**
     * value
     *
     * @return byte[]
     */
    public byte[] value() {
        return this.currentSingleStateIterator.value();
    }

    /**
     * kvStateId
     *
     * @return int
     */
    public int kvStateId() {
        return this.currentSingleStateIterator.kvStateId();
    }
}
