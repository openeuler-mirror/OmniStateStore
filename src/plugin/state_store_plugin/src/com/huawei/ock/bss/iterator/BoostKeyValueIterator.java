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

import com.huawei.ock.bss.iterator.struct.KVDataSorter;
import com.huawei.ock.bss.iterator.struct.SingleKeyGroupKVState;

import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * kvState迭代器
 *
 * @since BeiMing 25.0.T1
 */
public class BoostKeyValueIterator implements SingleStateIterator {
    private static final Logger LOG = LoggerFactory.getLogger(BoostKeyValueIterator.class);

    private final KVDataSorter kvDataSorter;

    private CloseableIterator<SingleKeyGroupKVState.KeyValueItem> currentKeyGroupIterator;

    private SingleKeyGroupKVState.KeyValueItem currentItem;

    public BoostKeyValueIterator(KVDataSorter kvDataSorter) {
        this.kvDataSorter = kvDataSorter;
        this.currentKeyGroupIterator = null;
        this.currentItem = null;
        LOG.info("Succeed to build BoostKeyValueIterator.");
    }

    /**
     * 指向下一个keyGroup内的元素
     *
     * @param keyGroup 要指向的keyGroup
     * @throws Exception exception
     */
    public void seek(int keyGroup) throws Exception {
        if (this.currentKeyGroupIterator != null) {
            this.currentKeyGroupIterator.close();
            this.currentKeyGroupIterator = null;
        }
        this.currentKeyGroupIterator = this.kvDataSorter.iterator(keyGroup);
        next();
    }

    /**
     * currentItem指向下一个迭代器
     *
     * @throws IOException IOException
     */
    public void next() throws IOException {
        this.currentItem = null;
        if (this.currentKeyGroupIterator.hasNext()) {
            this.currentItem = this.currentKeyGroupIterator.next();
        }
    }

    /**
     * 判断当前currentItem指向的迭代器是否为空
     *
     * @return 不为空为true
     */
    public boolean isValid() {
        return (this.currentItem != null);
    }

    /**
     * 从当前currentItem指向的迭代器获取key
     *
     * @return key的二进制
     */
    public byte[] key() {
        return this.currentItem.getKey();
    }

    /**
     * 从当前currentItem指向的迭代器获取value
     *
     * @return value的二进制
     */
    public byte[] value() {
        return this.currentItem.getValue();
    }

    /**
     * 从当前currentItem指向的迭代器获取kvStateId
     *
     * @return kvStateId
     */
    public int kvStateId() {
        return this.currentItem.getStateId();
    }

    /**
     * 关闭资源
     */
    public void close() {
        LOG.info("Closing savepoint iterator: BoostKeyValueStateIterator.");
        if (this.currentKeyGroupIterator != null) {
            IOUtils.closeQuietly(this.currentKeyGroupIterator);
            this.currentKeyGroupIterator = null;
        }
        IOUtils.closeQuietly(this.kvDataSorter);
    }
}
