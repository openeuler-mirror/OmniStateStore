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

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * 实现KeyValueStateIterator接口，由kvState迭代器和pqState迭代器共同组成，供flink侧调用
 *
 * @since BeiMing 25.0.T1
 */
public class BoostKeyValueStateIterator implements KeyValueStateIterator {
    private static final Logger LOG = LoggerFactory.getLogger(BoostKeyValueStateIterator.class);

    private final List<SingleStateIterator> keyGroupStateIterators;

    private final Iterator<Integer> keyGroupIterator;

    private int currentKeyGroup;

    private boolean newKeyGroup;

    private boolean newState;

    private boolean initialized;

    private ISingleStateIteratorWrapper currentIterator;

    public BoostKeyValueStateIterator(List<SingleStateIterator> keyGroupStateIterators, KeyGroupRange keyGroupRange) {
        this.keyGroupStateIterators = keyGroupStateIterators;
        this.keyGroupIterator = keyGroupRange.iterator();
        this.currentIterator = EmptySingleStateIteratorWrapper.INSTANCE;
        this.initialized = false;
        LOG.info("Succeed to build BoostKeyValueStateIterator.");
    }

    @Override
    public void next() throws IOException {
        this.newKeyGroup = false;
        boolean isPreviousValid = this.currentIterator.isValid();
        int previousId = isPreviousValid ? this.currentIterator.kvStateId() : Integer.MIN_VALUE;

        this.currentIterator.next();
        // 根据keyGroup循环迭代list中迭代器
        while (!this.currentIterator.isValid() && this.keyGroupIterator.hasNext()) {
            this.currentKeyGroup = this.keyGroupIterator.next();
            this.newKeyGroup = true;
            // currentIterator每次next迭代的是当前keyGroup下所有迭代器的值
            this.currentIterator = SingleStateIteratorWrapper
                .create(this.keyGroupStateIterators, this.currentKeyGroup);
        }

        // 标识X
        if (!this.currentIterator.isValid()) {
            this.currentIterator = EmptySingleStateIteratorWrapper.INSTANCE;
            return;
        }

        if (this.newKeyGroup) {
            this.newState = true;
        } else {
            // 如果上一个迭代器不合法，则证明这是第一次调用next或上一次调用next进入了标识X的分支
            this.newState = (!isPreviousValid || previousId != this.currentIterator.kvStateId());
        }
    }

    @Override
    public int keyGroup() {
        return this.currentKeyGroup;
    }

    @Override
    public byte[] key() {
        return this.currentIterator.key();
    }

    @Override
    public byte[] value() {
        return this.currentIterator.value();
    }

    @Override
    public int kvStateId() {
        return this.currentIterator.kvStateId();
    }

    @Override
    public boolean isNewKeyValueState() {
        return this.newState;
    }

    @Override
    public boolean isNewKeyGroup() {
        return this.newKeyGroup;
    }

    @Override
    public boolean isValid() {
        if (!this.initialized) {
            this.initialized = true;
            try {
                next();
            } catch (IOException e) {
                throw new BSSRuntimeException("Failed to judge isValid(), error during call next() ", e);
            }
        }
        return this.currentIterator.isValid();
    }

    @Override
    public void close() {
        LOG.info("Closing savepoint iterator: BoostKeyValueStateIterator. Current iterator size: {}",
            this.keyGroupStateIterators.size());
        this.keyGroupStateIterators.forEach(IOUtils::closeAllQuietly);
    }
}
