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

package com.huawei.ock.bss.iterator.struct;

import org.apache.flink.runtime.state.KeyGroupRange;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Meta
 *
 * @since BeiMing 25.0.T1
 */
public class Meta {
    private final KVStatesGroup kvStatesGroup;

    private final Set<Integer> validKeyGroups;

    public Meta(KVStatesGroup kvStatesGroup, Set<Integer> validKeyGroups) {
        this.kvStatesGroup = kvStatesGroup;
        this.validKeyGroups = validKeyGroups;
    }

    /**
     * getKVStatesGroup
     *
     * @return KVStatesGroup
     */
    public KVStatesGroup getKVStatesGroup() {
        return this.kvStatesGroup;
    }

    /**
     * getValidKeyGroups
     *
     * @return Set<Integer>
     */
    public Set<Integer> getValidKeyGroups() {
        return this.validKeyGroups;
    }

    /**
     * MetaBuilder
     */
    public static class MetaBuilder {
        private final KeyGroupRange keyGroupRange;

        private final KVStatesGroup.Builder builder;

        private final Set<Integer> validKeyGroups;

        public MetaBuilder(KeyGroupRange keyGroupRange, KVStatesGroup.Builder builder) {
            this.keyGroupRange = keyGroupRange;
            this.builder = builder;
            this.validKeyGroups = new HashSet<>();
        }

        /**
         * getKeyGroupRange
         *
         * @return KeyGroupRange
         */
        public KeyGroupRange getKeyGroupRange() {
            return this.keyGroupRange;
        }

        /**
         * add
         *
         * @param item SingleKeyGroupKVState.KeyValueItem
         * @throws IOException IOException
         */
        public void add(SingleKeyGroupKVState.KeyValueItem item) throws IOException {
            this.builder.add(item);
            this.validKeyGroups.add(item.getKeyGroup());
        }

        /**
         * build
         *
         * @return Meta
         * @throws IOException IOException
         */
        public Meta build() throws IOException {
            KVStatesGroup kvstatesgroup = this.builder.finish();
            return new Meta(kvstatesgroup, this.validKeyGroups);
        }
    }
}

