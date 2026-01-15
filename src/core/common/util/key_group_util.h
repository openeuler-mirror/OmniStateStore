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

#ifndef BOOST_SS_KEY_GROUP_UTIL_H
#define BOOST_SS_KEY_GROUP_UTIL_H

#include <cstdint>

namespace ock {
namespace bss {

class KeyGroupUtil {
public:
    static inline uint32_t ComputeKeyGroupForKeyHash(uint32_t keyHash, uint32_t maxParallelism)
    {
        if (LIKELY(maxParallelism < 129)) {
            return (keyHash & 0xFF000000) >> 24;
        }
        return (keyHash & 0xFFFF0000) >> 16;
    }

    static inline void SetKeyGroup(uint32_t &keyHashCode, uint32_t keyGroup, uint32_t maxParallelism)
    {
        if (LIKELY(maxParallelism < 129)) {
            keyHashCode = (keyHashCode & 0x00FFFFFF) | ((keyGroup & 0xFF) << 24);
        } else {
            keyHashCode = (keyHashCode & 0x0000FFFF) | ((keyGroup & 0xFFFF) << 16);
        }
    }
};
}  // namespace bss
}  // namespace ock

#endif  // BOOST_SS_KEY_GROUP_UTIL_H