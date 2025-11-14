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

#ifndef BOOST_SS_BINARY_DATA_H
#define BOOST_SS_BINARY_DATA_H

#include <cstring>
#include <memory>

#include "buffer.h"

namespace ock {
namespace bss {
struct BinaryData {
public:
    BinaryData() = default;
    inline void Init(const uint8_t *valueData, uint32_t valueLen, const BufferRef &buffer = nullptr)
    {
        mLength = valueLen;
        mData = valueData;
        mBuffer = buffer;
    }

    BinaryData(const uint8_t *data, uint32_t length, const BufferRef &buffer = nullptr)
        : mLength(length), mData(data), mBuffer(buffer)
    {
    }

    inline uint32_t Length() const
    {
        return mLength;
    }

    inline const uint8_t *Data() const
    {
        return mData;
    }

    inline BufferRef Buffer() const
    {
        return mBuffer;
    }

    inline bool IsNull()
    {
        return mData == nullptr;
    }

    inline bool operator==(const BinaryData &other) const
    {
        return mLength == other.mLength && mData && other.mData &&
               (mData == other.mData || memcmp(mData, other.mData, mLength) == 0);
    }
protected:
    uint32_t mLength{ 0 };
    const uint8_t *mData{ nullptr };
    BufferRef mBuffer{ nullptr };
};
using BinaryDataRef = std::shared_ptr<BinaryData>;
}
}

#endif