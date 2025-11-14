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

#ifndef BOOST_SS_BINARY_KEY_VALUE_ITEM_ITERATOR_H
#define BOOST_SS_BINARY_KEY_VALUE_ITEM_ITERATOR_H

#include "binary/key_value.h"
#include "common/util/iterator.h"
#include "kv_table/stateId_provider.h"

namespace ock {
namespace bss {
class BinaryKeyValueItem {
public:
    ~BinaryKeyValueItem()
    {
        std::vector<uint8_t>().swap(mValueVector);
    }

    inline void SetStateName(const std::string &stateName)
    {
        mStateName = stateName;
    }

    inline void SetStateType(StateType stateType)
    {
        mStateType = stateType;
    }

    inline void SetKeyGroup(uint32_t keyGroup)
    {
        mKeyGroup = keyGroup;
    }

    inline void SetKey(uint8_t *key, uint32_t keyLength)
    {
        mKey = key;
        mKeyLength = keyLength;
    }

    inline void SetNs(uint8_t *ns, uint32_t nsLength)
    {
        mNs = ns;
        mNsLength = nsLength;
    }

    inline void SetMapKey(uint8_t *mapKey, uint32_t mapKeyLength)
    {
        mMapKey = mapKey;
        mMapKeyLength = mapKeyLength;
    }

    inline void SetValue(uint8_t *value, uint32_t valueLength)
    {
        mValue = value;
        mValueLength = valueLength;
    }

    inline void SetKeyValue(const KeyValueRef &keyValue)
    {
        mKeyValue = keyValue;
    }

public:
    KeyValueRef mKeyValue = nullptr; // 持有相关引用，避免buffer被提前释放
    StateType mStateType;
    std::string mStateName;
    uint32_t mKeyGroup = 0;
    uint8_t *mKey;
    uint32_t mKeyLength = 0;
    uint8_t *mNs;
    uint32_t mNsLength = 0;
    uint8_t *mMapKey;
    uint32_t mMapKeyLength = 0;
    uint8_t *mValue;
    uint32_t mValueLength = 0;
    std::vector<uint8_t> mValueVector;
};
using BinaryKeyValueItemRef = std::shared_ptr<BinaryKeyValueItem>;

class BinaryKeyValueItemIterator : public Iterator<BinaryKeyValueItemRef> {
public:
    BinaryKeyValueItemIterator(const StateIdProviderRef &stateIdProvider, uint32_t maxParallelism,
                               const KeyValueIteratorRef &binaryIterator, const MemManagerRef &memManager)
        : mStateIdProvider(stateIdProvider), mMaxParallelism(maxParallelism), mBinaryIterator(binaryIterator),
          mMemManager(memManager)
    {
    }

    bool HasNext() override;

    BinaryKeyValueItemRef Next() override;

public:
    StateIdProviderRef mStateIdProvider = nullptr;
    uint32_t mMaxParallelism = 0;
    KeyValueIteratorRef mBinaryIterator = nullptr;
    BinaryKeyValueItemRef mCurrentItem = nullptr;
    MemManagerRef mMemManager = nullptr;

private:
    void Advance();

    BinaryKeyValueItemRef Convert(const KeyValueRef &pair, BinaryKeyValueItemRef &reuseItem);

    void BuildKey(const Key &key, const std::string &stateName, const StateType &stateType,
                  BinaryKeyValueItemRef &reuseItem);

    void BuildSecondaryKey(const Key &key, const std::string &stateName,
                           const StateType &stateType, BinaryKeyValueItemRef &reuseItem) const;
};
using BinaryKeyValueItemIteratorRef = Ref<BinaryKeyValueItemIterator>;

}  // namespace bss
}  // namespace ock
#endif  // BOOST_SS_BINARY_KEY_VALUE_ITEM_ITERATOR_H