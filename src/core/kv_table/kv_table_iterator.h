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

#ifndef BOOST_SS_KV_TABLE_ITERATOR_H
#define BOOST_SS_KV_TABLE_ITERATOR_H

#include <map>
#include <set>
#include <utility>

#include "include/auto_closeable.h"
#include "common/util/iterator.h"
#include "serialized_data.h"
#include "slice_table/slice_table.h"

namespace ock {
namespace bss {
enum class EntryIteratorType { KVALUE_ITERATOR, KMAP_ITERATOR, KLIST_ITERATOR, KSUBLIST_ITERATOR, KSUBMAP_ITERATOR };
class MapIterator : public Iterator<KeyValueRef> {
public:
    MapIterator(const KeyValueIteratorRef &freshTableIterator, const KeyValueIteratorRef &sliceTableIterator,
                EntryIteratorType iteratorType)
        : mSliceTableIterator(std::move(sliceTableIterator)), mIteratorType(iteratorType)
    {
        if (freshTableIterator != nullptr) {
            while (freshTableIterator->HasNext()) {
                auto keyValue = freshTableIterator->Next();
                if (keyValue != nullptr) {
                    mExistKeyValue.emplace(&keyValue->key, keyValue);
                }
            }
            mFreshTableIterator = mExistKeyValue.begin();
        } else {
            mFreshTableIterator = mExistKeyValue.end();
        }

        mCurrentKeyValue = Advance();
    }

    bool HasNext() override
    {
        return mCurrentKeyValue != nullptr;
    }

    KeyValueRef Next() override
    {
        mPreKeyValue = mCurrentKeyValue;
        mCurrentKeyValue = Advance();
        return mPreKeyValue;
    }

private:
    inline KeyValueRef Advance()
    {
        while (mFreshTableIterator != mExistKeyValue.end()) {
            auto keyValue = mFreshTableIterator->second;
            CONTINUE_LOOP_AS_NULLPTR(keyValue);
            mFreshTableIterator++;
            if (keyValue->value.ValueType() != DELETE) {
                return keyValue;
            }
        }
        while (mSliceTableIterator != nullptr && mSliceTableIterator->HasNext()) {
            auto keyValue = mSliceTableIterator->Next();
            if (keyValue != nullptr && keyValue->value.ValueType() != DELETE &&
                mExistKeyValue.find(&keyValue->key) == mExistKeyValue.end()) {
                return keyValue;
            }
        }
        return nullptr;
    }

private:
    KeyValueRef mPreKeyValue;
    KeyValueRef mCurrentKeyValue;
    KeyValueMap mExistKeyValue;
    KeyValueMap::iterator mFreshTableIterator;
    KeyValueIteratorRef mSliceTableIterator;
    EntryIteratorType mIteratorType;
};

class KeyIterator : public Iterator<KeyValueRef> {
public:
    KeyIterator(const KeyValueIteratorRef &freshTableIterator, const KeyValueIteratorRef &sliceTableIterator,
                EntryIteratorType iteratorType)
        : mSliceTableIterator(std::move(sliceTableIterator)), mIteratorType(iteratorType)
    {
        if (freshTableIterator != nullptr) {
            while (freshTableIterator->HasNext()) {
                auto keyValue = freshTableIterator->Next();
                if (keyValue != nullptr) {
                    mExistKeyValue.emplace(&keyValue->key, keyValue);
                }
            }
            mFreshTableIterator = mExistKeyValue.begin();
        } else {
            mFreshTableIterator = mExistKeyValue.end();
        }

        mCurrentKeyValue = Advance();
    }

    bool HasNext() override
    {
        return mCurrentKeyValue != nullptr;
    }

    KeyValueRef Next() override
    {
        mPreKeyValue = mCurrentKeyValue;
        mCurrentKeyValue = Advance();
        return mPreKeyValue;
    }

    inline EntryIteratorType GetIteratorType()
    {
        return mIteratorType;
    }

private:
    inline KeyValueRef Advance()
    {
        while (mFreshTableIterator != mExistKeyValue.end()) {
            auto keyValue = mFreshTableIterator->second;
            CONTINUE_LOOP_AS_NULLPTR(keyValue);
            mFreshTableIterator++;
            if (keyValue->value.ValueType() != DELETE && !IsVisited(keyValue)) {
                return keyValue;
            }
        }
        while (mSliceTableIterator != nullptr && mSliceTableIterator->HasNext()) {
            auto keyValue = mSliceTableIterator->Next();
            if (keyValue != nullptr && keyValue->value.ValueType() != DELETE &&
                mExistKeyValue.find(&keyValue->key) == mExistKeyValue.end() && !IsVisited(keyValue)) {
                return keyValue;
            }
        }
        return nullptr;
    }

    inline bool IsVisited(const KeyValueRef &keyValue)
    {
        RETURN_FALSE_AS_NULLPTR(keyValue);
        if (mIteratorType != EntryIteratorType::KMAP_ITERATOR) {
            return false;
        }
        bool visited = true;
        PriKeyNode *priKeyPtr = const_cast<PriKeyNode *>(&keyValue->key.PriKey());
        if (mVisited.find(priKeyPtr) == mVisited.end()) {
            visited = false;
            mVisited.emplace(priKeyPtr, keyValue);
        }
        return visited;
    }

private:
    KeyValueRef mCurrentKeyValue;
    KeyValueRef mPreKeyValue;
    KeyValueMap mExistKeyValue;
    KeyValueMap::iterator mFreshTableIterator;
    KeyValueIteratorRef mSliceTableIterator;
    EntryIteratorType mIteratorType;
    PriKeyValueMap mVisited;
};
}  // namespace bss
}  // namespace ock
#endif  // BOOST_SS_KV_TABLE_ITERATOR_H
