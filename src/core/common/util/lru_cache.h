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

#ifndef BOOST_SS_LRU_CACHE_H
#define BOOST_SS_LRU_CACHE_H

#include <memory>
#include <unordered_map>

#include "common/bss_log.h"
#include "util/bss_lock.h"
#include "common/block.h"

namespace ock {
namespace bss {
static const double TRIM_RATIO = 0.9;

class DLinkedNode;
using DLinkedNodeRef = std::shared_ptr<DLinkedNode>;
class DLinkedNode {
public:
    explicit DLinkedNode() : key(0), value(nullptr), prev(nullptr), next(nullptr)
    {
    }

    DLinkedNode(uint64_t key, BlockRef value) : key(key), value(value), prev(nullptr), next(nullptr)
    {
    }

public:
    uint64_t key;
    BlockRef value;
    DLinkedNodeRef prev;
    DLinkedNodeRef next;
};

class LRUCache {
public:
    explicit LRUCache(uint64_t capacity) : mCapacity(capacity), mCurrentSize(0)
    {
        mHead = std::make_shared<DLinkedNode>();
        mTail = std::make_shared<DLinkedNode>();
        mHead->next = mTail;
        mTail->prev = mHead;
        LOG_INFO("Create LRUCache capacity:" << capacity << " success.");
    }

    ~LRUCache()
    {
        Close();
        std::unordered_map<uint64_t, DLinkedNodeRef>().swap(mCache);
        std::unordered_map<ConfigRef, BoostNativeMetricPtr>().swap(mLruCachesMetricMap);
        LOG_INFO("Delete LRUCache success.");
    }

    BlockRef Get(const uint64_t &key)
    {
        mLock.Lock();
        auto iter = mCache.find(key);
        if (iter == mCache.end()) {
            mLock.UnLock();
            return nullptr;
        }
        DLinkedNodeRef node = iter->second;
        MoveToHead(node); // 将节点移动到链表头部
        mLock.UnLock();
        return node->value;
    }

    void Put(const uint64_t key, const BlockRef &value)
    {
        if (UNLIKELY(value == nullptr) || UNLIKELY(UINT64_MAX - value->BufferLen() < mCurrentSize)) {
            return;
        }
        mLock.Lock();
        auto iter = mCache.find(key);
        if (iter != mCache.end() && iter->second != nullptr) {
            DLinkedNodeRef node = iter->second;
            auto oldValueLen = node->value->BufferLen();
            UpdateCacheStat(node->value->GetBlockType(), false, oldValueLen);
            node->value = value;
            UpdateCacheStat(node->value->GetBlockType(), true, value->BufferLen());
            MoveToHead(node); // 将节点移动到链表头部
            mCurrentSize = mCurrentSize - oldValueLen + value->BufferLen(); // 更新当前资源
        } else {
            DLinkedNodeRef node = std::make_shared<DLinkedNode>(key, value);
            mCache[key] = node;
            AddToHead(node); // 将节点插入到链表头部
            mCurrentSize += value->BufferLen(); // 更新当前资源
            UpdateCacheStat(value->GetBlockType(), true, value->BufferLen());
        }
        TrimToSize();
        mLock.UnLock();
    }

    BlockRef Remove(uint64_t key)
    {
        mLock.Lock();
        auto iter = mCache.find(key);
        if (iter != mCache.end()) {
            DLinkedNodeRef node = iter->second;
            UpdateCacheStat(node->value->GetBlockType(), false, node->value->BufferLen());
            mCache.erase(key);
            RemoveNode(node);
            mCurrentSize -= node->value->BufferLen(); // 更新当前资源
            mLock.UnLock();
            return node->value;
        }
        mLock.UnLock();
        return nullptr;
    }

    void Close()
    {
        mLock.Lock();
        while (!mCache.empty()) {
            RemoveTail();
        }
        mHead->next = nullptr;
        mTail->prev = nullptr;
        mCurrentSize = 0;  // 更新当前资源
        mLock.UnLock();
    }

    void RemoveEldestEntry(uint64_t size)
    {
        mLock.Lock();
        uint64_t removeSize = 0;
        do {
            if (mCurrentSize > 0 && !mCache.empty()) {
                DLinkedNodeRef node = RemoveTail();
                if (UNLIKELY(node == nullptr)) {
                    break;
                }
                mCache.erase(node->key);
                mCurrentSize -= node->value->BufferLen();  // 更新当前资源
                removeSize += node->value->BufferLen();
            } else {
                break;
            }
        } while (removeSize < size);
        if (UNLIKELY(removeSize < size)) {
            LOG_DEBUG("Remove eldest entry not enough, removeSize:" << removeSize << ", expectSize:" << size
                                                                   << ", currentSize:" << mCurrentSize);
        }
        mLock.UnLock();
    }

    inline void RegisterMetric(ConfigRef config, BoostNativeMetricPtr metricPtr)
    {
        if (UNLIKELY(metricPtr == nullptr)) {
            LOG_ERROR("RegisterMetric for lru cache failed, metricPtr is nullptr.");
            return;
        }
        mMetricEnabled = metricPtr->IsFileCacheMetricEnabled();
        if (!mMetricEnabled) {
            return;
        }
        mLock.Lock();
        mLruCachesMetricMap.emplace(config, metricPtr);
        mLock.UnLock();
    }

    inline void DeRegisterMetric(ConfigRef config)
    {
        if (!mMetricEnabled) {
            return;
        }
        mLock.Lock();
        mLruCachesMetricMap.erase(config);
        mLock.UnLock();
    }

    inline void UpdateCacheStat(BlockType type, bool isAdd, uint64_t size)
    {
        if (!mMetricEnabled) {
            return;
        }
        for (auto it = mLruCachesMetricMap.begin(); it != mLruCachesMetricMap.end(); it++) {
            CONTINUE_LOOP_AS_NULLPTR(it->second);
            it->second->UpdateCacheStat(type, isAdd, size);
        } 
    }

private:
    void AddToHead(DLinkedNodeRef &node)
    {
        node->prev = mHead;
        node->next = mHead->next;
        mHead->next->prev = node;
        mHead->next = node;
    }

    void RemoveNode(DLinkedNodeRef &node)
    {
        if (node == mTail || node == mHead || node == nullptr) {
            return;
        }
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    void MoveToHead(DLinkedNodeRef &node)
    {
        RemoveNode(node);
        AddToHead(node);
    }

    DLinkedNodeRef RemoveTail()
    {
        DLinkedNodeRef node = mTail->prev;
        if (node == nullptr || node == mHead) {
            return nullptr;
        }
        mCache.erase(node->key);
        RemoveNode(node);
        return node;
    }

    void TrimToSize()
    {
        while ((mCurrentSize > (mCapacity * TRIM_RATIO)) && !mCache.empty()) {
            DLinkedNodeRef tail = RemoveTail();
            if (tail == nullptr) {
                return;
            }
            mCurrentSize -= tail->value->BufferLen();  // 更新当前资源
            LOG_DEBUG("Trim eldest entry, key:" << tail->key << "valueSize:" << tail->value->BufferLen()
                                                << ", currentSize:" << mCurrentSize);
        }
    }

private:
    std::unordered_map<uint64_t, DLinkedNodeRef> mCache;
    DLinkedNodeRef mHead;
    DLinkedNodeRef mTail;
    uint64_t mCapacity;
    uint64_t mCurrentSize;
    SpinLock mLock;
    bool mMetricEnabled = false;
    std::unordered_map<ConfigRef, BoostNativeMetricPtr> mLruCachesMetricMap; // config作为db的标识
};

}  // namespace bss
}  // namespace ock
#endif  // BOOST_SS_LRU_CACHE_H
