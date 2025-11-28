/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef BOOST_SS_PQ_TABLE_H
#define BOOST_SS_PQ_TABLE_H

#include "concurrent_deque.h"
#include "file/file_store_impl.h"
#include "fresh_table/memory/skiplist.h"
#include "include/binary_data.h"
#include "kv_table_iterator.h"
#include "memory/memory_segment.h"
#include "transform/pq_iterator.h"

namespace ock {
namespace bss {
class SkiplistProcessor;
using SkiplistProcessorRef = std::shared_ptr<SkiplistProcessor>;

using SkipListCompletedNotify =
    std::function<void(const PQSkipList &item)>;
class PQTable : public AutoCloseable {
public:
    PQTable(const MemManagerRef &memManager, const ExecutorServicePtr &service, const LsmStoreRef &lsmStore,
        const std::string &stateName, StateIdProviderRef provider, TableDescriptionRef &des) : mMemManager(memManager),
        mService(service), mLsmStore(lsmStore), mStateName(stateName), mStateIdProvider(provider), mDescription(des) {}

    BResult Initialize();

    BResult AddKey(const BinaryData &key, uint32_t hashcode);

    BResult RemoveKey(const BinaryData &key, uint32_t hashcode);

    PQKeyIterator* KeyIterator(const BinaryData &data);

    inline std::string &GetStateName()
    {
        return mStateName;
    }

    inline void SetStateIdProvider(const StateIdProviderRef &stateIdProvider)
    {
        mStateIdProvider = stateIdProvider;
    }

    BinaryData GetCopyData(const BinaryData &binaryData)
    {
        uint8_t *addr = nullptr;
        if (UNLIKELY(mSkipList->GetMemAddr(binaryData.Length(), addr) != BSS_OK)) {
            return BinaryData();
        }
        int ret = memcpy_s(addr, binaryData.Length(), binaryData.Data(), binaryData.Length());
        if (ret != EOK) {
            return BinaryData();
        }
        BinaryData data = BinaryData(addr, binaryData.Length(), mSkipList->GetAllocator());
        return data;
    }

    PQSkipList InitNewSkipList()
    {
        uintptr_t dataAddress = 0;
        uint32_t capacity = IO_SIZE_8M;
        auto ret = mMemManager->GetMemory(MemoryType::FILE_STORE, capacity, dataAddress, true);
        RETURN_NULLPTR_AS_NOT_OK(ret);
        MemorySegmentRef segment = 
            MakeRef<MemorySegment>(capacity, reinterpret_cast<uint8_t*>(dataAddress), mMemManager);
        if (UNLIKELY(segment == nullptr)) {
            mMemManager->ReleaseMemory(dataAddress);
            return nullptr;
        }
        PQBinaryDataComparator comparator;
        auto list = std::make_shared<SkipList<PQBinaryData, PQBinaryDataComparator>>(comparator, segment,
            mSeqId.fetch_add(1));
        ret = list->Initialize();
        if (UNLIKELY(ret != BSS_OK)) {
            mMemManager->ReleaseMemory(dataAddress);
            return nullptr;
        }
        return list;
    }

    void Close() override
    {
        mSkipList = nullptr;
        return;
    }

    BResult TriggerSegmentFlush(bool force = false)
    {
        if (mSkipList->Empty()) {
            return BSS_OK;
        }
        AddFlushingSegment();
        PQTableIteratorRef iter = std::make_shared<PQIterator>(mSkipList, mStateId);
        auto processor = std::make_shared<SkiplistProcessor>(iter, mLsmStore, mSkipList,
            [this](const PQSkipList &item) {
                PollFlushingSegment(item);
        });
        if (force) {
            std::static_pointer_cast<Runnable>(processor)->Run();
        } else {
            auto ret = mService->Execute(std::static_pointer_cast<Runnable>(processor));
            if (UNLIKELY(!ret)) {
                LOG_ERROR("Submit task failed" << mService->QueueSize());
                return BSS_ERR;
            }
        }

        mSkipList = InitNewSkipList();
        return BSS_OK;
    }

private:
    void RequireMemoryUntilSuccess(const std::function<BResult()> &function)
    {
        while (true) {
            // 1. 将kv写入到skiplist中.
            BResult result = function();
            if (UNLIKELY(result != BSS_OK)) {
                // 2. 写入失败则将当前active segment淘汰掉, 然后重试.
                result = TriggerSegmentFlush();
                if (UNLIKELY(result != BSS_OK)) {
                    LOG_WARN("Trigger segment flush failed, need inner retry, ret:" << result);
                }
                continue;
            }
            return;
        }
    }

    inline void AddFlushingSegment()
    {
        WriteLocker<ReadWriteLock> lock(&mRwLock);
        mSnapshotQueue.PushBack(mSkipList);
    }

    inline bool PollFlushingSegment(const PQSkipList &item)
    {
        WriteLocker<ReadWriteLock> lock(&mRwLock);
        PQSkipList tmp = nullptr;
        mSnapshotQueue.TryPopFront(tmp);
        while (tmp != nullptr && item != tmp) {
            mSnapshotQueue.PushBack(tmp);
            mSnapshotQueue.TryPopFront(tmp);
        }
        return true;
    }

private:
    bool mInitialized = false;
    MemManagerRef mMemManager = nullptr;
    ExecutorServicePtr mService = nullptr;
    LsmStoreRef mLsmStore = nullptr;
    PQSkipList mSkipList;
    ReadWriteLock mRwLock;
    ConcurrentDeque<std::shared_ptr<SkipList<PQBinaryData, PQBinaryDataComparator>>> mSnapshotQueue;
    std::atomic<uint64_t> mSeqId {0};
    std::string mStateName;
    StateIdProviderRef mStateIdProvider;
    TableDescriptionRef mDescription;
    uint16_t mStateId = 0;
};
using PQTableRef = std::shared_ptr<PQTable>;

class SkiplistProcessor : public Runnable {
public:
    SkiplistProcessor(const PQTableIteratorRef iterator, const LsmStoreRef &lsmStore, const PQSkipList &skipList,
        SkipListCompletedNotify notify) : mIterator(iterator), mLsmStore(lsmStore),
        mSkipList(skipList), mNotify(notify) {}

    ~SkiplistProcessor() override = default;

    void Run() override
    {
        bool expect = false;
        BResult ret = BSS_ERR;
        do {
            if (!mResourceCleanupOwnershipTaken.compare_exchange_strong(expect, true)) {
                break;
            }
            if (UNLIKELY(mLsmStore == nullptr)) {
                break;
            }
            // write kv pair to lsmStore.
            ret = mLsmStore->Put(mIterator);
        } while (false);
        if (ret == BSS_OK) {
            mNotify(mSkipList);
        }
    }

private:
    PQTableIteratorRef mIterator;
    LsmStoreRef mLsmStore = nullptr;
    PQSkipList mSkipList;
    SkipListCompletedNotify mNotify;
    std::atomic<bool> mResourceCleanupOwnershipTaken{ false };
};
}
}
#endif  // BOOST_SS_PQ_TABLE_H