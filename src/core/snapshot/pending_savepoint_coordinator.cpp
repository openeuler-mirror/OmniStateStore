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
#include "pending_savepoint_coordinator.h"

namespace ock {
namespace bss {
BResult PendingSavepointCoordinator::Start()
{
    {
        std::lock_guard<std::mutex> lock(mMutex);
        if (mState.load() != State::CREATED) {
            return BSS_ERR;
        }
        mState.store(State::RUNNING);
    }

    // 1. FreshTable执行savepoint.
    auto freshTableSnapshotOperator = std::make_shared<FreshTableSnapshotOperator>(AllocateOperatorId(), mFreshTable,
                                                                                   nullptr, mMemManager);
    RegisterSnapshotOperator(freshTableSnapshotOperator);
    freshTableSnapshotOperator->Start();
    RETURN_NOT_OK(freshTableSnapshotOperator->SyncSnapshot(true));

    // 2. SliceTable执行savepoint.
    auto sliceTableSnapshotOperator = std::make_shared<SliceTableSnapshotOperator>(AllocateOperatorId(), mSliceTable,
                                                                                   nullptr, mMemManager,
                                                                                   GetSnapshotId());
    RegisterSnapshotOperator(sliceTableSnapshotOperator);
    sliceTableSnapshotOperator->Start();
    RETURN_NOT_OK(sliceTableSnapshotOperator->SyncSnapshot(true));

    // 3. FileStore执行savepoint.
    auto fileStoreSnapshotOperator = mSliceTable->PrepareFileStoreSnapshot(AllocateOperatorId(),
        GetSnapshotId());
    RegisterSnapshotOperator(fileStoreSnapshotOperator);
    fileStoreSnapshotOperator->Start();
    return fileStoreSnapshotOperator->SyncSnapshot(true);
}

void PendingSavepointCoordinator::Cancel()
{
    {
        std::lock_guard<std::mutex> lock(mMutex);
        if (IsFinalState()) {
            return;
        }
        mState = State::CANCELED;
    }
    FailSnapshot();
}

void PendingSavepointCoordinator::FailSnapshot()
{
    std::lock_guard<std::mutex> lock(mMutex);
    for (const auto &snapshotOperator : mRegisteredSnapshotOperators) {
        snapshotOperator.second->Cancel();
    }
    mRegisteredSnapshotOperators.clear();
}

}  // namespace bss
}  // namespace ock
