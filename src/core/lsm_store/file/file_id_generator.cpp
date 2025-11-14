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

#include "common/bss_log.h"
#include "file_id_generator.h"

namespace ock {
namespace bss {
std::atomic<uint32_t> FileIdGenerator::mUniqueId = { 1 };

FileIdRef FileIdGenerator::Generate()
{
    std::lock_guard<std::mutex> lock(mMutex);
    if (UNLIKELY(mFileIdRecorder->RecordedFileIdNum() >= GetMaxAllowUniqueId())) {
        LOG_ERROR("The number of unique file ids exceeds the maximum limit.");
        return nullptr;
    }
    uint32_t currentUniqueId = mUniqueId.fetch_add(1);
    while (mFileIdRecorder->IsIDRecorded(currentUniqueId)) {
        currentUniqueId = mUniqueId.fetch_add(1);
        if (currentUniqueId > GetMaxAllowUniqueId()) {
            mUniqueId.store(1);
            currentUniqueId = 1;
        }
    }

    mFileIdRecorder->RecordUniqueID(currentUniqueId);
    FileIdRef fileId = std::make_shared<FileId>();
    BResult result = fileId->Init(mInitValue | currentUniqueId);
    if (result != BSS_OK) {
        LOG_ERROR("Init fileId failed, initValue: " << mInitValue << ", currentUniqueId:" << currentUniqueId);
        return nullptr;
    }
    return fileId;
}

void FileIdGenerator::Restore(std::vector<FileIdRef> &fileIds)
{
    std::lock_guard<std::mutex> lock(mMutex);
    for (const auto &fileId : fileIds) {
        RecordUniqueId(fileId->GetUniqueId());
        LOG_DEBUG("Restore fileId generator success, unique id:" << fileId->GetUniqueId());
    }
}

}  // namespace bss
}  // namespace ock