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

#ifndef BOOST_SS_FILE_ID_GENERRATOR_H
#define BOOST_SS_FILE_ID_GENERRATOR_H

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include "file_info.h"

namespace ock {
namespace bss {
class FileIdRecorder {
public:
    explicit FileIdRecorder(uint32_t maxAllowedUniqueID) : mMaxAllowedUniqueID(maxAllowedUniqueID), mCardinality(0)
    {
        mUsedFileUniqueIDs = std::vector<bool>(maxAllowedUniqueID, false);
    }

    ~FileIdRecorder()
    {
        std::vector<bool>().swap(mUsedFileUniqueIDs);
    }

    inline bool IsIDRecorded(uint32_t uniqueID) const
    {
        if (uniqueID >= mMaxAllowedUniqueID) {
            LOG_ERROR("Invalid id: " << uniqueID);
            return false;
        }
        return mUsedFileUniqueIDs[uniqueID];
    }

    inline void RecordUniqueID(uint32_t uniqueID)
    {
        if (UNLIKELY(uniqueID >= mMaxAllowedUniqueID)) {
            LOG_ERROR("Invalid id: " << uniqueID);
            return;
        }
        bool isRecorded = mUsedFileUniqueIDs[uniqueID];
        mUsedFileUniqueIDs[uniqueID] = true;
        if (!isRecorded) {
            mCardinality++;
        }
    }

    inline uint32_t RecordedFileIdNum() const
    {
        return mCardinality;
    }

    inline void RecycleFileId(uint32_t uniqueId)
    {
        if (uniqueId >= mMaxAllowedUniqueID) {
            return;
        }
        bool isRecorded = mUsedFileUniqueIDs[uniqueId];
        if (isRecorded) {
            mUsedFileUniqueIDs[uniqueId] = false;
            mCardinality--;
        }
    }

private:
    std::vector<bool> mUsedFileUniqueIDs;
    uint32_t mMaxAllowedUniqueID = 0;
    uint32_t mCardinality = 0;
};
using FileIdRecorderRef = std::shared_ptr<FileIdRecorder>;

class FileIdGenerator {
public:
    explicit FileIdGenerator(FileModeInfo fileModeInfo)
        : mFileMode(fileModeInfo), mFileIdRecorder(std::make_shared<FileIdRecorder>(mFileMode.maxAllowedUniqueID))
    {
        mInitValue = mFileMode.initValue;
    }

    FileIdGenerator(uint32_t initUniqueIdValue, FileModeInfo fileModeInfo)
        : mFileMode(fileModeInfo), mFileIdRecorder(std::make_shared<FileIdRecorder>(mFileMode.maxAllowedUniqueID))
    {
        mInitValue = mFileMode.initValue;
        mUniqueId.store(initUniqueIdValue);
    }

    FileIdRef Generate();

    inline uint32_t GetMaxAllowUniqueId() const
    {
        return mFileMode.maxAllowedUniqueID;
    }

    inline void Recycle(const FileIdRef &fileId)
    {
        RETURN_AS_NULLPTR(fileId);
        std::lock_guard<std::mutex> lock(mMutex);
        RecycleUniqueId(fileId->GetUniqueId());
    }

    void Restore(std::vector<FileIdRef> &fileIds);

    inline void RecycleUniqueId(uint32_t uniqueId)
    {
        mFileIdRecorder->RecycleFileId(uniqueId);
    }

    inline void RecordUniqueId(uint32_t uniqueId)
    {
        mFileIdRecorder->RecordUniqueID(uniqueId);
    }

private:
    FileModeInfo mFileMode;
    uint32_t mInitValue = 0;
    static std::atomic<uint32_t> mUniqueId;
    FileIdRecorderRef mFileIdRecorder = nullptr;
    mutable std::mutex mMutex;
};
using FileIdGeneratorRef = std::shared_ptr<FileIdGenerator>;

}  // namespace bss
}  // namespace ock
#endif  // BOOST_SS_FILE_ID_GENERRATOR_H