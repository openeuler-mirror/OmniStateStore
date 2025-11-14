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

#include "fresh_transformer.h"

namespace ock {
namespace bss {
BResult FreshHandler::Handle(BoostSegmentRef segment)
{
    // 1. 组织数据.
    std::unordered_map<SliceIndexContextRef, std::shared_ptr<RawDataSlice>, SliceIndexContextHash,
                       SliceIndexContextEqual> organizedData;
    BResult ret = DivideFreshData(organizedData, segment);
    if (UNLIKELY(ret != BSS_OK)) {
        LOG_ERROR("Organize fresh table memory segment failed, ret:" << ret);
        return ret;
    }

    // 2. 写入到sliceTable中.
    return WriteDataToSlice(organizedData);
}

BResult FreshHandler::DivideFreshData(
    std::unordered_map<SliceIndexContextRef, std::shared_ptr<RawDataSlice>, SliceIndexContextHash,
    SliceIndexContextEqual> &organizedData, const BoostSegmentRef &segment)
{
    if (UNLIKELY(segment == nullptr)) {
        LOG_ERROR("Memory segment is nullptr.");
        return BSS_ERR;
    }
    BoostHashMapRef binaryData = segment->GetBinaryData();
    if (UNLIKELY(binaryData == nullptr)) {
        LOG_ERROR("Memory segment boost data is nullptr.");
        return BSS_ERR;
    }

    // 从segment获取迭代器依次转换加到rawDataSlice中.
    auto iterator = binaryData->KVIterator();
    RETURN_ALLOC_FAIL_AS_NULLPTR(iterator);
    std::vector<std::pair<BinaryKey, FreshValueNodePtr>> collection;
    while (iterator->HasNext()) {
        auto kvPair = iterator->Next();
        collection.clear();
        BResult result = DoTrans(kvPair.first, kvPair.second, collection, segment);
        if (UNLIKELY(result != BSS_OK)) {
            LOG_ERROR("Failed to do kv pair transform, ret:" << result);
            return result;
        }
        for (const auto &pair : collection) {
            SliceIndexContextRef context =
                mSliceTable->GetSliceBucketIndex()->GetSliceIndexContext(pair.first.mKeyHashCode, true);
            RETURN_NOT_OK_AS_FALSE(UNLIKELY(context == nullptr), BSS_ERR);
            std::shared_ptr<RawDataSlice> rawDataSlice;
            auto finder = organizedData.find(context);
            if (finder != organizedData.end()) {
                rawDataSlice = finder->second;
            } else {
                rawDataSlice = std::make_shared<RawDataSlice>(binaryData->GetMemorySegment(), segment->GetVersion());
                organizedData.emplace(context, rawDataSlice);
            }
            rawDataSlice->AddBinaryData(pair);
            rawDataSlice->PutMixHashCode(pair.first.mMixedHashCode);
            rawDataSlice->PutIndexVec();
        }
    }
    return BSS_OK;
}

BResult FreshHandler::DoTrans(FreshKeyNodePtr key, FreshValueNodePtr value,
                              std::vector<std::pair<BinaryKey, FreshValueNodePtr>> &collection,
                              const BoostSegmentRef &segment)
{
    RETURN_INVALID_PARAM_AS_NULLPTR(key);
    RETURN_INVALID_PARAM_AS_NULLPTR(value);
    PriKey primaryKey;
    primaryKey.Parse(key);
    uint16_t stateId = primaryKey.mStateId;
    StateType stateType = StateId::GetStateType(stateId);
    if (StateTypeUtil::HasSecKey(stateType)) {
        BoostHashMapRef hashMap = MakeRef<BoostHashMap>();
        if (UNLIKELY(hashMap == nullptr)) {
            LOG_ERROR("MakeRef failed, boostHashMap is null.");
            return BSS_ERR;
        }

        uint32_t offset = value->MapData() - segment->GetMemorySegment()->GetSegment();
        hashMap->Init(segment->GetMemorySegment(), offset, false);
        auto iterator = hashMap->KVIterator();
        RETURN_ERROR_AS_NULLPTR(iterator);
        while (iterator->HasNext()) {
            auto entry = iterator->Next();
            SecKey secondKey;
            secondKey.Parse(entry.first);
            BinaryKey binaryKey;
            binaryKey.Parse(primaryKey, secondKey);
            LOG_TRACE(binaryKey.ToString() << entry.second->ToString());
            collection.emplace_back(std::move(binaryKey), entry.second);
        }
    } else {
        BinaryKey binaryKey;
        binaryKey.Parse(primaryKey, stateType == VALUE);
        collection.emplace_back(std::move(binaryKey), value);
        LOG_TRACE(binaryKey.ToString() << value->ToString());
    }
    return BSS_OK;
}

BResult FreshHandler::WriteDataToSlice(
    std::unordered_map<SliceIndexContextRef, RawDataSliceRef, SliceIndexContextHash,
                       SliceIndexContextEqual> &organizedData)
{
    if (UNLIKELY(organizedData.empty())) {
        LOG_WARN("Organized data is empty, not need to write data to slice.");
        return BSS_OK;
    }

    BResult ret = BSS_OK;
    for (const auto &iter : organizedData) {
        SliceIndexContextRef bucketIndexContext = iter.first;
        uint32_t addSize = 0;
        bool forceEvict = false;
        do {
            // 将fresh数据写入SliceTable
            ret = mSliceTable->AddSlice(bucketIndexContext, *iter.second, addSize, forceEvict);
            if (ret == BSS_OK && forceEvict) {
                // 避免list合并, 基于当前DB, 以4M的文件粒度淘汰整个DB数据
                mSliceTable->TryCurrentDBEvict(addSize, true, true, IO_SIZE_4M);
                break;
            }
            // 基于当前slot, 异步淘汰SliceTable数据
            mSliceTable->TryCurrentSlotEvict(ret == BSS_OK ? addSize : 0, false, ret == BSS_ALLOC_FAIL);
        } while (UNLIKELY(ret == BSS_ALLOC_FAIL) && (usleep(NO_100), 1));
        RETURN_NOT_OK(ret);
        auto self = shared_from_this();
        mSliceTable->TryCompact(bucketIndexContext, [self](LogicalSliceChainRef logicalSliceChain,
                                                   uint32_t bucketIndex, uint32_t compactionStartChainIndex,
                                                   uint32_t compactionEndChainIndex, DataSliceRef compactedDataSlice,
                                                   std::vector<SliceAddressRef> invalidSliceAddressList,
                                                   uint32_t finalOldSliceSize, bool fromRestore) {
            self->CompactCallback(logicalSliceChain, bucketIndex, compactionStartChainIndex, compactionEndChainIndex,
                                  compactedDataSlice, invalidSliceAddressList);
        });
    }
    return ret;
}

void FreshHandler::CompactCallback(LogicalSliceChainRef &logicalSliceChain, uint32_t bucketIndex,
                                   uint32_t compactionStartChainIndex, uint32_t compactionEndChainIndex,
                                   DataSliceRef &compactedDataSlice,
                                   std::vector<SliceAddressRef> &invalidSliceAddressList)
{
    RETURN_AS_NULLPTR(logicalSliceChain);
    RETURN_AS_NULLPTR(compactedDataSlice);

    ReplaceLogicalSliceRef replaceLogicalSlice = std::make_shared<ReplaceLogicalSlice>(mSliceTable, mConfig);
    RunnablePtr processor = std::make_shared<ReplaceLogicalSliceTask>(replaceLogicalSlice, logicalSliceChain,
                                                                     bucketIndex, compactionStartChainIndex,
                                                                     compactionEndChainIndex, compactedDataSlice,
                                                                     invalidSliceAddressList);
    if (!mTransExecutor->Execute(processor, true)) {
        logicalSliceChain->SetCompactionToNormal();
        LOG_ERROR("Execute replace logic slice task failed.");
    }
}

void FreshTransformer::TriggerTransform()
{
    // 生成transform任务并调度执行
    FreshHandlerRef mHandle = std::make_shared<FreshHandler>(mConfig, mSliceTable, mTransformExecutor);
    auto self = shared_from_this();
    RunnablePtr processor = std::make_shared<TransformerProcessor>(mFreshTable, mSliceTable, mHandle);
    bool isOk = mTransformExecutor->Execute(processor, false);
    if (UNLIKELY(!isOk)) {
        LOG_ERROR("Execute transform failed.");
    }
}

}  // namespace bss
}  // namespace ock