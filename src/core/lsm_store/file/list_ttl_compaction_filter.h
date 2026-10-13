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

#ifndef BOOST_SS_LIST_TTL_COMPACTION_FILTER_H
#define BOOST_SS_LIST_TTL_COMPACTION_FILTER_H
#include <unordered_map>

#include "binary/value/value.h"
namespace ock {
namespace bss {
// A separate compaction-only callback; ordinary state filters never invoke it.
using ListTtlCompactionFilter = std::function<BResult(Value &, const Value::BufferAllocator &)>;
using ListTtlCompactionFilters = std::unordered_map<uint16_t, ListTtlCompactionFilter>;
}  // namespace bss
}  // namespace ock
#endif
