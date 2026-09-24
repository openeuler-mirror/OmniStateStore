/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "gtest/gtest.h"
#include "include/config.h"

namespace ock {
namespace bss {

TEST(ConfigTest, DefaultConstructorDisablesLocalRecovery)
{
    Config config;
    EXPECT_FALSE(config.GetEnableLocalRecovery());
}

TEST(ConfigTest, ParameterizedConstructorDisablesLocalRecovery)
{
    Config config(0, 127, 128);
    EXPECT_FALSE(config.GetEnableLocalRecovery());
}

TEST(ConfigTest, ExplicitlyEnablesLocalRecovery)
{
    Config config;
    config.SetEnableLocalRecovery(true);
    EXPECT_TRUE(config.GetEnableLocalRecovery());
}

}  // namespace bss
}  // namespace ock
