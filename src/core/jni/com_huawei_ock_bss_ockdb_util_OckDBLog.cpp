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
#include "com_huawei_ock_bss_ockdb_util_OckDBLog.h"

#include <string>

#include "bss_def.h"
#include "bss_log.h"
#include "kv_helper.h"
using namespace ock::bss;

/*
 * Class:     com_huawei_ock_bss_ockdb_util_OckDBLog
 * Method:    initial
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_ock_bss_ockdb_OckDBLog_initial(JNIEnv *env, jclass, jstring jlogPath,
                                                                       jint jloglevel, jint jsize, jint jcount)
{
    if (UNLIKELY(env == nullptr)) {
        LOG_ERROR("Input env is nullptr.");
        return 0;
    }
    if (UNLIKELY(jlogPath == nullptr)) {
        LOG_ERROR("LogPath is invalid.");
        return 0;
    }
    if (UNLIKELY(jloglevel < 0 || jloglevel > MIN_LOG_LEVEL_MAX)) {
        LOG_ERROR("LogLevel is out of bounds.");
        return 0;
    }
    if (UNLIKELY(jsize <= 0)) {
        LOG_ERROR("LogSize less than zero.");
        return 0;
    }
    if (UNLIKELY(jcount <= 0)) {
        LOG_ERROR("LogCount less than zero.");
        return 0;
    }
    const char *logPath = env->GetStringUTFChars(jlogPath, nullptr);
    if (UNLIKELY(logPath == nullptr)) {
        LOG_ERROR("Path is null.");
        return 0;
    }
    std::string logPathStr(logPath);
    env->ReleaseStringUTFChars(jlogPath, logPath);
    if (UNLIKELY(!CheckPathValid(PathTransform::ExtractDirectory(logPathStr)))) {
        LOG_ERROR("Invalid Log Paths, check whether the log path configuration items comply with the specifications.");
        return 0;
    }
    const LoggerOptions loggerOption = { 1, static_cast<int32_t>(jloglevel), static_cast<uint32_t>(jsize),
        static_cast<uint32_t>(jcount), logPathStr };
    jlong logHandle;
    if (Logger::gInstance) {
        logHandle = reinterpret_cast<jlong>(Logger::gInstance);
        return logHandle;
    }
    Logger::Instance(loggerOption);
    if (UNLIKELY(Logger::gInstance == nullptr)) {
        LOG_ERROR("gInstance is nullptr.");
        return 0;
    }
    Logger::gInstance->Init();
    logHandle = reinterpret_cast<jlong>(Logger::gInstance);
    return logHandle;
}
