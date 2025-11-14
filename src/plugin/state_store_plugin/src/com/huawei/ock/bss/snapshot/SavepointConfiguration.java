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

package com.huawei.ock.bss.snapshot;

import com.huawei.ock.bss.EmbeddedOckStateBackend;
import com.huawei.ock.bss.OckDBOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SavepointConfiguration类
 *
 * @since 2025年1月12日16:26:41
 */
public class SavepointConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(SavepointConfiguration.class);

    /**
     * Maximum number of output streams opened simultaneously.
     */
    private static final int SAVEPOINT_EXTERNAL_SORT_MAX_OUTPUT_STREAM = 8;

    private static final float DEFAULT_LOCAL_USAGE_THRESHOLD = 0.0F;

    /**
     * Local storage will be selected for external sort if local space usage is no more than the threshold
     */
    private static final float SAVEPOINT_EXTERNAL_SORT_AUTO_SELECT_LOCAL_THRESHOLD = 0.3F;

    /**
     * Maximum size of a file created for external sort.
     */
    private static final String SAVEPOINT_EXTERNAL_SORT_FILE_SIZE = "64mb";

    /**
     * Maximum size of a block in a file for external sort
     */
    private static final String SAVEPOINT_EXTERNAL_SORT_BLOCK_SIZE = "64kb";

    private static final String DEFAULT_LOCAL_TMP_PATH = "/usr/local/flink/savepoint/tmp";

    private final int externalSortMaxOutputStream;

    private final int externalSortBlockSize;

    private final long externalSortFileSize;

    private final float localUsageThreshold;

    private final boolean localStorageEnabled;

    private final boolean storageAutoSelect;

    private final boolean externalSortCompressionEnabled;

    private final boolean externalSortChecksumEnabled;

    private final Path externalSortLocalTemporaryPath;

    private final Path externalSortRemoteTemporaryPath;

    private SavepointConfiguration(Path externalSortLocalTemporaryPath, Path externalSortRemoteTemporaryPath,
        boolean localStorageEnabled, boolean storageAutoSelect, float localUsageThreshold,
        int externalSortMaxOutputStream, long externalSortFileSize, int externalSortBlockSize,
        boolean externalSortCompressionEnabled, boolean externalSortChecksumEnabled) {
        this.externalSortLocalTemporaryPath = externalSortLocalTemporaryPath;
        this.externalSortRemoteTemporaryPath = externalSortRemoteTemporaryPath;
        this.localStorageEnabled = localStorageEnabled;
        this.storageAutoSelect = storageAutoSelect;
        this.localUsageThreshold = localUsageThreshold;
        this.externalSortMaxOutputStream = externalSortMaxOutputStream;
        this.externalSortFileSize = externalSortFileSize;
        this.externalSortBlockSize = externalSortBlockSize;
        this.externalSortCompressionEnabled = externalSortCompressionEnabled;
        this.externalSortChecksumEnabled = externalSortChecksumEnabled;
    }

    public Path getExternalSortLocalTemporaryPath() {
        return this.externalSortLocalTemporaryPath;
    }

    public Path getExternalSortRemoteTemporaryPath() {
        return this.externalSortRemoteTemporaryPath;
    }

    public boolean isLocalStorageEnabled() {
        return this.localStorageEnabled;
    }

    public boolean isStorageAutoSelect() {
        return this.storageAutoSelect;
    }

    public float getLocalUsageThreshold() {
        return this.localUsageThreshold;
    }

    public int getExternalSortMaxOutputStream() {
        return this.externalSortMaxOutputStream;
    }

    public long getExternalSortFileSize() {
        return this.externalSortFileSize;
    }

    public int getExternalSortBlockSize() {
        return this.externalSortBlockSize;
    }

    public boolean isExternalSortCompressionEnabled() {
        return this.externalSortCompressionEnabled;
    }

    public boolean isExternalSortChecksumEnabled() {
        return this.externalSortChecksumEnabled;
    }

    /**
     * toString
     *
     * @return String
     */
    public String toString() {
        return "SavepointConfiguration{externalSortLocalTemporaryPath=" + this.externalSortLocalTemporaryPath.getName()
            + ", externalSortRemoteTemporaryPath=" + this.externalSortRemoteTemporaryPath.getName()
            + ", localStorageEnabled="
            + this.localStorageEnabled + ", storageAutoSelect=" + this.storageAutoSelect + ", localUsageThreshold="
            + this.localUsageThreshold + ", externalSortMaxOutputStream=" + this.externalSortMaxOutputStream
            + ", externalSortFileSize=" + this.externalSortFileSize + ", externalSortBlockSize="
            + this.externalSortBlockSize + ", externalSortCompressionEnabled=" + this.externalSortCompressionEnabled
            + ", externalSortChecksumEnabled=" + this.externalSortChecksumEnabled + '}';
    }

    /**
     * In fact, configs to select path for ex-sort does not make sense in this version.
     *
     * @param configuration configuration
     * @return SavepointConfiguration
     */
    public static SavepointConfiguration build(Configuration configuration) {
        boolean localStorageEnabled = true;
        boolean storageAutoSelect = false;
        boolean compressionEnabled = false;
        boolean checksumEnabled = false;

        long fileSize = (MemorySize.parse(SAVEPOINT_EXTERNAL_SORT_FILE_SIZE)).getBytes();
        long blockSize = (MemorySize.parse(SAVEPOINT_EXTERNAL_SORT_BLOCK_SIZE)).getBytes();

        Path localPath = new Path(configuration.get(OckDBOptions.SAVEPOINT_EX_SORT_DIRECTORIES));
        EmbeddedOckStateBackend.checkPathValid(localPath.getPath(),
            OckDBOptions.SAVEPOINT_EX_SORT_DIRECTORIES.key());
        Path remotePath = new Path(configuration.get(OckDBOptions.SAVEPOINT_EX_SORT_DIRECTORIES));
        EmbeddedOckStateBackend.checkPathValid(remotePath.getPath(),
            OckDBOptions.SAVEPOINT_EX_SORT_DIRECTORIES.key());

        SavepointConfiguration savepointConfiguration =
            new SavepointConfiguration(localPath, remotePath, localStorageEnabled, storageAutoSelect,
                SAVEPOINT_EXTERNAL_SORT_AUTO_SELECT_LOCAL_THRESHOLD, SAVEPOINT_EXTERNAL_SORT_MAX_OUTPUT_STREAM,
                fileSize, (int) blockSize, compressionEnabled, checksumEnabled);

        LOG.info("Build savepoint configuration, {}", savepointConfiguration);
        return savepointConfiguration;
    }

    /**
     * 主流程初始化时调用，DT调用；
     * 由于主流程中之前初始化了savepointConfiguration参数，如果采用直接移除该参数的方式分离两个流程的配置初始化可能导致之前相关DT失败，
     * 因此添加此方法，可以不需要读取配置项生成一个SavepointConfiguration对象，还作为DT的打桩；
     * savepoint真正执行时生效的参数在另一个build方法中从配置项读取并初始化
     *
     * @param localPath  localPath
     * @param remotePath remotePath
     * @return SavepointConfiguration
     */
    public static SavepointConfiguration build(String localPath, String remotePath) {
        if (localPath == null || localPath.isEmpty()) {
            localPath = DEFAULT_LOCAL_TMP_PATH;
        }

        if (remotePath == null || remotePath.isEmpty()) {
            remotePath = DEFAULT_LOCAL_TMP_PATH;
        }

        return new SavepointConfiguration(
            new Path(localPath),
            new Path(remotePath),
            true,
            false,
            DEFAULT_LOCAL_USAGE_THRESHOLD,
            SAVEPOINT_EXTERNAL_SORT_MAX_OUTPUT_STREAM,
            MemorySize.parse(SAVEPOINT_EXTERNAL_SORT_FILE_SIZE).getBytes(),
            (int) MemorySize.parse(SAVEPOINT_EXTERNAL_SORT_BLOCK_SIZE).getBytes(),
            false,
            true);
    }

    /**
     * decideExternalSortTemporaryPath
     *
     * @param savepointConfiguration SavepointConfiguration
     * @return Path
     */
    public static Path decideExternalSortTemporaryPath(SavepointConfiguration savepointConfiguration) {
        return savepointConfiguration.getExternalSortLocalTemporaryPath();
    }
}
