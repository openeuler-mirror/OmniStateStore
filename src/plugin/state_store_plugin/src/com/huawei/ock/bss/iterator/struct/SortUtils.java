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

package com.huawei.ock.bss.iterator.struct;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nullable;

/**
 * SortUtils
 *
 * @since BeiMing 25.0.T1
 */
public class SortUtils {
    /**
     * buildSavepointTemporaryPath
     *
     * @param basePath basePath
     * @param savepointId savepointId
     * @return Path
     */
    public static Path buildSavepointTemporaryPath(Path basePath, long savepointId) {
        return new Path(basePath,
            String.format("savepoint-%s-%s", savepointId, UUID.randomUUID()));
    }

    /**
     * deleteFileQuietly
     *
     * @param path path
     * @param logger logger
     */
    public static void deleteFileQuietly(Path path, @Nullable Logger logger) {
        try {
            path.getFileSystem().delete(path, false);
            if (logger != null) {
                logger.info("Deleted file {}", path.getName());
            }
        } catch (IOException ie) {
            if (logger != null) {
                logger.error("Failed to delete file {}", path.getName(), ie);
            }
        }
    }

    /**
     * deleteFileQuietly
     *
     * @param paths paths
     * @param logger logger
     */
    public static void deleteFileQuietly(Collection<Path> paths, @Nullable Logger logger) {
        paths.forEach(path -> deleteFileQuietly(path, logger));
    }

    /**
     * splitKeyGroups
     *
     * @param keyGroupRange KeyGroupRange
     * @param numSplits numSplits
     * @return List<KeyGroupRange>
     */
    public static List<KeyGroupRange> splitKeyGroups(KeyGroupRange keyGroupRange, int numSplits) {
        int numKeyGroups = keyGroupRange.getNumberOfKeyGroups();
        int avgKeyGroups = numKeyGroups / numSplits;
        int modKeyGroups = numKeyGroups % numSplits;

        int size = (avgKeyGroups > 0) ? numSplits : modKeyGroups;
        List<KeyGroupRange> splitKeyGroupRanges = new ArrayList<>(size);
        for (int i = 0, start = keyGroupRange.getStartKeyGroup(); i < size; i++) {
            int num = avgKeyGroups + ((i < modKeyGroups) ? 1 : 0);
            KeyGroupRange range = new KeyGroupRange(start, start + num - 1);
            splitKeyGroupRanges.add(range);
            start += num;
        }
        return splitKeyGroupRanges;
    }
}



