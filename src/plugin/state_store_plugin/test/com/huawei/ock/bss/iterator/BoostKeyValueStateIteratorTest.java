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

package com.huawei.ock.bss.iterator;

import com.huawei.ock.bss.common.BinaryKeyValueItem;
import com.huawei.ock.bss.common.BoostStateType;
import com.huawei.ock.bss.common.exception.BSSRuntimeException;
import com.huawei.ock.bss.common.memory.DirectDataOutputSerializer;
import com.huawei.ock.bss.iterator.struct.KVDataSorter;
import com.huawei.ock.bss.iterator.struct.KVDataSorterBuilder;
import com.huawei.ock.bss.ockdb.OckDBLog;
import com.huawei.ock.bss.snapshot.FullBoostSnapshotResources;
import com.huawei.ock.bss.snapshot.SavepointConfiguration;
import com.huawei.ock.bss.snapshot.SavepointDBResult;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {BoostKeyValueStateIterator.class, OckDBLog.class})
public class BoostKeyValueStateIteratorTest {
    private static final Logger LOG = LoggerFactory.getLogger(BoostKeyValueStateIteratorTest.class);

    private static final String WINDOWS_TEST_LOCAL_DIR = "D:\\tmp\\ockdb\\db";

    private static final String WINDOWS_TEST_REMOTE_DIR = "D:\\tmp\\ockdb\\remote\\db";

    private static final String WINDOWS_TEST_LOG_FILE = "D:\\tmp\\ockdb\\log\\bss.log";

    private BoostKeyValueStateIterator keyValueStateIterator_VALUE;

    private BoostKeyValueStateIterator keyValueStateIterator_MAP;

    private BoostQueueIterator queueIterator;

    private Map<String, String> kValueMap = new HashMap<>();

    private Map<String, Map<String, List<String>>> kMapMap = new HashMap<>();


    private ListSerializer<String> listSerializer = new ListSerializer<>(StringSerializer.INSTANCE);

    @Before
    public void setUp() throws Exception {
        Configuration configuration = new Configuration();
        final SavepointConfiguration savepointConfiguration =
            SavepointConfiguration.build(WINDOWS_TEST_LOCAL_DIR, WINDOWS_TEST_REMOTE_DIR);

        KeyGroupRange keyGroupRange = new KeyGroupRange(56, 86);
        int totalKeyGroups = 8;

        // MAP
        List<String> mapStateNames = new ArrayList<>();
        Map<String, FullBoostSnapshotResources.KvMetaData> mapStateMetas =
            getKvStateMetas(100, mapStateNames, StateDescriptor.Type.MAP);
        SavepointDBResult mapDBResult = getSavepointDBResult(mapStateNames, StateDescriptor.Type.MAP);
        KVDataSorter mapDataSorter =
            (new KVDataSorterBuilder<>(totalKeyGroups, keyGroupRange, StringSerializer.INSTANCE, savepointConfiguration,
                mapStateMetas, mapDBResult).build());

        BoostKeyValueIterator keyMapIterator = new BoostKeyValueIterator(mapDataSorter);

        // VALUE
        List<String> valueStateNames = new ArrayList<>();
        Map<String, FullBoostSnapshotResources.KvMetaData> valueStateMetas =
            getKvStateMetas(100, valueStateNames, StateDescriptor.Type.VALUE);
        SavepointDBResult valueDBResult = getSavepointDBResult(valueStateNames, StateDescriptor.Type.VALUE);
        KVDataSorter valueDataSorter =
            (new KVDataSorterBuilder<>(totalKeyGroups, keyGroupRange, StringSerializer.INSTANCE, savepointConfiguration,
                valueStateMetas, valueDBResult).build());

        BoostKeyValueIterator keyValueIterator = new BoostKeyValueIterator(valueDataSorter);
        BoostQueueIterator queueIterator = new BoostQueueIterator(new HashMap<>(),
            CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(totalKeyGroups));

        // 暂时看看单kvState行不行
        keyValueStateIterator_VALUE =
            new BoostKeyValueStateIterator(Collections.singletonList(keyValueIterator), keyGroupRange);
        // todo UV设置为list,查看format时的反序列化操作
        keyValueStateIterator_MAP =
            new BoostKeyValueStateIterator(Collections.singletonList(keyMapIterator), keyGroupRange);
    }

    @Test
    public void test_BoostKeyValueStateIterator_VALUE_normal() throws IOException {
        int count = 0;
        while (keyValueStateIterator_VALUE.isValid()) {
            count++;
            int stateId = keyValueStateIterator_VALUE.kvStateId();
            // 获取的迭代器最终返回给上层flink的key
            String key = new String(keyValueStateIterator_VALUE.key(), StandardCharsets.US_ASCII);
            String value = new String(keyValueStateIterator_VALUE.value(), StandardCharsets.US_ASCII);
            // 一个字节记录原始keyGroup
            Assert.assertTrue(key.charAt(0) >= 56 && key.charAt(0) <= 86);
            // 最后一个字节为'\0'结束符
            String valueStr = kValueMap.get(key.substring(1, key.length() - 1));
            Assert.assertEquals(valueStr, value);
            keyValueStateIterator_VALUE.next();
        }
        keyValueStateIterator_VALUE.close();
    }

    @Test
    public void test_BoostKeyValueStateIterator_MAP_normal() throws IOException {
        int count = 0;
        while (keyValueStateIterator_MAP.isValid()) {
            count++;
            int stateId = keyValueStateIterator_MAP.kvStateId();
            String key = new String(keyValueStateIterator_MAP.key(), StandardCharsets.US_ASCII);
            List<String> userValue = getUserListValue(keyValueStateIterator_MAP.value(), listSerializer);
            keyValueStateIterator_MAP.next();
        }
    }

    private List<String> getUserListValue(byte[] bytes, ListSerializer<String> listSerializer) throws IOException {
        DataInputDeserializer dataInputView = new DataInputDeserializer();
        dataInputView.setBuffer(Arrays.copyOfRange(bytes, 1, bytes.length));
        return listSerializer.deserialize(dataInputView);
    }

    private SavepointDBResult getSavepointDBResult(List<String> stateNames, StateDescriptor.Type type) {
        switch (type) {
            case VALUE:
                return new SavepointDBResultTestUtil(-1L,
                    getBinaryKeyValueItemIterator(stateNames, StateDescriptor.Type.VALUE));
            case MAP:
                return new SavepointDBResultTestUtil(-1L,
                    getBinaryKeyValueItemIterator(stateNames, StateDescriptor.Type.MAP));
            default:
                throw new BSSRuntimeException();
        }
    }

    private Map<String, FullBoostSnapshotResources.KvMetaData> getKvStateMetas(int num, List<String> stateNames,
        StateDescriptor.Type type) {
        Map<String, FullBoostSnapshotResources.KvMetaData> kvMetaDataMap = new HashMap<>();
        if (type == StateDescriptor.Type.VALUE){
            for (int i = 0; i < num; i++) {
                String stateDescName = "savepointValueStateTest" + i;
                FullBoostSnapshotResources.KvMetaData kvMetaData = getKvMetaData(num, stateDescName);
                stateNames.add(stateDescName);
                kvMetaDataMap.put(stateDescName, kvMetaData);
            }
            return kvMetaDataMap;
        } else if (type == StateDescriptor.Type.MAP) {
            for (int i = 0; i < num; i++) {
                String stateDescName = "savepointMapStateTest" + i;
                FullBoostSnapshotResources.KvMetaData kvMetaData = getMapMetaData(num, stateDescName);
                stateNames.add(stateDescName);
                kvMetaDataMap.put(stateDescName, kvMetaData);
            }
            return kvMetaDataMap;
        }
        throw new BSSRuntimeException();
    }

    private FullBoostSnapshotResources.KvMetaData getKvMetaData(int stateId, String name) {
        RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo;
        TypeSerializer<String> namespaceSerializer = StringSerializer.INSTANCE;
        TypeSerializer<String> newStateSerializer = StringSerializer.INSTANCE;
        ValueStateDescriptor<String> stateDesc = new ValueStateDescriptor<>(name, newStateSerializer, "defaultValue");

        metaInfo =
            new RegisteredKeyValueStateBackendMetaInfo<>(stateDesc.getType(), stateDesc.getName(), namespaceSerializer,
                newStateSerializer, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        StateMetaInfoSnapshot stateMetaInfoSnapshot = metaInfo.snapshot();

        return new FullBoostSnapshotResources.KvMetaData(stateId, StateDescriptor.Type.VALUE, stateMetaInfoSnapshot,
            null);
    }

    private FullBoostSnapshotResources.KvMetaData getMapMetaData(int stateId, String name) {
        RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo;
        TypeSerializer<String> namespaceSerializer = StringSerializer.INSTANCE;
        TypeSerializer<List<String>> userValueSerializer = new ListSerializer<>(StringSerializer.INSTANCE);
        MapSerializer<String, List<String>> newStateSerializer =
            new MapSerializer<>(StringSerializer.INSTANCE, userValueSerializer);
        MapStateDescriptor<String, List<String>> stateDesc =
            new MapStateDescriptor<>(name, StringSerializer.INSTANCE, userValueSerializer);

        metaInfo =
            new RegisteredKeyValueStateBackendMetaInfo<>(stateDesc.getType(), stateDesc.getName(), namespaceSerializer,
                newStateSerializer, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        StateMetaInfoSnapshot stateMetaInfoSnapshot = metaInfo.snapshot();

        return new FullBoostSnapshotResources.KvMetaData(stateId, StateDescriptor.Type.MAP, stateMetaInfoSnapshot,
            null);
    }

    private CloseableIterator<BinaryKeyValueItem> getBinaryKeyValueItemIterator(List<String> stateNames,
        StateDescriptor.Type type) {
        return new CloseableIterator<BinaryKeyValueItem>() {
            private int count = 0;

            DataOutputSerializer dataOutputView = new DataOutputSerializer(64);

            @Override
            public void close() throws Exception {
                LOG.info("Closing CloseableIterator<BinaryKeyValueItem>.");
            }

            @Override
            public boolean hasNext() {
                count++;
                // 共99次
                return count <= stateNames.size();
            }

            @Override
            public BinaryKeyValueItem next() {
                if (type == StateDescriptor.Type.VALUE) {
                    return getValueNext();
                } else if (type == StateDescriptor.Type.MAP) {
                    try {
                        return getMapNext();
                    } catch (IOException e) {
                        throw new BSSRuntimeException(e);
                    }
                }
                throw new BSSRuntimeException();
            }

            private BinaryKeyValueItem getValueNext() {
                if (count > stateNames.size()) {
                    return null;
                }
                BoostStateType stateType = BoostStateType.VALUE;
                String stateName = stateNames.get(count - 1);
                Random random = new Random();
                int keyGroup = random.nextInt(31) + 56;
                byte[] key = ("key" + count).getBytes(StandardCharsets.US_ASCII);
                DirectDataOutputSerializer keySer = new DirectDataOutputSerializer(key.length);
                keySer.write(key);
                byte[] namespace = "namespace".getBytes(StandardCharsets.US_ASCII);
                DirectDataOutputSerializer nameSer = new DirectDataOutputSerializer(key.length);
                nameSer.write(namespace);
                byte[] mapKey = ("mapKey" + count).getBytes(StandardCharsets.US_ASCII);
                DirectDataOutputSerializer mapKeySer = new DirectDataOutputSerializer(key.length);
                mapKeySer.write(mapKey);
                byte[] value = ("value" + count).getBytes(StandardCharsets.US_ASCII);
                DirectDataOutputSerializer valSer = new DirectDataOutputSerializer(key.length);
                valSer.write(value);
                String keyStr = new String(key, StandardCharsets.US_ASCII);
                String valueStr = new String(value, StandardCharsets.US_ASCII);
                kValueMap.put(keyStr, valueStr);
                return new BinaryKeyValueItem(stateType, stateName, keyGroup, keySer.data(), key.length, nameSer.data(),
                    namespace.length, mapKeySer.data(), mapKey.length, valSer.data(), value.length);
            }

            private BinaryKeyValueItem getMapNext() throws IOException {
                if (count > stateNames.size()) {
                    return null;
                }
                BoostStateType stateType = BoostStateType.MAP;
                String stateName = stateNames.get(count - 1);
                Random random = new Random();
                int keyGroup = random.nextInt(31) + 56;
                byte[] key = ("key" + count).getBytes(StandardCharsets.US_ASCII);
                DirectDataOutputSerializer keySer = new DirectDataOutputSerializer(key.length);
                keySer.write(key);
                byte[] namespace = "namespace".getBytes(StandardCharsets.US_ASCII);
                DirectDataOutputSerializer nameSer = new DirectDataOutputSerializer(key.length);
                nameSer.write(namespace);
                byte[] mapKey = ("mapKey" + count).getBytes(StandardCharsets.US_ASCII);
                DirectDataOutputSerializer mapKeySer = new DirectDataOutputSerializer(key.length);
                mapKeySer.write(mapKey);
                String keyStr = new String(key, StandardCharsets.US_ASCII);
                List<String> mapValue = new ArrayList<>();
                String element = "value";
                for (int i = 0; i < 3; i++) {
                    mapValue.add(element + count);
                }
                byte[] value = getSerializedMapValue(mapValue);
                DirectDataOutputSerializer valSer = new DirectDataOutputSerializer(key.length);
                valSer.write(value);
                kMapMap.put(keyStr, Collections.singletonMap(keyStr, mapValue));
                return new BinaryKeyValueItem(stateType, stateName, keyGroup, keySer.data(), key.length, nameSer.data(),
                        namespace.length, mapKeySer.data(), mapKey.length, valSer.data(), value.length);
            }

            private byte[] getSerializedMapValue(List<String> mapValue) throws IOException {
                dataOutputView.setPosition(0);
                listSerializer.serialize(mapValue, dataOutputView);
                return dataOutputView.getCopyOfBuffer();
            }
        };
    }
}