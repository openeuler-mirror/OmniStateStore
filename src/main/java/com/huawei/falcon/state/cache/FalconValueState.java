package com.huawei.falcon.state.cache;

import java.util.Map;
import java.util.LinkedHashMap;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.contrib.streaming.state.RocksDBValueState;

import com.huawei.falcon.state.cache.api.FalconValueStateAPI;
import com.huawei.falcon.state.cache.utils.FalconValue;
import com.huawei.falcon.state.cache.utils.FalconKey;

import static org.apache.flink.optimizer.Optimizer.LOG;

/**
 * {@link FalconValueStateAPI} implementation, which is bind with a {@link RocksDBValueState} instance. Currently,
 * FalconValueState only support LRU cache method, details are as follows.
 *
 * <p> For states(FalconKey-FalconValue pairs) corresponding to this columnFamily, we use a LinkedHashMap to store them.
 * By setting the accessOrder as true, when a state is read、added or updated, it will be moved to the tail of link.
 * Thus, hot states are located at the tail of link, while cold states are located at the head of link.
 *
 * <p> Using LinkedHashMap, read、write、hot state insertion and cold state elimination are all O(1) time complexity.
 *
 * @brief FALCON implementation
 * @param <K> Type of ValueState's key
 * @param <N> Type of ValueState's namespace
 * @param <V> Type of ValueState's value
 */
public class FalconValueState<K, N, V> implements FalconValueStateAPI<K, N, V> {
    private final LinkedHashMap<FalconKey<K, N>, FalconValue<V>> cache;
    private int cacheSizeLimit; // falcon cache size limit, 0 means disable falcon cache, otherwise enable falcon cache
    private final RocksDBValueState<K, N, V> valueState; // used for falcon cache to access RocksDB
    private long accessCnt;     // frequency of accessing falcon cache, use long type (2^64) to avoid overflow
    private long hitCnt;        // frequency of hitting falcon cache
    // get falcon cache hit ratio threshold from yaml file. default value is -1.0, which means open cache all the time.
    public static final double hitThr = GlobalConfiguration.loadConfiguration().get(
        ConfigOptions.key("state.backend.rocksdb.falcon.state-cache-bypass-hitRatio")
            .doubleType()
            .defaultValue(-1.0)
            .withDescription("If falcon cache hitRatio is less than the given value, then bypass falcon cache.")
    );
    public FalconValueState(RocksDBValueState<K, N, V> dbAccessor) {
        // default cacheSizeLimit is 0, which means disable falcon. falcon will be enabled by calling updateSizeLimit().
        this.cacheSizeLimit = 0;
        this.cache = new LinkedHashMap<>(100, 0.75F, true);
        this.valueState = dbAccessor;
        // statistical item to calculate cache hit ratio
        this.accessCnt = 0;
        this.hitCnt = 0;
    }

    @Override
    public V get(K key, N namespace) {
        accessCnt += 1;
        FalconKey<K, N> falconKey = new FalconKey<>(key, namespace);
        if (cache.containsKey(falconKey)) { // falcon cache hit
            hitCnt += 1;
            FalconValue<V> falconValue = cache.get(falconKey);
            return falconValue.getValue();
        } else { // falcon cache miss, get from rocksdb and insert into falcon cache
            V value = valueState.getValue();
            if (value != null) {
                if (!value.equals(valueState.defaultValue())) { // rocksdb hit, put value into falcon cache
                    FalconValue<V> falconValue = new FalconValue<>(value, false);
                    cache.put(falconKey, falconValue);
                    if (cache.size() > cacheSizeLimit) {
                        removeEldestState();
                    }
                }
            }
            return value;
        }
    }

    @Override
    public void put(K key, N namespace, V value) {
        accessCnt += 1;
        FalconKey<K, N> falconKey = new FalconKey<>(key, namespace);
        FalconValue<V> falconValue = new FalconValue<>(value, false);
        if (cache.containsKey(falconKey)) { // falcon cache hit
            hitCnt += 1;
            falconValue.markAsDirty();
            cache.put(falconKey, falconValue);
        } else { // falcon cache miss, put to rocksdb and insert into falcon cache
            valueState.writeValue(value);
            cache.put(falconKey, falconValue);
            if (cache.size() > cacheSizeLimit) {
                removeEldestState();
            }
        }
    }

    public void remove(K key, N namespace) {
        accessCnt += 1;
        FalconKey<K, N> falconKey = new FalconKey<>(key, namespace);
        if (cache.containsKey(falconKey)) { // falcon cache hit
            hitCnt += 1;
            cache.remove(falconKey);
        }
        // the state is going to be deleted from rocksdb, thus we do not need to flush it from falcon cache to rocksdb
        valueState.deleteValue();
    }

    @Override
    public void flush() {
        K currentKey = valueState.getCurrentKey();
        N currentNamespace = valueState.getNamespace();
        cache.forEach(
            (falconKey, falconValue) -> {
                if (falconValue.isDirty()) {
                    valueState.setKeyAndNamespace(falconKey.getKey(), falconKey.getNamespace());
                    valueState.writeValue(falconValue.getValue());
                    falconValue.markAsClean();
                }
            }
        );
        // after flush, restore key and namespace. Note that when flush in snapshot, key and namespace may be null, thus
        // we do not restore them after flush.
        if (currentKey != null && currentNamespace != null) {
            valueState.setKeyAndNamespace(currentKey, currentNamespace);
        }
    }

    @Override
    public void clearAll() {
        cache.clear();
    }

    @Override
    public void removeEldestState() {
        K currentKey = valueState.getCurrentKey();
        N currentNamespace = valueState.getNamespace();
        if (cache.size() >= cacheSizeLimit * 1.2) { // defensive programming, clear cache
            flush();
            cache.clear();
            // reset accessCnt and hitCnt to 0 after cache clearing
            accessCnt = 0;
            hitCnt = 0;
        } else { // remove coldest state
            Map.Entry<FalconKey<K, N>, FalconValue<V>> entry = cache.entrySet().iterator().next();
            FalconKey<K, N> falconKey = entry.getKey();
            FalconValue<V> falconValue = entry.getValue();
            // note that when value is null, it will not be put in falcon cache, thus do not need to check here
            if (falconValue.isDirty()) {
                valueState.setKeyAndNamespace(falconKey.getKey(), falconKey.getNamespace());
                valueState.writeValue(falconValue.getValue());
            }
            cache.remove(falconKey);
        }
        valueState.setKeyAndNamespace(currentKey, currentNamespace); // after flush, restore key and namespace
    }

    @Override
    public int getCacheSizeLimit() {
        return cacheSizeLimit;
    }

    @Override
    public void updateCacheSizeLimit(int newSizeLimit) {
        this.cacheSizeLimit = newSizeLimit;
        if (cache.size() > cacheSizeLimit) {
            flush();
            cache.clear();
            // reset accessCnt and hitCnt to 0 after cache clearing
            accessCnt = 0;
            hitCnt = 0;
        }
    }

    @Override
    public boolean bypassCache() {
        // perform hit ratio check every 10w times (avoid too much calculating), then decide whether to bypass cache
        if (accessCnt % 1e5 == 0) {
            double hitRatio = (double) hitCnt / accessCnt;
            if (hitRatio < hitThr) {
                LOG.info("[FALCON] hitRatio = {}/{} = {} < {} thus bypass cache", hitCnt, accessCnt, hitRatio, hitThr);
                return true;
            } else {
                return false;
            }
        }
        return false;
    }
}
