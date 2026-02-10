package com.huawei.falcon.state.cache;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.ConfigOptions;
import org.rocksdb.*;
import org.apache.flink.contrib.streaming.state.RocksDBValueState;

import static java.lang.Thread.sleep;

/**
 * Java JNI class for falcon cache of RocksDBValueState, which is bind with a {@link RocksDBValueState} instance.
 * <p> Falcon ValueState holds a falcon cache, serialized K-V states will be firstly aggregated in falcon cache. When
 * cache size reaches upper limit, falcon cache will eliminate a K-V state using LRU method, and then flush the state to
 * rocksdb. Using such falcon cache, state get and put operation are all O(1) time complexity.
 * <p> This JNI class exposes get, put and delete method, which are same as RocksDB JNI API. Besides, We also expose
 * some API for flink to set/get size limit of native falcon cache, and we expose API for flink to flush all the states
 * into rocksdb during checkpoint.
 */
public class FalconValueState implements AutoCloseable {
    private enum FalconLibState {
        NOT_LOADED,
        LOADING,
        LOADED
    }

    private static AtomicReference<FalconLibState> libraryLoaded = new AtomicReference<>(FalconLibState.NOT_LOADED);

    static {
        System.out.println("[FALCON] trying to load falcon dynamic library.");
        FalconValueState.loadFalconLibrary();
        System.out.println("[FALCON] successfully loaded falcon dynamic library.");
    }

    private long falconHandle_;          // falcon handle, which is used to construct c++ falcon cache
    private final long rocksdbHandle_;   // rocksdb handle, which can be converted to c++ RocksDB*

    public FalconValueState(RocksDB db) {
        final double cacheBypassThreshold = GlobalConfiguration.loadConfiguration().get(
            ConfigOptions.key("state.backend.rocksdb.falcon.state-cache-bypass-hitRatio")
                .doubleType()
                .defaultValue(-1.0)
                .withDescription("If falcon cache hitRatio is less than the given value, then bypass falcon cache.")
        );
        // init falcon cache (set cache bypass threshold), and get handle of falcon cache.
        this.falconHandle_ = initFalconCache(cacheBypassThreshold);
        this.rocksdbHandle_ = db.getNativeHandle();
    }

    @Override
    public void close() {
        if (falconHandle_ != 0) {
            destroyFalconCache(falconHandle_);  // initFalconCache will new, and destroyFalconCache will delete
            falconHandle_ = 0;
        }
    }

    /**
     * Load Falcon dynamic library files, calling this method twice will have no effect. This method try to load falcon
     * library from inside jar.
     */
    public static void loadFalconLibrary() {
        // If falcon lib has been loaded, just return
        if (libraryLoaded.get() == FalconLibState.LOADED) {
            return;
        }

        // If falcon lib has not been loaded, modify its status as LOADING
        if (libraryLoaded.compareAndSet(FalconLibState.NOT_LOADED, FalconLibState.LOADING)) {
            try {
                FalconLibLoader.getInstance().loadLibrary();  // try to load from inside jar
            } catch (final IOException e) {
                libraryLoaded.set(FalconLibState.NOT_LOADED);
                throw new RuntimeException("[FALCON] unable to load falcon shared library", e);
            }
            libraryLoaded.set(FalconLibState.LOADED);
            return;
        }

        // Spin waiting for falcon lib been successfully loaded
        while (libraryLoaded.get() == FalconLibState.LOADING) {
            try {
                sleep(10);
            } catch(InterruptedException e) {
                // ignore
            }
        }
    }

    /**
     * The optimized version of get using falcon cache, which returns a new byte array storing the value associated with
     * the specified input key if any. Null will be returned if the specified key is not found. In detail, value will be
     * read from falcon cache if cache hit, otherwise value will be read from rocksdb and be inserted into cache.
     * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
     * @param writeOpts {@link WriteOptions} instance, which is used when falcon cache elimination fires
     * @param key the key retrieve the value.
     * @return a byte array storing the value associated with the input key if a byte array storing the value associated
     * with the input key if the specified key is not found.
     * @throws FalconException thrown if error happens in underlying native library.
     */
    public byte[] get(final ColumnFamilyHandle columnFamilyHandle, final WriteOptions writeOpts, final byte[] key)
            throws FalconException {
        return get(falconHandle_, rocksdbHandle_, columnFamilyHandle.getNativeHandle(), writeOpts.getNativeHandle(),
            key, 0, key.length);
    }

    /**
     * Put the database entry for "key" to "value" of the specified column family into falcon cache and rocksdb. In
     * detail, entry will be put into falcon cache first. When cache size reaches upper limit, then cache elimination
     * fires, the state will be flushed into rocksdb.
     * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
     * @param writeOpts {@link WriteOptions} instance.
     * @param key the specified key to be inserted.
     * @param value the value associated with the specified key.
     *
     * @throws FalconException thrown if error happens in underlying native library.
     */
    public void put(final ColumnFamilyHandle columnFamilyHandle, final WriteOptions writeOpts, final byte[] key,
                    final byte[] value) throws FalconException {
        put(falconHandle_, rocksdbHandle_, columnFamilyHandle.getNativeHandle(), writeOpts.getNativeHandle(),
            key, 0, key.length, value, 0, value.length);
    }

    /**
     * Delete the database entry (if any) for "key" from falcon cache and rocksdb. Returns OK on success, and a non-OK
     * status on error. It is not an error if "key" did not exist in the database.
     * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
     * @param writeOpt WriteOptions to be used with delete operation
     * @param key Key to delete within database
     *
     * @throws FalconException thrown if error happens in underlying native library.
     */
    public void delete(final ColumnFamilyHandle columnFamilyHandle, final WriteOptions writeOpt, final byte[] key)
            throws FalconException {
        delete(falconHandle_, rocksdbHandle_, columnFamilyHandle.getNativeHandle(), writeOpt.getNativeHandle(),
               key, 0, key.length);
    }

    /**
     * Check whether falcon cache is open, cacheSizeLimit > 0 means falcon cache is enabled.
     * @return true means falcon cache is enabled, false means falcon cache is disabled
     * @throws FalconException thrown if error happens in underlying native library.
     */
    public boolean isFalconCacheOpen() throws FalconException {
        int cacheSizeLimit = getCacheSizeLimit(falconHandle_);
        return cacheSizeLimit > 0;
    }

    /**
     * Update falcon cache size limit. If cache size reaches new size limit, flush all the state into rocksdb and clear
     * the cache.
     * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
     * @param writeOpt WriteOptions to be used with delete operation
     * @throws FalconException thrown if error happens in underlying native library.
     */
    public void updateCacheSizeLimit(final ColumnFamilyHandle columnFamilyHandle, final WriteOptions writeOpt,
                                     final int newSizeLimit) throws FalconException {
        setCacheSizeLimit(falconHandle_, rocksdbHandle_, columnFamilyHandle.getNativeHandle(),
            writeOpt.getNativeHandle(), newSizeLimit);
    }

    /**
     * Flush all the state into rocksdb when performing checkpoint or savepoint
     * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
     * @param writeOpt WriteOptions to be used with delete operation
     * @throws FalconException thrown if error happens in underlying native library.
     */
    public void flushWhenCheckpoint(final ColumnFamilyHandle columnFamilyHandle, final WriteOptions writeOpt)
            throws FalconException {
        flush(falconHandle_, rocksdbHandle_, columnFamilyHandle.getNativeHandle(), writeOpt.getNativeHandle());
    }

    private native long initFalconCache(final double cacheBypassThreshold);
    private native void destroyFalconCache(final long falconHandle);
    private native byte[] get(final long falconHandle, final long rocksdbHandle, final long cfHandle,
                              final long writeOptHandle, final byte[] key, final int keyOffset, final int keyLength)
            throws FalconException;
    private native void put(final long falconHandle, final long rocksdbHandle, final long cfHandle,
                            final long writeOptHandle, final byte[] key, final int keyOffset, final int keyLength,
                            final byte[] value, final int valueOffset, final int valueLength) throws FalconException;
    private native void delete(final long falconHandle, final long rocksdbHandle, final long cfHandle,
                               final long writeOptHandle, final byte[] key, final int keyOffset,
                               final int keyLength) throws FalconException;
    private native int getCacheSizeLimit(final long falconHandle) throws FalconException;
    private native void setCacheSizeLimit(final long falconHandle, final long rocksdbHandle, final long cfHandle,
                                          final long writeOptHandle, final int newSizeLimit) throws FalconException;
    private native void flush(final long falconHandle, final long rocksdbHandle, final long cfHandle,
                              final long writeOptHandle) throws FalconException;
}