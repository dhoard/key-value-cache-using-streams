package com.github.dhoard.kafka.streams;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.Options;

import java.util.Map;
import org.rocksdb.TableFormatConfig;

public class CustomRocksDBConfig implements RocksDBConfigSetter {

    public static long BLOCK_CACHE_SIZE = 128 * 1024 * 1024L;

    public static long BLOCK_SIZE = 32 * 1024L;

    public static long WRITE_BUFFER_SIZE = 128 * 1024 * 1024;

    public static int MAX_WRITE_BUFFERS = 128 * 1024 * 1024;

    public static CompactionStyle COMPACTION_STYLE = CompactionStyle.UNIVERSAL;

    public static boolean CREATE_IF_MISSING = true;

    public static boolean ERROR_IF_EXISTS = false;

    public static boolean PREPARE_FOR_BULK_LOAD = true;

    private org.rocksdb.Cache cache = new org.rocksdb.LRUCache(BLOCK_CACHE_SIZE);

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        TableFormatConfig tableConfig = options.tableFormatConfig();

        ((BlockBasedTableConfig) tableConfig).setBlockCache(cache);
        ((BlockBasedTableConfig) tableConfig).setBlockSize(BLOCK_SIZE);

        options.setWriteBufferSize(WRITE_BUFFER_SIZE);
        options.setCompactionStyle(COMPACTION_STYLE);
        options.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
        options.setCreateIfMissing(CREATE_IF_MISSING);
        options.setErrorIfExists(ERROR_IF_EXISTS);
        options.setIncreaseParallelism(Math.max(Runtime.getRuntime().availableProcessors(), 2));

        if (PREPARE_FOR_BULK_LOAD) {
            options.prepareForBulkLoad();
        }

        options.setTableFormatConfig(tableConfig);
    }
}