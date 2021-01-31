package com.github.dhoard.kafka.streams;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.Options;

import java.util.Map;
import org.rocksdb.TableFormatConfig;

public class CustomRocksDBConfig implements RocksDBConfigSetter {

    public static long blockCacheSize = 50 * 1024 * 1024L;

    public static long blockSize = 32 * 1024L;

    public static boolean cacheIndexAndFilterBlock = true;

    private org.rocksdb.Cache cache = new org.rocksdb.LRUCache(blockCacheSize);

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        /*
        TableFormatConfig tableConfig = options.tableFormatConfig();

        ((BlockBasedTableConfig) tableConfig).setBlockCache(cache);
        ((BlockBasedTableConfig) tableConfig).setBlockSize(blockSize);
        ((BlockBasedTableConfig) tableConfig).setCacheIndexAndFilterBlocks(cacheIndexAndFilterBlock);

        options.setTableFormatConfig(tableConfig);
        options.setMaxWriteBufferNumber(2);
        */
        options.setCompactionStyle(CompactionStyle.UNIVERSAL);
    }
}