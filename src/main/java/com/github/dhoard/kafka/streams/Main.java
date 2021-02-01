package com.github.dhoard.kafka.streams;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private final static Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Throwable {
        new Main().run(args);
    }

    public void run(String[] args) throws Throwable {
        LOGGER.info("Application starting");

        String bootstrapServers = "cp-5-5-x.address.cx:9092";
        String applicationId = "key-value-cache-using-streams";
        Class keySerde = Serdes.String().getClass();
        Class valueSerde = Serdes.String().getClass();
        String autoOffsetResetConfig = "earliest";
        String topicName = "test_single_partition";
        topicName = "test";
        int numStreamThreads = 10;
        long commitIntervalMS = 30000;
        long cacheMaxBytesBuffering = 16 * 1024 * 1024L;
        String stateDir = "/mnt/sdb";

        LOGGER.info("bootstrapServers       = [" + bootstrapServers + "]");
        LOGGER.info("autoOffsetResetConfig  = [" + autoOffsetResetConfig + "]");
        LOGGER.info("applicationId          = [" + applicationId + "]");
        LOGGER.info("defaultKeySerde        = [" + keySerde + "]");
        LOGGER.info("defaultValueSerde      = [" + valueSerde + "]");
        LOGGER.info("topicName              = [" + topicName + "]");
        LOGGER.info("numStreamThreads       = [" + numStreamThreads + "]");
        LOGGER.info("commitIntervalMS       = [" + commitIntervalMS + "]");
        LOGGER.info("cacheMaxBytesBuffering = [" + cacheMaxBytesBuffering + "]");
        LOGGER.info("stateDir               = [" + stateDir + "]");

        ProcessorSupplier processorSupplier = new SampleProcessSupplier();

        StoreBuilder storeBuilder = Stores.timestampedWindowStoreBuilder(
            Stores.persistentTimestampedWindowStore(
                SampleProcessor.STATE_STORE_NAME,
                Duration.of(14, ChronoUnit.DAYS),
                Duration.of(14, ChronoUnit.DAYS),
                true),
            Serdes.String(),
            Serdes.String());

        Topology topology = new Topology();
        topology.addSource("sourceTopic", topicName)
            .addProcessor("sample-processor", processorSupplier, "sourceTopic")
            .addStateStore(storeBuilder,"sample-processor");

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);
        properties.put(
                "default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);
        properties.put(
                StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                WallclockTimestampExtractor.class);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numStreamThreads);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheMaxBytesBuffering);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMS);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        properties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);

        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();

        while (true) {
            KafkaStreams.State state = kafkaStreams.state();

            if ("RUNNING".equals(state.toString())) {
                break;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                // DO NOTHING
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        /*
        APIServlet apiServlet = new APIServlet(streamsBean);

        HttpServerBean.Builder httpServerBeanBuilder = HttpServerBean.Builder.newBuilder();
        httpServerBeanBuilder.setPort(9876);
        httpServerBeanBuilder.setServlet(apiServlet);

        HttpServerBean httpServerBean = httpServerBeanBuilder.build();

        LOGGER.info("HttpServerBean starting...");

        httpServerBean.start();

        LOGGER.info("HttpServerBean started");
        LOGGER.info("Application started");

        httpServerBean.join();
        */

        while (true) {
            try {
                Thread.sleep(100);
            } catch (Throwable t) {
                // DO NOTHING
            }
        }
    }
}
