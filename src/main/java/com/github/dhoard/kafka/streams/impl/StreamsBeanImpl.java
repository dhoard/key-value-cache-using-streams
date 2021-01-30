package com.github.dhoard.kafka.streams.impl;

import com.github.dhoard.kafka.streams.StreamsBean;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamsBeanImpl implements StreamsBean {

    private final static Logger LOGGER = LoggerFactory.getLogger(StreamsBeanImpl.class);

    private Properties properties;

    private Processor processor;

    private Topology topology;

    private KafkaStreams kafkaStreams;

    public StreamsBeanImpl(
        String bootstrapServers,
        String applicationId,
        Class keySerde,
        Class valueSerde,
        String autoOffsetResetConfig,
        String topicName,
        Processor processor,
        Topology topology) {

        LOGGER.info("bootstrapServers      = [" + bootstrapServers + "]");
        LOGGER.info("autoOffsetResetConfig = [" + autoOffsetResetConfig + "]");
        LOGGER.info("applicationId         = [" + applicationId + "]");
        LOGGER.info("defaultKeySerde       = [" + keySerde + "]");
        LOGGER.info("defaultValueSerde     = [" + valueSerde + "]");
        LOGGER.info("topicName             = [" + topicName + "]");

        this.properties = new Properties();
        this.properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        this.properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde);
        this.properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde);
        this.properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);
        this.properties.put(
            "default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);
        this.properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
            WallclockTimestampExtractor.class);
        this.properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 8);
        this.properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 1);
        this.properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        this.properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        this.processor = processor;
        this.topology = topology;
    }

    public void start() {
        this.kafkaStreams = new KafkaStreams(this.topology, this.properties);
        this.kafkaStreams.start();

        while (true) {
            State state = this.kafkaStreams.state();

            if ("RUNNING".equals(state.toString())) {
                break;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                // DO NOTHING
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(this.kafkaStreams::close));
    }

    public Processor getProcessor() {
        return this.processor;
    }
}
