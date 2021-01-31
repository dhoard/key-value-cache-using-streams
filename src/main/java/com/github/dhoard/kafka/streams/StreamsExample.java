package com.github.dhoard.kafka.streams;

import io.confluent.shaded.serializers.StringSerde;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamsExample {

    private final static Logger LOGGER = LoggerFactory.getLogger(StreamsExample.class);

    private StreamsBuilder streamsBuilder;

    private KafkaStreams kafkaStreams;

    private KTable ktable;

    private ReadOnlyWindowStore readOnlyWindowStore;

    public static void main(String[] args) throws Throwable {
        String bootstrapServers = "cp-5-5-x.address.cx:9092";
        String applicationId = "streams-main";
        Class keySerde = StringSerde.class;
        Class valueSerde = StringSerde.class;
        String autoOffsetResetConfig = "earliest";
        String topicName = "test";
        String tableName = "test_table";

        new StreamsExample(
            bootstrapServers,
            applicationId,
            keySerde,
            valueSerde,
            autoOffsetResetConfig,
            topicName,
            tableName);
    }

    public StreamsExample(
        String bootstrapServers,
        String applicationId,
        Class keySerde,
        Class valueSerde,
        String autoOffsetResetConfig,
        String topicName,
        String tableName) throws Throwable {
        try {
            LOGGER.info("bootstrapServers      = [" + bootstrapServers + "]");
            LOGGER.info("autoOffsetResetConfig = [" + autoOffsetResetConfig + "]");
            LOGGER.info("applicationId         = [" + applicationId + "]");
            LOGGER.info("defaultKeySerde       = [" + keySerde + "]");
            LOGGER.info("defaultValueSerde     = [" + valueSerde + "]");
            LOGGER.info("topicName             = [" + topicName + "]");
            LOGGER.info("tableName             = [" + tableName + "]");

            Properties properties = new Properties();
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde);
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde);
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);
            properties.put(
                "default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);

            this.streamsBuilder = new StreamsBuilder();

            this.ktable = this.streamsBuilder.table(
                topicName, Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as(tableName));

            this.kafkaStreams = new KafkaStreams(this.streamsBuilder.build(), properties);

            Runtime.getRuntime().addShutdownHook(new Thread(this.kafkaStreams::close));

            this.kafkaStreams.start();

            while (true) {
                State state = this.kafkaStreams.state();

                if ("RUNNING".equals(state.toString())) {
                    break;
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    // DO NOTHING
                }
            }

            String queryableStoreName = this.ktable.queryableStoreName();

            this.readOnlyWindowStore =
                this.kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                    queryableStoreName, QueryableStoreTypes.timestampedWindowStore()));

            String key = "x";

            WindowStoreIterator windowStoreIterator =
                this.readOnlyWindowStore.backwardFetch(
                    key,
                    Instant.now(), Instant.now().minus(10, ChronoUnit.MINUTES));

            Object value = null;

            if (windowStoreIterator.hasNext()) {
                value = windowStoreIterator.next();
            }

            LOGGER.info("value = [" + value + "]");
        } finally {
            if (this.kafkaStreams != null) {
                this.kafkaStreams.close();
            }
        }
    }
}
