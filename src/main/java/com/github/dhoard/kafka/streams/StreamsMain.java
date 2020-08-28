package com.github.dhoard.kafka.streams;


import io.confluent.shaded.serializers.StringSerde;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamsMain {

    private final static Logger logger = LoggerFactory.getLogger(StreamsMain.class);

    private StreamsBuilder streamsBuilder;

    public static void main(String[] args) throws Throwable {
        new StreamsMain().run(args);
    }

    public void run(String[] args) throws Throwable {
        String bootstrapServers = "cp-5-4-x.address.cx:9092";
        String applicationId = "streams-main";
        Class keySerde = StringSerde.class;
        Class valueSerde = StringSerde.class;
        String autoOffsetResetConfig = "earliest";

        logger.info("bootstrapServers      = [" + bootstrapServers + "]");
        logger.info("autoOffsetResetConfig = [" + autoOffsetResetConfig + "]");
        logger.info("applicationId         = [" + applicationId + "]");
        logger.info("defaultKeySerde       = [" + keySerde + "]");
        logger.info("defaultValueSerde     = [" + valueSerde + "]");

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);
        properties.put("default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);

        String topicName = "ROCM_test_logcompaction";
        logger.info("topicName             = [" + topicName + "]");

        String tableName = "ROCM_test_keyvalue-store";
        logger.info("tableName             = [" + tableName + "]");

        this.streamsBuilder = new StreamsBuilder();

        KTable table =  this.streamsBuilder.table(
            topicName, Consumed.with(Serdes.String(), Serdes.String()), Materialized.as(tableName));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        kafkaStreams.start();

        logger.info(kafkaStreams.toString());

        while (true) {
            State state = kafkaStreams.state();

            if ("RUNNING".equals(state.toString())) {
                break;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                // DO NOTHING
            }
        }

        String queryableStoreName = table.queryableStoreName();
        ReadOnlyKeyValueStore view = kafkaStreams.store(queryableStoreName, QueryableStoreTypes.keyValueStore());
        logger.error("approximateNumEntries = [" + view.approximateNumEntries() + "]");

        while (true) {
            long t0 = System.currentTimeMillis();

            Object object = view.get("10000");

            long t1 = System.currentTimeMillis();
            long t = t1 - t0;

            logger.info("view.get time     = [" + t + "] ms");
            logger.info("object.class.name = [" + object.getClass().getName() + "]");
            logger.info("object            = [" + object + "]");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                // DO NOTHING
            }
        }

        //kafkaStreams.close();
    }
}
