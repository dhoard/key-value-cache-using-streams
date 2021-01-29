package com.github.dhoard.kafka.streams;

import com.github.dhoard.kafka.streams.impl.SampleProcessSupplier;
import com.github.dhoard.kafka.streams.impl.SampleProcessor;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
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
        String topicName = "test";

        Processor processor = new SampleProcessor();
        ProcessorSupplier processorSupplier = new SampleProcessSupplier(processor);

        StoreBuilder storeBuilder = Stores.timestampedWindowStoreBuilder(
            Stores.persistentTimestampedWindowStore(
                "key-value-window-store",
                Duration.of(14, ChronoUnit.DAYS),
                Duration.of(14, ChronoUnit.DAYS),
                true),
            Serdes.String(),
            Serdes.String());

        Topology topology = new Topology();
        topology.addSource("sourceTopic", topicName)
            .addProcessor("sample-processor", processorSupplier, "sourceTopic")
            .addStateStore(storeBuilder,"sample-processor");

        StreamsBean.Builder streamsBeanBuilder = StreamsBean.Builder.newBuilder();
        streamsBeanBuilder.setBootstrapServers(bootstrapServers);
        streamsBeanBuilder.setApplicationId(applicationId);
        streamsBeanBuilder.setKeySerde(keySerde);
        streamsBeanBuilder.setValueSerde(valueSerde);
        streamsBeanBuilder.setAutoOffsetResetConfig(autoOffsetResetConfig);
        streamsBeanBuilder.setTopicName(topicName);
        streamsBeanBuilder.setProcessor(processor);
        streamsBeanBuilder.setTopology(topology);

        StreamsBean streamsBean = streamsBeanBuilder.build();

        LOGGER.info("StreamsBean staring...");

        streamsBean.start();

        LOGGER.info("StreamsBean started");

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
    }
}
