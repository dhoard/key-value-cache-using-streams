package com.github.dhoard.kafka.streams;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleProcessor implements Processor<String, String> {

    private final static Logger LOGGER = LoggerFactory.getLogger(SampleProcessor.class);

    private ProcessorContext processorContext;

    private TimestampedWindowStore timestampedWindowStore;

    public final static String STATE_STORE_NAME = "key-value-window-store";

    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;

        this.timestampedWindowStore =
            (TimestampedWindowStore) this.processorContext.getStateStore(STATE_STORE_NAME);

        /*
        this.processorContext.schedule(Duration.ofDays(1), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            KeyValueIterator<String, ValueAndTimestamp> keyValueIterator =
                this.timestampedWindowStore.fetchAll(
                    Instant.now().minus(8, ChronoUnit.DAYS),
                    Instant.now().minus(15, ChronoUnit.DAYS));

                while (keyValueIterator.hasNext()) {
                    KeyValue<String, ValueAndTimestamp> entry = keyValueIterator.next();
                    Optional<KeyValue<String, ValueAndTimestamp>> message = Optional.of(entry);

                    message.filter(
                        e -> Instant.ofEpochMilli(e.value.timestamp()).isBefore(
                            Instant.now().minus(7, ChronoUnit.DAYS)))
                        .ifPresent(e -> {
                            this.processorContext.forward(e.key, e.value.value());
                            this.timestampedWindowStore.put(e.key, null, System.currentTimeMillis());
                        });
                }

                keyValueIterator.close();
        });
        */
    }

    @Override
    public void process(String key, String value) {
        long timeMilliseconds = System.currentTimeMillis();

        /*
        WindowStoreIterator<ValueAndTimestamp<String>> windowStoreIterator =
            this.timestampedWindowStore.backwardFetch(
                key, Instant.now().minus(8, ChronoUnit.DAYS), Instant.now());

        if (windowStoreIterator.hasNext()) {
            this.timestampedWindowStore.put(key, null, timeMilliseconds);
        }
        */

        this.timestampedWindowStore.put(key, ValueAndTimestamp.make(value, timeMilliseconds), timeMilliseconds);
    }

    @Override
    public void close() {
        // DO NOTHING
    }
}
