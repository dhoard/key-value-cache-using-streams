package com.github.dhoard.kafka.streams.impl;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleProcessor implements Processor<String, String> {

    private final static Logger LOGGER = LoggerFactory.getLogger(SampleProcessor.class);

    private ProcessorContext processorContext;

    private TimestampedWindowStore timestampedWindowStore;

    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
        this.timestampedWindowStore =
            (TimestampedWindowStore) this.processorContext.getStateStore("key-store");
    }

    @Override
    public void process(String key, String value) {
        LOGGER.info("process(" + key + ", " + value + ")");

        long timeMilliseconds = System.currentTimeMillis();
        this.timestampedWindowStore.put(key, ValueAndTimestamp.make(value, timeMilliseconds), timeMilliseconds);
    }

    @Override
    public void close() {
        // DO NOTHING
    }

    public TimestampedWindowStore getTimestampedWindowStore() {
        return this.timestampedWindowStore;
    }
}
