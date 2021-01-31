package com.github.dhoard.kafka.streams;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class SampleProcessSupplier implements ProcessorSupplier {

    public SampleProcessSupplier() {
        // DO NOTHING
    }

    @Override
    public Processor get() {
        return new SampleProcessor();
    }
}
