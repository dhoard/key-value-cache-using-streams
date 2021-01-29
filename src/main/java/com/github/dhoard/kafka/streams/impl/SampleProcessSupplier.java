package com.github.dhoard.kafka.streams.impl;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class SampleProcessSupplier implements ProcessorSupplier {

    private Processor processor;

    public SampleProcessSupplier(Processor processor) {
        this.processor = processor;
    }

    @Override
    public Processor get() {
        return this.processor;
    }
}
