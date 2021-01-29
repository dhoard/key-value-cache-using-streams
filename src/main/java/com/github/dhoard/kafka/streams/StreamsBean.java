package com.github.dhoard.kafka.streams;

import com.github.dhoard.kafka.streams.impl.StreamsBeanImpl;
import com.github.dhoard.util.ThrowableUtil;
import io.confluent.shaded.serializers.StringSerde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;

public interface StreamsBean {

    void start();

    Processor getProcessor();

    class Builder {

        private String bootstrapServers;

        private String applicationId;

        private Class keySerde = StringSerde.class;

        private Class valueSerde = StringSerde.class;

        private String autoOffsetResetConfig = "earliest";

        private String topicName;

        private Processor processor;

        private Topology topology;

        private Builder() {
            // DO NOTHING
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;

            return this;
        }

        public Builder setApplicationId(String applicationId) {
            this.applicationId = applicationId;

            return this;
        }

        public Builder setKeySerde(Class keySerde) {
            this.keySerde = keySerde;

            return this;
        }

        public Builder setValueSerde(Class valueSerde) {
            this.valueSerde = valueSerde;

            return this;
        }

        public Builder setAutoOffsetResetConfig(String autoOffsetResetConfig) {
            this.autoOffsetResetConfig = autoOffsetResetConfig;

            return this;
        }

        public Builder setTopicName(String topicName) {
            this.topicName = topicName;

            return this;
        }

        public Builder setProcessor(Processor processor) {
            this.processor = processor;

            return this;
        }

        public Builder setTopology(Topology topology) {
            this.topology = topology;

            return this;
        }

        public StreamsBean build() {
            StreamsBean streamsBean = null;

            try {
                streamsBean = new StreamsBeanImpl(
                    this.bootstrapServers,
                    this.applicationId,
                    this.keySerde,
                    this.valueSerde,
                    this.autoOffsetResetConfig,
                    this.topicName,
                    this.processor,
                    this.topology);
            } catch (Throwable t) {
                ThrowableUtil.throwUnchecked(t);
            }

            return streamsBean;
        }
    }
}
