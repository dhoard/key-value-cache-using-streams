package com.github.dhoard.kafka.streams;

import com.github.dhoard.kafka.streams.StreamsBean.Builder;
import com.github.dhoard.kafka.streams.impl.HttpServerBeanImpl;
import javax.servlet.Servlet;

public interface HttpServerBean {

    public void start() throws Exception;

    public void join();

    class Builder {

        private int port;

        private Servlet servlet;

        private Builder() {
            // DO NOTHING
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setPort(int port) {
            this.port = port;

            return this;
        }

        public Builder setServlet(Servlet servlet) {
            this.servlet = servlet;

            return this;
        }

        public HttpServerBean build() {
            return new HttpServerBeanImpl(this.port, this.servlet);
        }
    }
}
