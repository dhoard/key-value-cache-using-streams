package com.github.dhoard.kafka.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class APIServlet extends HttpServlet {

    private final static Logger LOGGER = LoggerFactory.getLogger(APIServlet.class);

    public APIServlet() {
        super();
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        LOGGER.info("init()");
    }

    @Override
    protected void doGet(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse)
        throws ServletException, IOException {

        String key = httpServletRequest.getParameter("key");

        int status = HttpServletResponse.SC_NOT_FOUND;
        String message = "{ \"status\": 404, \"message\": \"NOT_FOUND\"}";

        /*
        //this.streamsBean.getKafkaStreams().queryMetadataForKey(key, new StringSerializer());
        StreamsMetadata streamsMetadata = null;
        WindowStore<String, ValueAndTimestamp<String>> windowStore = sampleProcessor.getTimestampedWindowStore();

        WindowStoreIterator<ValueAndTimestamp<String>> windowStoreIterator =
            windowStore.backwardFetch(key, Instant.now().minus(365, ChronoUnit.DAYS), Instant.now());

        if (windowStoreIterator.hasNext()) {
            KeyValue<Long, ValueAndTimestamp<String>> keyValue = windowStoreIterator.next();
            message = keyValue.value.value();
            LOGGER.info("data found [" + message + "]");
            status = HttpServletResponse.SC_OK;
        }

        windowStoreIterator.close();
         */

        setNoCache(httpServletResponse);
        httpServletResponse.setStatus(status);
        httpServletResponse.getWriter().println(message);
    }

    @Override
    public void destroy() {
        // DO NOTHING
    }

    private void setNoCache(HttpServletResponse httpServletResponse) {
        httpServletResponse.setHeader("Cache-Control", "no-cache, no-store");
        httpServletResponse.setHeader("Pragma", "no-cache");
        httpServletResponse.setDateHeader("Expires", 0);
    }
}
