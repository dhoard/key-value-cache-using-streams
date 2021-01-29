package com.github.dhoard.kafka.streams.impl;

import com.github.dhoard.kafka.streams.HttpServerBean;
import com.github.dhoard.util.ThrowableUtil;
import javax.servlet.Servlet;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpServerBeanImpl implements HttpServerBean {

    private final static Logger LOGGER = LoggerFactory.getLogger(HttpServerBeanImpl.class);

    private int port;

    private Servlet servlet;

    private Server server;

    private ServerConnector serverConnector;

    public HttpServerBeanImpl(int port, Servlet servlet) {
        this.port = port;
        this.servlet = servlet;
    }

    public void start() {
        LOGGER.info("start()");

        this.server = new Server();
        this.serverConnector = new ServerConnector(server);
        this.serverConnector.setPort(this.port);

        this.server.setConnectors(new Connector[] { this.serverConnector });

        ServletHolder servletHolder = new ServletHolder(this.servlet);

        ServletContextHandler servletContextHandler = new ServletContextHandler();
        servletContextHandler.addServlet(servletHolder, "/v1/api");

        HandlerList handlerList = new HandlerList();
        handlerList.addHandler(servletContextHandler);

        this.server.setHandler(handlerList);

        try {
            this.server.start();
        } catch (Throwable t) {
            ThrowableUtil.throwUnchecked(t);
        }
    }

    public void join() {
        try {
            this.server.join();
        } catch (Throwable t) {
            ThrowableUtil.throwUnchecked(t);
        }
    }
}
