/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.acfun.flume.plugins.maidian.source;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLServerSocket;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.acfun.flume.plugins.maidian.constant.AcfunMaidianConstants;
import org.acfun.flume.plugins.utils.NetUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.flume.source.http.JSONHandler;
import org.apache.flume.tools.HTTPServerConstraintUtil;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * A source which accepts Flume Events by HTTP POST and GET. GET should be used
 * for experimentation only. HTTP requests are converted into flume events by a
 * pluggable "handler" which must implement the {@linkplain HTTPSourceHandler}
 * interface. This handler takes a {@linkplain HttpServletRequest} and returns a
 * list of flume events.
 *
 * The source accepts the following parameters:
 * <p>
 * <tt>port</tt>: port to which the server should bind. Mandatory
 * <p>
 * <tt>handler</tt>: the class that deserializes a HttpServletRequest into a
 * list of flume events. This class must implement HTTPSourceHandler. Default:
 * {@linkplain JSONHandler}.
 * <p>
 * <tt>handler.*</tt> Any configuration to be passed to the handler.
 * <p>
 *
 * All events deserialized from one Http request are committed to the channel in
 * one transaction, thus allowing for increased efficiency on channels like the
 * file channel. If the handler throws an exception this source will return a
 * HTTP status of 400. If the channel is full, or the source is unable to append
 * events to the channel, the source will return a HTTP 503 - Temporarily
 * unavailable status.
 *
 * A JSON handler which converts JSON objects to Flume events is provided.
 *
 */
public class AcfunHttpSource extends AbstractSource implements EventDrivenSource, Configurable {
	/*
	 * There are 2 ways of doing this: a. Have a static server instance and use
	 * connectors in each source which binds to the port defined for that
	 * source. b. Each source starts its own server instance, which binds to the
	 * source's port.
	 *
	 * b is more efficient than a because Jetty does not allow binding a servlet
	 * to a connector. So each request will need to go through each each of the
	 * handlers/servlet till the correct one is found.
	 *
	 */

	private static final Logger LOG = LoggerFactory.getLogger(AcfunHttpSource.class);
	private volatile Integer port;
	private volatile Server srv;
	private volatile String host;

	private Map<String, HTTPSourceHandler> bizTypeHandlerMap;

	private SourceCounter sourceCounter;

	// SSL configuration variable
	private volatile String keyStorePath;
	private volatile String keyStorePassword;
	private volatile Boolean sslEnabled;
	private final List<String> excludedProtocols = new LinkedList<String>();

	public synchronized void configure(Context context) {
		// SSL related config
		sslEnabled = context.getBoolean(AcfunMaidianConstants.SSL_ENABLED, false);

		port = context.getInteger(AcfunMaidianConstants.CONFIG_PORT);
		host = context.getString(AcfunMaidianConstants.CONFIG_BIND,
				AcfunMaidianConstants.DEFAULT_BIND);

		Preconditions.checkState(host != null && !host.isEmpty(), "HTTPSource hostname specified is empty");
		Preconditions.checkNotNull(port, "HTTPSource requires a port number to be" + " specified");

		if (sslEnabled) {
			LOG.debug("SSL configuration enabled");
			keyStorePath = context.getString(AcfunMaidianConstants.SSL_KEYSTORE);
			Preconditions.checkArgument(keyStorePath != null && !keyStorePath.isEmpty(),
					"Keystore is required for SSL Conifguration");
			keyStorePassword = context.getString(AcfunMaidianConstants.SSL_KEYSTORE_PASSWORD);
			Preconditions.checkArgument(keyStorePassword != null,
					"Keystore password is required for SSL Configuration");
			String excludeProtocolsStr = context.getString(AcfunMaidianConstants.EXCLUDE_PROTOCOLS);
			if (excludeProtocolsStr == null) {
				excludedProtocols.add("SSLv3");
			} else {
				excludedProtocols.addAll(Arrays.asList(excludeProtocolsStr.split(" ")));
				if (!excludedProtocols.contains("SSLv3")) {
					excludedProtocols.add("SSLv3");
				}
			}
		}

		String configedHandlers = context.getString(AcfunMaidianConstants.CONFIG_BIZTYPE_HANDLER_MAP)
				.trim();

		this.constructorHandlers(configedHandlers);

		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}
	}

	private void constructorHandlers(String configedHandlers) {
		bizTypeHandlerMap = new ConcurrentHashMap<String,HTTPSourceHandler>();
		try {
			String[] handlerEntrys = configedHandlers
					.split(AcfunMaidianConstants.CONFIG_HANDLERS_MAP_SEPRATOR);
			for (String handlerEntry : handlerEntrys) {
				String biztype = StringUtils.substringBefore(handlerEntry,
						AcfunMaidianConstants.CONFIG_HANDLERS_ENTRY_SEPRATOR);
				String handlerClassName = StringUtils.substringAfter(handlerEntry,
						AcfunMaidianConstants.CONFIG_HANDLERS_ENTRY_SEPRATOR);

				@SuppressWarnings("unchecked")
				Class<? extends HTTPSourceHandler> clazz = (Class<? extends HTTPSourceHandler>) Class
						.forName(handlerClassName);

				bizTypeHandlerMap.put(biztype, clazz.getDeclaredConstructor().newInstance());
				LOG.info("create handler : "+clazz.getName()+"  for :"+biztype);
			}
		} catch (ClassNotFoundException ex) {
			LOG.error("custom handler not found", ex);
			Throwables.propagate(ex);
		} catch (ClassCastException ex) {
			LOG.error("custom handler must implements HTTPSourceHandler");
			Throwables.propagate(ex);
		} catch (Exception ex) {
			LOG.error("Error configuring AcfunHttpSource!", ex);
			Throwables.propagate(ex);
		}
	}

	@Override
	public void start() {
		Preconditions.checkState(srv == null, "Running HTTP Server found in source: " + getName()
				+ " before I started one." + "Will not attempt to start.");
		srv = new Server();

		// Connector Array
		Connector[] connectors = new Connector[1];

		if (sslEnabled) {
			SslSocketConnector sslSocketConnector = new HTTPSourceSocketConnector(excludedProtocols);
			sslSocketConnector.setKeystore(keyStorePath);
			sslSocketConnector.setKeyPassword(keyStorePassword);
			sslSocketConnector.setReuseAddress(true);
			connectors[0] = sslSocketConnector;
		} else {
			SelectChannelConnector connector = new SelectChannelConnector();
			connector.setReuseAddress(true);
			connectors[0] = connector;
		}

		connectors[0].setHost(host);
		connectors[0].setPort(port);
		srv.setConnectors(connectors);
		try {
			org.mortbay.jetty.servlet.Context root = new org.mortbay.jetty.servlet.Context(srv, "/",
					org.mortbay.jetty.servlet.Context.SESSIONS);
			root.addServlet(new ServletHolder(new FlumeHTTPServlet()), "/");
			HTTPServerConstraintUtil.enforceConstraints(root);
			srv.start();
			Preconditions.checkArgument(srv.getHandler().equals(root));
		} catch (Exception ex) {
			LOG.error("Error while starting HTTPSource. Exception follows.", ex);
			Throwables.propagate(ex);
		}
		Preconditions.checkArgument(srv.isRunning());
		sourceCounter.start();
		super.start();
	}

	@Override
	public void stop() {
		try {
			srv.stop();
			srv.join();
			srv = null;
		} catch (Exception ex) {
			LOG.error("Error while stopping HTTPSource. Exception follows.", ex);
		}
		sourceCounter.stop();
		LOG.info("Http source {} stopped. Metrics: {}", getName(), sourceCounter);
	}

	private class FlumeHTTPServlet extends HttpServlet {

		private static final long serialVersionUID = 4891924863218790344L;
		
		/**
		 * 获取http请求业务类型
		 * 
		 * @param request
		 * @return WEB,H5,APP
		 */
		private String getHttpType(HttpServletRequest request) {
			String method = request.getMethod();
			String typePath = "";
			if (method.equals(AcfunMaidianConstants.REQUEST_TYPE_GET)) {
				String servletPath = request.getServletPath();
				typePath = servletPath.substring(1, servletPath.length());
				LOG.debug("路径为："+servletPath);
				if (typePath.equals(AcfunMaidianConstants.H5)) {
					return AcfunMaidianConstants.H5;
				} else if(typePath.equals(AcfunMaidianConstants.WEB)){
					return AcfunMaidianConstants.WEB;
				}
			} else {
				return AcfunMaidianConstants.APP;
			}
			return null;
		}
		

		@Override
		public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
			response.setHeader("Content-type", "text/html;charset=UTF-8");  
			response.setHeader("Access-Control-Allow-Origin", "*");
			List<Event> events = Collections.emptyList(); // create empty list
			try {
				String httpType = getHttpType(request);
				
				if(StringUtils.isEmpty(httpType)){
					LOG.warn("路径："+httpType+"不明确，找不到对应的HTTPSourceHandler，请检查请求路径，或配置文件配置的handler是否正确");
					response.sendError(HttpServletResponse.SC_BAD_REQUEST, "路径："+httpType+"不明确，请检查请求路径 ");
					return;
				}
				
				events = bizTypeHandlerMap.get(httpType).getEvents(request);
				
			}catch (HTTPBadRequestException ex) {
				LOG.warn("Received bad request from client. ", ex);
				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Bad request from client. " + ex.getMessage());
				return;
			} catch (Exception ex) {
				LOG.error("Deserializer threw unexpected exception. "+NetUtils.getRealIp(request)+" : "+ex.getMessage(), ex);
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
						"Deserializer threw unexpected exception. " + ex.getMessage());
				return;
			}
			sourceCounter.incrementAppendBatchReceivedCount();
			sourceCounter.addToEventReceivedCount(events.size());
			try {
				getChannelProcessor().processEventBatch(events);
			} catch (ChannelException ex) {
				LOG.warn(
						"Error appending event to channel. " + "Channel might be full. Consider increasing the channel "
								+ "capacity or make sure the sinks perform faster.",
						ex);
				response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
						"Error appending event to channel. Channel might be full." + ex.getMessage());
				return;
			} catch (Exception ex) {
				LOG.warn("Unexpected error appending event to channel. ", ex);
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
						"Unexpected error while appending event to channel. " + ex.getMessage());
				return;
			}
			response.setCharacterEncoding(request.getCharacterEncoding());
			response.setStatus(HttpServletResponse.SC_OK);
			response.flushBuffer();
			sourceCounter.incrementAppendBatchAcceptedCount();
			sourceCounter.addToEventAcceptedCount(events.size());
		}

		@Override
		public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
			doPost(request, response);
		}
	}

	private static class HTTPSourceSocketConnector extends SslSocketConnector {

		private final List<String> excludedProtocols;

		HTTPSourceSocketConnector(List<String> excludedProtocols) {
			this.excludedProtocols = excludedProtocols;
		}

		@Override
		public ServerSocket newServerSocket(String host, int port, int backlog) throws IOException {
			SSLServerSocket socket = (SSLServerSocket) super.newServerSocket(host, port, backlog);
			String[] protocols = socket.getEnabledProtocols();
			List<String> newProtocols = new ArrayList<String>(protocols.length);
			for (String protocol : protocols) {
				if (!excludedProtocols.contains(protocol)) {
					newProtocols.add(protocol);
				}
			}
			socket.setEnabledProtocols(newProtocols.toArray(new String[newProtocols.size()]));
			return socket;
		}
	}
}
