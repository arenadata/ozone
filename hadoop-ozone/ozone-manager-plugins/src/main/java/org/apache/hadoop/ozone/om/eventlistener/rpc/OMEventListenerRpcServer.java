/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.eventlistener.rpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.om.eventlistener.OMEventListener;
import org.apache.hadoop.ozone.om.eventlistener.OMEventListenerPluginContext;
import org.apache.hadoop.ozone.om.eventlistener.protocol.proto.OMEventListenerProtocolProtos.OMEventListenerService;
import org.apache.hadoop.ozone.om.eventlistener.rpc.protocol.OMEventListenerProtocolPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link OMEventListener} implementation which does not push events to any
 * sink. Instead, it exposes a Hadoop RPC (ProtobufRpcEngine) endpoint that
 * exposes the OM "completed request" ledger via {@code listCompletedRequestInfo},
 * so that external, poll-style consumers can pull the feed at their own pace.
 *
 * <p>Because it uses Hadoop RPC (the same transport as the OM client protocol)
 * it inherits Hadoop's security and HA machinery: SASL/Kerberos authentication,
 * {@code UserGroupInformation} propagation, service ACLs (via
 * {@code hadoop.security.authorization}) and client-side retry/failover proxies.
 */
public class OMEventListenerRpcServer implements OMEventListener {
  public static final Logger LOG =
      LoggerFactory.getLogger(OMEventListenerRpcServer.class);

  private static final String CONFIG_PREFIX = "ozone.om.plugin.eventlistener.rpc.";
  static final String BIND_HOST_CONFIG = CONFIG_PREFIX + "bind-host";
  static final String DEFAULT_BIND_HOST = "0.0.0.0";

  static final String PORT_CONFIG = CONFIG_PREFIX + "port";
  static final int DEFAULT_PORT = 9891;

  static final String HANDLER_COUNT_CONFIG = CONFIG_PREFIX + "handler.count";
  static final int DEFAULT_HANDLER_COUNT = 10;

  static final String READ_THREADS_CONFIG = CONFIG_PREFIX + "thread.count";
  static final int DEFAULT_READ_THREADS = 3;

  static final String MAX_EVENTS_LIMIT_CONFIG = CONFIG_PREFIX + "events.max";
  static final int DEFAULT_MAX_EVENTS_LIMIT = 10_000;

  private OMEventListenerPluginContext pluginContext;
  private OzoneConfiguration conf;
  private String bindHost;
  private int port;
  private int handlerCount;
  private int readThreads;
  private int maxResultsLimit;

  private volatile RPC.Server server;

  @Override
  public void initialize(OzoneConfiguration configuration,
      OMEventListenerPluginContext context) {
    this.conf = configuration;
    this.pluginContext = context;
    this.bindHost = configuration.get(BIND_HOST_CONFIG, DEFAULT_BIND_HOST);
    this.port = configuration.getInt(PORT_CONFIG, DEFAULT_PORT);
    this.handlerCount = configuration.getInt(HANDLER_COUNT_CONFIG, DEFAULT_HANDLER_COUNT);
    this.readThreads = configuration.getInt(READ_THREADS_CONFIG, DEFAULT_READ_THREADS);
    this.maxResultsLimit =
        configuration.getInt(MAX_EVENTS_LIMIT_CONFIG, DEFAULT_MAX_EVENTS_LIMIT);
    LOG.info("Initialized OMEventListenerRpcServer with bindHost={}, port={}, handlerCount={}, "
            + "readThreads={}, maxResultsLimit={}",
        bindHost, port, handlerCount, readThreads, maxResultsLimit);
  }

  @Override
  public void start() {
    if (server != null) {
      LOG.warn("OMEventListenerRpcServer already started, ignoring start()");
      return;
    }

    RPC.setProtocolEngine(conf, OMEventListenerProtocolPB.class, ProtobufRpcEngine.class);

    OMEventListenerProtocolServerSideTranslatorPB translator =
        new OMEventListenerProtocolServerSideTranslatorPB(pluginContext, maxResultsLimit);
    BlockingService service =
        OMEventListenerService.newReflectiveBlockingService(translator);

    InetSocketAddress addr = new InetSocketAddress(bindHost, port);
    try {
      RPC.Server rpcServer = new RPC.Builder(conf)
          .setProtocol(OMEventListenerProtocolPB.class)
          .setInstance(service)
          .setBindAddress(addr.getHostString())
          .setPort(addr.getPort())
          .setNumHandlers(handlerCount)
          .setNumReaders(readThreads)
          .setVerbose(false)
          // delegation-token auth would require the OM secret manager to
          // be surfaced through OMEventListenerPluginContext.
          .setSecretManager(null)
          .build();

      // Enforce service ACLs when Hadoop authorization is enabled. This is the
      // same mechanism the OM RPC server uses.
      if (conf.getBoolean(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
        rpcServer.refreshServiceAcl(conf, OMEventListenerRpcPolicyProvider.getInstance());
      }

      rpcServer.start();
      this.server = rpcServer;

      InetSocketAddress listenAddr = Optional.ofNullable(rpcServer.getListenerAddress())
          .orElse(addr);
      LOG.info("OMEventListenerRpcServer listening on {}", listenAddr);
    } catch (IOException ex) {
      LOG.error("Failed to start OMEventListenerRpcServer on {}:{}", bindHost, port, ex);
      server = null;
    }
  }

  @Override
  public void stop() {
    if (server == null) {
      return;
    }
    LOG.info("Shutting down OMEventListenerRpcServer");
    server.stop();
    server = null;
  }

  /**
   * The bound address (after start, reflects the actual port when configured
   * with port 0).
   */
  @VisibleForTesting
  public InetSocketAddress getListenerAddress() {
    if (server != null) {
      return server.getListenerAddress();
    }
    return NetUtils.createSocketAddr(bindHost, port);
  }
}
