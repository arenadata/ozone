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
import org.apache.hadoop.ozone.om.OMConfigKeys;
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
    this.bindHost = configuration.get(
        OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_BIND_HOST_KEY,
        OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_BIND_HOST_DEFAULT);
    this.port = configuration.getInt(
        OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_PORT_KEY,
        OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_PORT_DEFAULT);
    this.handlerCount = configuration.getInt(
        OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_HANDLER_COUNT_KEY,
        OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_HANDLER_COUNT_DEFAULT);
    this.readThreads = configuration.getInt(
        OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_READ_THREADS_KEY,
        OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_READ_THREADS_DEFAULT);
    this.maxResultsLimit = configuration.getInt(
        OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_MAX_EVENTS_KEY,
        OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_MAX_EVENTS_DEFAULT);
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
