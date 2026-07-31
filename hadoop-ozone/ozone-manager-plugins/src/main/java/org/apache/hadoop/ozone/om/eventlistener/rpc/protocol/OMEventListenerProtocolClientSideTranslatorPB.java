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

package org.apache.hadoop.ozone.om.eventlistener.rpc.protocol;

import com.google.protobuf.ServiceException;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc_.ProtobufHelper;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.eventlistener.protocol.proto.OMEventListenerProtocolProtos.ListCompletedRequestInfoRequest;
import org.apache.hadoop.ozone.om.eventlistener.protocol.proto.OMEventListenerProtocolProtos.ListCompletedRequestInfoResponse;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CompletedRequestInfo;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Client-side translator which turns the clean {@link OMEventListenerProtocol}
 * calls into Hadoop RPC requests against {@link OMEventListenerProtocolPB} and
 * maps the protobuf responses back into {@link OmCompletedRequestInfo}.
 */
public final class OMEventListenerProtocolClientSideTranslatorPB
    implements OMEventListenerProtocol, Closeable {

  private final OMEventListenerProtocolPB rpcProxy;

  public OMEventListenerProtocolClientSideTranslatorPB(OMEventListenerProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public List<OmCompletedRequestInfo> listCompletedRequestInfo(Long startKey, int maxResults)
      throws IOException {
    ListCompletedRequestInfoRequest.Builder requestBuilder =
        ListCompletedRequestInfoRequest.newBuilder()
            .setMaxResults(maxResults);
    if (startKey != null) {
      requestBuilder.setStartKey(startKey);
    }

    final ListCompletedRequestInfoResponse response;
    try {
      response = rpcProxy.listCompletedRequestInfo(null, requestBuilder.build());
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }

    List<OmCompletedRequestInfo> results =
        new ArrayList<>(response.getCompletedRequestInfoCount());
    for (CompletedRequestInfo proto : response.getCompletedRequestInfoList()) {
      results.add(OmCompletedRequestInfo.getFromProtobuf(proto));
    }
    return results;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  public static Builder builder(OzoneConfiguration conf) {
    return new Builder(conf);
  }

  /**
   * Builder for a proxy using Hadoop RPC. This wires up Hadoop security (SASL /
   * Kerberos when configured), so the supplied {@link UserGroupInformation}
   * ticket is used to authenticate to the OM.
   *
   * <p>The client is always HA-aware: it fails over across a list of OM
   * endpoints (round-robin) to reach the ready leader. By default, that list is
   * discovered from configuration, in configuration order. Calling
   * {@link #address(InetSocketAddress)} one or more times instead supplies the
   * endpoints explicitly, in call order; the first is tried first. A single
   * explicit address is just a one-element failover list.
   */
  public static class Builder {
    private final OzoneConfiguration conf;
    private final List<InetSocketAddress> addresses = new ArrayList<>();
    private UserGroupInformation ugi;

    public Builder(OzoneConfiguration conf) {
      this.conf = conf;
    }

    public Builder address(InetSocketAddress address) {
      addresses.add(address);
      return this;
    }

    public Builder address(String address) {
      return address(NetUtils.createSocketAddr(address));
    }

    public Builder ugi(UserGroupInformation userGroupInformation) {
      this.ugi = userGroupInformation;
      return this;
    }

    public OMEventListenerProtocol build() throws IOException {
      if (ugi == null) {
        ugi = UserGroupInformation.getCurrentUser();
      }
      RPC.setProtocolEngine(conf, OMEventListenerProtocolPB.class, ProtobufRpcEngine.class);

      List<InetSocketAddress> failoverAddresses = addresses.isEmpty()
          ? OMEventListenerRpcConfigUtils.getListenerAddresses(conf)
          : addresses;

      OMEventListenerRpcFailoverProxyProvider failoverProxyProvider =
          new OMEventListenerRpcFailoverProxyProvider(conf, ugi,
              failoverAddresses);

      int maxFailovers = conf.getInt(
          OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_CLIENT_MAX_RETRIES_KEY,
          OMConfigKeys
              .OZONE_OM_PLUGIN_EVENTLISTENER_RPC_CLIENT_MAX_RETRIES_DEFAULT)
          * failoverProxyProvider.getNodeCount();

      OMEventListenerProtocolPB proxy =
          (OMEventListenerProtocolPB) RetryProxy.create(
              OMEventListenerProtocolPB.class,
              failoverProxyProvider,
              failoverProxyProvider.getRetryPolicy(maxFailovers));
      return new OMEventListenerProtocolClientSideTranslatorPB(proxy);
    }
  }
}
