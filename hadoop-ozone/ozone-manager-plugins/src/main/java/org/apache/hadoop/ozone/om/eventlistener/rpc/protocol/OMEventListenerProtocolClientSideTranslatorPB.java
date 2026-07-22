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
import org.apache.hadoop.ipc_.ProtobufHelper;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
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
   */
  public static class Builder {
    private final OzoneConfiguration conf;
    private InetSocketAddress address;
    private UserGroupInformation ugi;

    public Builder(OzoneConfiguration conf) {
      this.conf = conf;
    }

    public Builder address(InetSocketAddress omAddress) {
      this.address = omAddress;
      return this;
    }

    public Builder address(String omAddress) {
      this.address = NetUtils.createSocketAddr(omAddress);
      return this;
    }

    public Builder ugi(UserGroupInformation userGroupInformation) {
      this.ugi = userGroupInformation;
      return this;
    }

    public OMEventListenerProtocol build() throws IOException {
      if (address == null) {
        address = OmUtils.getOmAddress(conf);
      }

      if (ugi == null) {
        ugi = UserGroupInformation.getCurrentUser();
      }

      RPC.setProtocolEngine(conf, OMEventListenerProtocolPB.class, ProtobufRpcEngine.class);
      OMEventListenerProtocolPB proxy = RPC.getProxy(
          OMEventListenerProtocolPB.class,
          RPC.getProtocolVersion(OMEventListenerProtocolPB.class),
          address,
          ugi,
          conf,
          NetUtils.getDefaultSocketFactory(conf));
      return new OMEventListenerProtocolClientSideTranslatorPB(proxy);
    }
  }
}
