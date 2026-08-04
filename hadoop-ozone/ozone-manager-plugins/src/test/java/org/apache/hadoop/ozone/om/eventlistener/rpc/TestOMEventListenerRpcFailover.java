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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.eventlistener.OMEventListenerPluginContext;
import org.apache.hadoop.ozone.om.eventlistener.rpc.protocol.OMEventListenerProtocol;
import org.apache.hadoop.ozone.om.eventlistener.rpc.protocol.OMEventListenerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo.OperationArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the HA-aware event listener RPC client.
 */
public class TestOMEventListenerRpcFailover {

  private final List<OMEventListenerRpcServer> servers = new ArrayList<>();
  private OMEventListenerProtocol client;

  @AfterEach
  public void tearDown() throws IOException {
    if (client != null) {
      client.close();
    }
    for (OMEventListenerRpcServer server : servers) {
      server.stop();
    }
  }

  @Test
  public void testFailsOverFromFollowerToLeader() throws IOException {
    // First OM in the list is a follower (rejects), second is the ready leader.
    OMEventListenerRpcServer follower = startServer(mockFollower());

    List<OmCompletedRequestInfo> ledger = Arrays.asList(
        completed(5L, Type.CreateBucket),
        completed(6L, Type.CreateKey));
    OMEventListenerRpcServer leader = startServer(mockLeaderReturning(ledger));

    client = clientOver(
        follower.getListenerAddress(),
        leader.getListenerAddress());

    List<OmCompletedRequestInfo> result =
        client.listCompletedRequestInfo(null, 100);

    assertThat(result).hasSize(2);
    assertThat(result.get(0).getTrxLogIndex()).isEqualTo(5L);
    assertThat(result.get(1).getTrxLogIndex()).isEqualTo(6L);
  }

  @Test
  public void testFailsOverPastUnreachableOM() throws IOException {
    // First endpoint is a dead address (nothing listening), second is leader.
    InetSocketAddress deadAddress =
        new InetSocketAddress("127.0.0.1", findFreePort());

    OMEventListenerRpcServer leader =
        startServer(mockLeaderReturning(Collections.emptyList()));

    client = clientOver(deadAddress, leader.getListenerAddress());

    List<OmCompletedRequestInfo> result =
        client.listCompletedRequestInfo(null, 100);

    assertThat(result).isEmpty();
  }

  @Test
  public void testDefinitiveErrorIsNotFailedOver() throws IOException {
    OMEventListenerPluginContext leaderCtx = mockLeader();
    when(leaderCtx.listCompletedRequestInfo(any(), anyInt()))
        .thenThrow(new IOException("Missing rows - start key not found"));
    OMEventListenerRpcServer leader = startServer(leaderCtx);

    client = clientOver(leader.getListenerAddress());

    assertThatThrownBy(() -> client.listCompletedRequestInfo(3L, 10))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Missing rows");
  }

  private OMEventListenerRpcServer startServer(
      OMEventListenerPluginContext context) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_BIND_HOST_KEY,
        "127.0.0.1");
    conf.setInt(OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_PORT_KEY, 0);

    OMEventListenerRpcServer server = new OMEventListenerRpcServer();
    server.initialize(conf, context);
    server.start();
    servers.add(server);
    return server;
  }

  private OMEventListenerProtocol clientOver(InetSocketAddress... addresses)
      throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    // Fail fast on connection refused rather than retrying inside the IPC layer.
    conf.setInt("ipc.client.connect.max.retries", 0);
    conf.setInt("ipc.client.connect.max.retries.on.timeouts", 0);
    OMEventListenerProtocolClientSideTranslatorPB.Builder builder =
        OMEventListenerProtocolClientSideTranslatorPB.builder(conf)
            .ugi(UserGroupInformation.createRemoteUser("testuser"));
    // Endpoints are tried in the order they are added; the first is tried first.
    for (InetSocketAddress address : addresses) {
      builder.address(address);
    }
    return builder.build();
  }

  private static OMEventListenerPluginContext mockFollower() {
    OMEventListenerPluginContext ctx =
        org.mockito.Mockito.mock(OMEventListenerPluginContext.class);
    when(ctx.isLeaderReady()).thenReturn(false);
    return ctx;
  }

  private static OMEventListenerPluginContext mockLeader() {
    OMEventListenerPluginContext ctx =
        org.mockito.Mockito.mock(OMEventListenerPluginContext.class);
    lenient().when(ctx.isLeaderReady()).thenReturn(true);
    return ctx;
  }

  private static OMEventListenerPluginContext mockLeaderReturning(
      List<OmCompletedRequestInfo> ledger) throws IOException {
    OMEventListenerPluginContext ctx = mockLeader();
    when(ctx.listCompletedRequestInfo(any(), anyInt())).thenReturn(ledger);
    return ctx;
  }

  private static int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static OmCompletedRequestInfo completed(long trxLogIndex,
      Type cmdType) {
    return OmCompletedRequestInfo.newBuilder()
        .setTrxLogIndex(trxLogIndex)
        .setCmdType(cmdType)
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setKeyName("key")
        .setCreationTime(System.currentTimeMillis())
        .setOpArgs(new OperationArgs.NoArgs())
        .build();
  }
}
