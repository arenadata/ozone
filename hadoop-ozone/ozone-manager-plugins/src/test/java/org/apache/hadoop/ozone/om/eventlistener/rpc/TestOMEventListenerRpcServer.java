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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests {@link OMEventListenerRpcServer} end-to-end over a real Hadoop RPC
 * client proxy.
 */
@ExtendWith(MockitoExtension.class)
public class TestOMEventListenerRpcServer {

  @Mock
  private OMEventListenerPluginContext pluginContext;

  private OMEventListenerRpcServer plugin;
  private OMEventListenerProtocol client;

  @BeforeEach
  public void setUp() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_BIND_HOST_KEY, "127.0.0.1");
    conf.setInt(OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_PORT_KEY, 0);
    // These tests target a single OM and assert the immediate response, so
    // disable client-side failover retries against that one endpoint.
    conf.setInt(
        OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_CLIENT_MAX_RETRIES_KEY, 0);

    plugin = new OMEventListenerRpcServer();
    plugin.initialize(conf, pluginContext);
    plugin.start();

    InetSocketAddress addr = plugin.getListenerAddress();
    // createRemoteUser (rather than getCurrentUser) keeps this unit test off the
    // JDK Subject/SecurityManager path; the endpoint is plaintext here.
    client = OMEventListenerProtocolClientSideTranslatorPB.builder(conf)
        .address(addr)
        .ugi(UserGroupInformation.createRemoteUser("testuser"))
        .build();
  }

  @AfterEach
  public void shutDown() throws IOException {
    if (client != null) {
      client.close();
    }
    if (plugin != null) {
      plugin.stop();
    }
  }

  @Test
  public void testListReturnsLedgerEntries() throws IOException {
    when(pluginContext.isLeaderReady()).thenReturn(true);
    List<OmCompletedRequestInfo> ledger = Arrays.asList(
        completed(5L, Type.CreateBucket, "vol", "bucket", null),
        completed(6L, Type.CreateKey, "vol", "bucket", "key"));
    when(pluginContext.listCompletedRequestInfo(isNull(), eq(100))).thenReturn(ledger);

    List<OmCompletedRequestInfo> result = client.listCompletedRequestInfo(null, 100);

    assertThat(result).hasSize(2);
    assertThat(result.get(0).getTrxLogIndex()).isEqualTo(5L);
    assertThat(result.get(0).getCmdType()).isEqualTo(Type.CreateBucket);
    assertThat(result.get(1).getKeyName()).isEqualTo("key");
  }

  @Test
  public void testStartKeyIsPassedThrough() throws IOException {
    when(pluginContext.isLeaderReady()).thenReturn(true);
    when(pluginContext.listCompletedRequestInfo(eq(6L), eq(100)))
        .thenReturn(Collections.emptyList());

    List<OmCompletedRequestInfo> result = client.listCompletedRequestInfo(6L, 100);

    assertThat(result).isEmpty();
  }

  @Test
  public void testMaxResultsClampedToConfiguredLimit() throws IOException {
    when(pluginContext.isLeaderReady()).thenReturn(true);
    // request more than the default limit of 10_000
    when(pluginContext.listCompletedRequestInfo(
        isNull(), eq(OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_MAX_EVENTS_DEFAULT)))
        .thenReturn(Collections.emptyList());

    List<OmCompletedRequestInfo> result =
        client.listCompletedRequestInfo(null, 1_000_000);

    assertThat(result).isEmpty();
  }

  @Test
  public void testNotLeaderIsRejected() {
    when(pluginContext.isLeaderReady()).thenReturn(false);

    assertThatThrownBy(() -> client.listCompletedRequestInfo(null, 10))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("not the ready leader");
  }

  @Test
  public void testBackendErrorSurfacesToClient() throws IOException {
    when(pluginContext.isLeaderReady()).thenReturn(true);
    when(pluginContext.listCompletedRequestInfo(eq(3L), eq(10)))
        .thenThrow(new IOException("Missing rows - start key not found"));

    assertThatThrownBy(() -> client.listCompletedRequestInfo(3L, 10))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Missing rows");
  }

  private static OmCompletedRequestInfo completed(long trxLogIndex, Type cmdType,
      String volume, String bucket, String key) {
    return OmCompletedRequestInfo.newBuilder()
        .setTrxLogIndex(trxLogIndex)
        .setCmdType(cmdType)
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(key)
        .setCreationTime(System.currentTimeMillis())
        .setOpArgs(new OperationArgs.NoArgs())
        .build();
  }
}
