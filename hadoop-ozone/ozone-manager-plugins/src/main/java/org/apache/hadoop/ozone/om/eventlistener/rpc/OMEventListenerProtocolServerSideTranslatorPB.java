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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.ozone.om.eventlistener.OMEventListenerPluginContext;
import org.apache.hadoop.ozone.om.eventlistener.protocol.proto.OMEventListenerProtocolProtos.ListCompletedRequestInfoRequest;
import org.apache.hadoop.ozone.om.eventlistener.protocol.proto.OMEventListenerProtocolProtos.ListCompletedRequestInfoResponse;
import org.apache.hadoop.ozone.om.eventlistener.rpc.protocol.OMEventListenerNotLeaderException;
import org.apache.hadoop.ozone.om.eventlistener.rpc.protocol.OMEventListenerProtocolPB;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server-side translator that forwards Hadoop RPC requests received on
 * {@link OMEventListenerProtocolPB} to the {@link OMEventListenerPluginContext}.
 */
public class OMEventListenerProtocolServerSideTranslatorPB
    implements OMEventListenerProtocolPB {
  public static final Logger LOG =
      LoggerFactory.getLogger(OMEventListenerProtocolServerSideTranslatorPB.class);

  private final OMEventListenerPluginContext pluginContext;
  private final int maxResultsLimit;

  public OMEventListenerProtocolServerSideTranslatorPB(
      OMEventListenerPluginContext pluginContext, int maxResultsLimit) {
    this.pluginContext = pluginContext;
    this.maxResultsLimit = maxResultsLimit;
  }

  @Override
  public ListCompletedRequestInfoResponse listCompletedRequestInfo(
      RpcController controller, ListCompletedRequestInfoRequest request)
      throws ServiceException {

    // Only the leader OM has an authoritative, up-to-date ledger. Followers
    // reject the call with a typed exception so the client's failover proxy
    // provider fails over to another OM rather than reading stale data or
    // treating this as a fatal error.
    if (!pluginContext.isLeaderReady()) {
      throw new ServiceException(new OMEventListenerNotLeaderException());
    }

    // A nullable startKey maps to a proto2 optional: absent => start at oldest.
    Long startKey = request.hasStartKey() ? request.getStartKey() : null;
    int maxResults = getMaxResults(request.getMaxResults());

    try {
      List<OmCompletedRequestInfo> results =
          pluginContext.listCompletedRequestInfo(startKey, maxResults);

      ListCompletedRequestInfoResponse.Builder responseBuilder =
          ListCompletedRequestInfoResponse.newBuilder();
      for (OmCompletedRequestInfo info : results) {
        responseBuilder.addCompletedRequestInfo(info.getProtobuf());
      }
      if (!results.isEmpty()) {
        responseBuilder.setNextStartKey(
            results.get(results.size() - 1).getTrxLogIndex());
      }
      return responseBuilder.build();
    } catch (IOException ex) {
      // e.g. startKey has already been reclaimed from the ledger.
      LOG.error("listCompletedRequestInfo failed (startKey={}, maxResults={})",
          startKey, maxResults, ex);
      throw new ServiceException(ex);
    }
  }

  private int getMaxResults(int requested) {
    if (requested <= 0) {
      return maxResultsLimit;
    }
    return Math.min(requested, maxResultsLimit);
  }
}
