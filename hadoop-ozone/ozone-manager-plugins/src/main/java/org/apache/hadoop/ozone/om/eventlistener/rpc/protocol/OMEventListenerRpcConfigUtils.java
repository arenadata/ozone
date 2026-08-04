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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;

/**
 * Helpers shared by the OM event listener RPC server and its client. The
 * configuration keys and defaults themselves live in {@link OMConfigKeys}.
 */
public final class OMEventListenerRpcConfigUtils {

  private OMEventListenerRpcConfigUtils() {
  }

  /**
   * Resolve the address a client should connect to in order to reach the event
   * listener RPC server. The server runs in-process with the OM, so this reuses
   * the OM host but targets the event listener port
   * ({@link OMConfigKeys#OZONE_OM_PLUGIN_EVENTLISTENER_RPC_PORT_KEY}), rather
   * than the OM client RPC port that {@link OmUtils#getOmAddress} returns.
   *
   * @param conf the configuration to resolve the address from
   * @return the {@link InetSocketAddress} of the event listener RPC endpoint
   */
  public static InetSocketAddress getListenerAddress(OzoneConfiguration conf) {
    String omHost = OmUtils.getOmAddress(conf).getHostName();
    return NetUtils.createSocketAddr(omHost, getListenerPort(conf), null);
  }

  /**
   * Resolve the event listener RPC address of every OM in the service, in
   * configuration order. That order is the failover order used by the client -
   * the first configured OM is tried first. The event listener server runs
   * in-process with each OM, so each address reuses that OM's host (from
   * {@code ozone.om.address.<serviceId>.<nodeId>}) combined with the shared
   * event listener port
   * ({@link OMConfigKeys#OZONE_OM_PLUGIN_EVENTLISTENER_RPC_PORT_KEY}). The
   * endpoint is assumed to run on the same port on every OM.
   *
   * <p>For a non-HA (single OM) deployment there are no configured node ids;
   * in that case a single address is returned using
   * {@link #getListenerAddress(OzoneConfiguration)}.
   *
   * @param conf the configuration to resolve addresses from
   * @return the ordered list of event listener RPC addresses, never empty
   * @throws IOException if the OM service id cannot be resolved
   */
  public static List<InetSocketAddress> getListenerAddresses(
      OzoneConfiguration conf) throws IOException {
    int port = getListenerPort(conf);
    String omServiceId = OmUtils.getOzoneManagerServiceId(conf);

    List<InetSocketAddress> addresses = new ArrayList<>();
    Collection<String> omNodeIds =
        OmUtils.getActiveNonListenerOMNodeIds(conf, omServiceId);

    for (String nodeId : omNodeIds) {
      String omRpcAddrKey = ConfUtils.addKeySuffixes(
          OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, nodeId);
      String omRpcAddrStr = OmUtils.getOmRpcAddress(conf, omRpcAddrKey);
      if (omRpcAddrStr == null) {
        continue;
      }
      String omHost = NetUtils.createSocketAddr(omRpcAddrStr).getHostName();
      addresses.add(NetUtils.createSocketAddr(omHost, port, null));
    }

    if (addresses.isEmpty()) {
      // Non-HA, or no per-node addresses configured: fall back to the single
      // OM address.
      addresses.add(getListenerAddress(conf));
    }
    return addresses;
  }

  private static int getListenerPort(OzoneConfiguration conf) {
    return conf.getInt(
        OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_PORT_KEY,
        OMConfigKeys.OZONE_OM_PLUGIN_EVENTLISTENER_RPC_PORT_DEFAULT);
  }
}
