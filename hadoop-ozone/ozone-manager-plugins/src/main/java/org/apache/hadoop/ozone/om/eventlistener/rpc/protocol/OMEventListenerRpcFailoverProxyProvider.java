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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link FailoverProxyProvider} for the OM event listener RPC endpoint.
 *
 * <p>The endpoint runs in-process with every OM but only the ready Ratis leader
 * serves the ledger; the followers reject requests with an
 * {@link OMEventListenerNotLeaderException}. This provider therefore builds one
 * proxy per OM (each at that OM's host and the shared event listener port) and,
 * paired with a {@link RetryPolicy} from {@link #getRetryPolicy(int)}, fails
 * over between them in round-robin order until it reaches the leader. It also
 * fails over when an OM is unreachable, so a client survives an OM restart or a
 * Ratis leader change without being manually re-pointed.
 */
public final class OMEventListenerRpcFailoverProxyProvider
    implements FailoverProxyProvider<OMEventListenerProtocolPB>, Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMEventListenerRpcFailoverProxyProvider.class);

  private final OzoneConfiguration conf;
  private final UserGroupInformation ugi;

  private final List<InetSocketAddress> addresses;
  private final List<OMEventListenerProtocolPB> proxies;

  private final long waitBetweenRetries;

  // Index of the proxy currently in use. Advanced round-robin, only ever inside
  // the synchronized #performFailover, which Hadoop's RetryInvocationHandler
  // calls once per failover generation (guarded by its own failover count).
  // All reads/writes happen under this object's monitor.
  private int currentProxyIndex;

  public OMEventListenerRpcFailoverProxyProvider(OzoneConfiguration conf,
      UserGroupInformation ugi, List<InetSocketAddress> addresses) {
    if (addresses.isEmpty()) {
      throw new IllegalArgumentException(
          "No event listener RPC addresses configured to fail over between.");
    }
    this.conf = conf;
    this.ugi = ugi;
    this.addresses = new ArrayList<>(addresses);
    this.proxies = new ArrayList<>(this.addresses.size());
    this.addresses.forEach(a -> proxies.add(null));
    this.waitBetweenRetries = conf.getLong(
        OMConfigKeys
            .OZONE_OM_PLUGIN_EVENTLISTENER_RPC_CLIENT_WAIT_BETWEEN_RETRIES_KEY,
        OMConfigKeys
            .OZONE_OM_PLUGIN_EVENTLISTENER_RPC_CLIENT_WAIT_BETWEEN_RETRIES_DEFAULT);
    this.currentProxyIndex = 0;
  }

  @Override
  public Class<OMEventListenerProtocolPB> getInterface() {
    return OMEventListenerProtocolPB.class;
  }

  @Override
  public synchronized ProxyInfo<OMEventListenerProtocolPB> getProxy() {
    int index = currentProxyIndex;
    OMEventListenerProtocolPB proxy = proxies.get(index);
    if (proxy == null) {
      InetSocketAddress address = addresses.get(index);
      try {
        proxy = createProxy(address);
      } catch (IOException e) {
        // Building a ProtobufRpcEngine proxy only wires up the client stub and
        // does not open a connection, so this fails only on a setup problem
        // (e.g. protocol-engine registration) that would affect every endpoint
        // equally - failing over to another OM would not help. Surface it to
        // the caller as an unchecked exception rather than silently retrying.
        throw new IllegalStateException(
            "Failed to create event listener RPC proxy for " + address, e);
      }
      proxies.set(index, proxy);
    }
    return new ProxyInfo<>(proxy, addresses.get(index).toString());
  }

  @Override
  public synchronized void performFailover(OMEventListenerProtocolPB current) {
    int previous = currentProxyIndex;
    currentProxyIndex = (currentProxyIndex + 1) % addresses.size();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Failing over event listener RPC client from {} to {}",
          addresses.get(previous), addresses.get(currentProxyIndex));
    }
  }

  /**
   * Build a retry policy that fails over to the next OM on a "not leader"
   * rejection or a connection failure, and surfaces every other error to the
   * caller.
   *
   * @param maxFailovers the maximum number of failovers before giving up,
   *     typically the configured per-OM retry count times the number of OMs.
   */
  public RetryPolicy getRetryPolicy(int maxFailovers) {
    return (exception, retries, failovers, isIdempotentOrAtMostOnce) -> {
      if (failovers >= maxFailovers) {
        LOG.warn("Failed to reach the event listener leader after {} failovers "
            + "over OMs {}.", failovers, addresses);
        return RetryPolicy.RetryAction.FAIL;
      }

      if (isNotLeader(exception)) {
        return failoverAction(failovers);
      }

      if (exception.getCause() instanceof RemoteException) {
        // The OM was reachable and returned a definitive error (e.g. the
        // startKey has been reclaimed, or an authorization failure). Do not
        // fail over; surface it to the caller.
        return RetryPolicy.RetryAction.FAIL;
      }

      // A transport-level failure: this OM is unreachable. Try the next one.
      return failoverAction(failovers);
    };
  }

  /**
   * Fail over to the next OM. Backs off by {@link #waitBetweenRetries} once
   * every OM has been tried once within this call, so a sustained "no ready
   * leader" condition does not spin. The cursor itself is advanced in
   * {@link #performFailover}.
   */
  private RetryPolicy.RetryAction failoverAction(int failovers) {
    long waitMs = ((failovers + 1) % getNodeCount() == 0) ? waitBetweenRetries : 0L;
    return new RetryPolicy.RetryAction(
        RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY, waitMs);
  }

  private OMEventListenerProtocolPB createProxy(InetSocketAddress address)
      throws IOException {
    RPC.setProtocolEngine(conf, OMEventListenerProtocolPB.class,
        ProtobufRpcEngine.class);
    // Do not retry on the same OM: fail fast on a network error so the outer
    // failover policy moves on to the next OM.
    RetryPolicy connectionRetryPolicy =
        RetryPolicies.failoverOnNetworkException(0);
    return RPC.getProtocolProxy(
        OMEventListenerProtocolPB.class,
        RPC.getProtocolVersion(OMEventListenerProtocolPB.class),
        address,
        ugi,
        conf,
        NetUtils.getDefaultSocketFactory(conf),
        (int) OmUtils.getOMClientRpcTimeOut(conf),
        connectionRetryPolicy).getProxy();
  }

  @Override
  public synchronized void close() {
    for (OMEventListenerProtocolPB proxy : proxies) {
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }

  public int getNodeCount() {
    return addresses.size();
  }

  private static boolean isNotLeader(Exception exception) {
    Throwable cause = exception.getCause();
    if (cause instanceof RemoteException) {
      IOException unwrapped = ((RemoteException) cause).unwrapRemoteException();
      return unwrapped instanceof OMEventListenerNotLeaderException;
    }
    return exception instanceof OMEventListenerNotLeaderException;
  }
}
