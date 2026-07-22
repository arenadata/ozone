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

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.ozone.om.eventlistener.rpc.protocol.OMEventListenerProtocol;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;

/**
 * {@link PolicyProvider} for the OM event listener RPC endpoint. When
 * {@code hadoop.security.authorization} is enabled the RPC server enforces the
 * ACL configured under {@link #EVENT_LISTENER_PROTOCOL_ACL}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class OMEventListenerRpcPolicyProvider extends PolicyProvider {

  public static final String EVENT_LISTENER_PROTOCOL_ACL =
      "ozone.om.security.eventlistener.protocol.acl";

  private static final OMEventListenerRpcPolicyProvider INSTANCE =
      new OMEventListenerRpcPolicyProvider();

  private static final Service[] SERVICES = new Service[] {
      new Service(EVENT_LISTENER_PROTOCOL_ACL, OMEventListenerProtocol.class)
  };

  private OMEventListenerRpcPolicyProvider() {
  }

  public static OMEventListenerRpcPolicyProvider getInstance() {
    return INSTANCE;
  }

  @Override
  public Service[] getServices() {
    return SERVICES.clone();
  }
}
