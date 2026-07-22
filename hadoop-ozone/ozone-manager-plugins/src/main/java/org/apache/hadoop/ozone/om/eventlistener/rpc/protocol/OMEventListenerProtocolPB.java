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

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.ipc_.ProtocolInfo;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.eventlistener.protocol.proto.OMEventListenerProtocolProtos.OMEventListenerService;
import org.apache.hadoop.security.KerberosInfo;

/**
 * Protocol type used for Hadoop RPC (ProtobufRpcEngine) between an external
 * poller and the {@code OMEventListenerRpcServer} plugin.
 *
 * <p>The server principal is the OM's Kerberos principal because the plugin
 * runs inside, and authenticates as, the OM process.
 */
@ProtocolInfo(
    protocolName =
        "org.apache.hadoop.ozone.om.eventlistener.rpc.protocol.OMEventListenerProtocol",
    protocolVersion = 1)
@KerberosInfo(serverPrincipal = OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface OMEventListenerProtocolPB
    extends OMEventListenerService.BlockingInterface {
}
