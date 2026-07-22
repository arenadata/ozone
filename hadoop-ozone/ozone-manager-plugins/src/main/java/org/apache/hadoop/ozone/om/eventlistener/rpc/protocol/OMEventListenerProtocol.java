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
import java.util.List;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.security.KerberosInfo;

/**
 * Clean, protobuf-free client view of the OM event listener RPC endpoint.
 */
@KerberosInfo(serverPrincipal = OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface OMEventListenerProtocol extends Closeable {

  /**
   * Page through the completed-request ledger.
   *
   * @param startKey exclusive cursor - return entries with a transaction log
   *                 index strictly greater than this. When {@code null},
   *                 iteration begins at the oldest available entry.
   * @param maxResults maximum number of entries to return.
   * @return the next page of ledger entries, oldest first.
   * @throws IOException on transport failure or when {@code startKey} has
   *                     already been reclaimed from the ledger.
   */
  List<OmCompletedRequestInfo> listCompletedRequestInfo(Long startKey, int maxResults)
      throws IOException;

  static OMEventListenerProtocol newClient(OzoneConfiguration conf) throws IOException {
    return OMEventListenerProtocolClientSideTranslatorPB.builder(conf).build();
  }
}
