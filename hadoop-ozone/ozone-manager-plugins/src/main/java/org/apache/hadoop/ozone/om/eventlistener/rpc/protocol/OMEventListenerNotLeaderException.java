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

/**
 * Thrown by the event listener RPC server when the receiving OM is not the
 * ready Ratis leader. Only the leader has an authoritative, up-to-date ledger,
 * so a client that lands on a follower (or on the leader before it is ready)
 * must fail over to another OM and retry.
 *
 * <p>Hadoop RPC reconstructs remote exceptions on the client through the
 * {@link #OMEventListenerNotLeaderException(String)} constructor, so that
 * constructor must remain public.
 */
public class OMEventListenerNotLeaderException extends IOException {

  private static final long serialVersionUID = 1L;

  private static final String DEFAULT_MESSAGE =
      "OM is not the ready leader for the event listener endpoint.";

  public OMEventListenerNotLeaderException() {
    super(DEFAULT_MESSAGE);
  }

  public OMEventListenerNotLeaderException(String message) {
    super(message);
  }
}
