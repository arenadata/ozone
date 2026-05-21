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

package org.apache.hadoop.ozone.s3.client;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension of {@link S3OzoneClientBuilder} that produces an
 * {@link OzoneClient} backed by a {@link CachingS3OzoneRpcClient}.
 */
public class CachingS3OzoneClientBuilder extends S3OzoneClientBuilder {
  private static final Logger LOG =
      LoggerFactory.getLogger(CachingS3OzoneClientBuilder.class);

  @Override
  protected OzoneClient getRpcClient(OzoneConfiguration ozoneConfiguration) throws IOException {
    return OzoneClientFactory.getRpcClient(
        ozoneConfiguration, CachingS3OzoneClientBuilder::getCachingRpcClient);
  }

  @Override
  protected OzoneClient getRpcClient(String omServiceId, OzoneConfiguration ozoneConfiguration) throws IOException {
    return OzoneClientFactory.getRpcClient(
        omServiceId, ozoneConfiguration, CachingS3OzoneClientBuilder::getCachingRpcClient);
  }

  private static ClientProtocol getCachingRpcClient(ConfigurationSource config,
                                                    String omServiceId) throws IOException {
    try {
      return new CachingS3OzoneRpcClient(config, omServiceId);
    } catch (Exception e) {
      final String message = "Couldn't create CachingS3OzoneRpcClient protocol";
      LOG.error(message, e);
      if (e instanceof RemoteException) {
        throw ((RemoteException) e).unwrapRemoteException();
      } else if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(message, e);
      }
    }
  }
}
