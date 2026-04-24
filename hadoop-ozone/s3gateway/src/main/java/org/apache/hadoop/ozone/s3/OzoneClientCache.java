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

package org.apache.hadoop.ozone.s3;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_ENABLED;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_ENABLED_DEFAULT;

import java.io.IOException;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.s3.client.CachingS3OzoneClientBuilder;
import org.apache.hadoop.ozone.s3.client.S3OzoneClientBuilder;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cached ozone client for s3 requests.
 */
@Singleton
public class OzoneClientCache {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneClientCache.class);

  private final OzoneConfiguration conf;
  private OzoneClient client;

  @Inject
  OzoneClientCache(OzoneConfiguration conf) {
    this.conf = conf;
    LOG.debug("{}: Created", this);
  }

  @PostConstruct
  public void initialize() throws IOException {
    conf.set("ozone.om.group.rights", "NONE");
    // Set the expected OM version if not set via config.
    conf.setIfUnset(OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_KEY,
        OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_DEFAULT);
    conf.setBoolean(S3Auth.S3_AUTH_CHECK, true);

    client = createClient(conf);
    Preconditions.assertTrue(conf.getBoolean(S3Auth.S3_AUTH_CHECK, false), S3Auth.S3_AUTH_CHECK);

    LOG.debug("{}: Initialized", this);
  }

  public OzoneClient getClient() {
    return client;
  }

  public static OzoneClient createClient(OzoneConfiguration ozoneConfiguration) throws IOException {
    boolean isCachingClientEnabled = ozoneConfiguration.getBoolean(
        OZONE_S3G_OBJECT_CACHE_ENABLED,
        OZONE_S3G_OBJECT_CACHE_ENABLED_DEFAULT);

    S3OzoneClientBuilder clientBuilder = isCachingClientEnabled
        ? new CachingS3OzoneClientBuilder()
        : new S3OzoneClientBuilder();

    return clientBuilder.fromConfig(ozoneConfiguration);
  }

  @PreDestroy
  public void cleanup() {
    LOG.debug("{}: Closing cached client", this);
    IOUtils.close(LOG, client);
  }
}
