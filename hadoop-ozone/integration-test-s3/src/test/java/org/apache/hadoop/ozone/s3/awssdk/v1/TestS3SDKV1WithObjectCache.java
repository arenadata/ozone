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

package org.apache.hadoop.ozone.s3.awssdk.v1;

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_ENABLED;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Runs the full AWS SDK v1 S3 test suite against S3 Gateways with the
 * S3 Gateway object cache enabled (ADH-8124), verifying that caching does
 * not change the externally visible behavior of the gateway.
 */
public class TestS3SDKV1WithObjectCache extends AbstractS3SDKV1Tests {

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_S3G_OBJECT_CACHE_ENABLED, true);
    startCluster(conf);
  }

  @AfterAll
  public static void shutdown() throws IOException {
    shutdownCluster();
  }
}
