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

package org.apache.hadoop.ozone.recon.security;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.ozone.recon.scm.ReconStorageConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link ReconCertificateClient}.
 */
public class TestReconCertificateClient {

  @Test
  public void testConfigureCSRBuilderUsesConfiguredHostname(
      @TempDir File metaDir) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ReconConfigKeys.OZONE_RECON_HOSTNAME_KEY, "myhost");
    conf.set(HddsConfigKeys.HDDS_METADATA_DIR_NAME, metaDir.getPath());

    ReconCertificateClient client = new ReconCertificateClient(
        new SecurityConfig(conf), null, mock(ReconStorageConfig.class),
        null, () -> { });

    LogCapturer reconLogs = LogCapturer.captureLogs(ReconCertificateClient.class);
    try {
      client.configureCSRBuilder();
      assertTrue(reconLogs.getOutput().contains(
          UserGroupInformation.getCurrentUser().getShortUserName() + "@myhost"));
    } finally {
      reconLogs.stopCapturing();
    }
  }
}
