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
import java.security.KeyPair;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyStorage;
import org.apache.hadoop.ozone.recon.scm.ReconStorageConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.bouncycastle.asn1.x500.X500Name;
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

    SecurityConfig securityConfig = new SecurityConfig(conf);
    KeyStorage keyStorage =
        new KeyStorage(securityConfig, ReconCertificateClient.COMPONENT_NAME);
    KeyPair keyPair = new HDDSKeyGenerator(securityConfig).generateKey();
    keyStorage.storePrivateKey(keyPair.getPrivate());
    keyStorage.storePublicKey(keyPair.getPublic());

    ReconCertificateClient client = new ReconCertificateClient(
        securityConfig, null, mock(ReconStorageConfig.class),
        null, () -> { });

    X500Name subject = client.configureCSRBuilder().build()
        .generateCSR().getSubject();
    assertTrue(subject.toString().contains(
        "CN=" + UserGroupInformation.getCurrentUser().getShortUserName()
            + "@myhost"));
  }
}
