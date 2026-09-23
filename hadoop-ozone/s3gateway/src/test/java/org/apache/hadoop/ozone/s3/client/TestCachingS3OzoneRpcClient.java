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

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_BUCKET_ENTRY_TTL_MS;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_KEY_ENTRY_TTL_MS;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_METRICS_ENABLED;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_VOLUME_ENTRY_TTL_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

/**
 * Unit tests for the cache eviction wiring in {@link CachingS3OzoneRpcClient}.
 *
 * The OM protocol translator constructed inside the RpcClient constructor is
 * replaced with a Mockito mock, so the client talks to a mocked OM. A cache
 * eviction is then observed as a repeated metadata request. Write operations
 * need no stubbing at all: their return values are not used by the tests.
 * These tests complement the S3 API integration tests in
 * TestS3GatewayObjectCache by covering eviction from write operations that
 * are not reachable through the S3 API (rename, bucket settings, ACLs).
 */
class TestCachingS3OzoneRpcClient {

  private static final String VOLUME = "s3v";
  private static final String BUCKET = "bucket1";
  private static final String KEY = "key1";
  private static final S3Auth S3_AUTH =
      new S3Auth("stringToSign", "signature", "accessId", "user1");

  private MockedConstruction<OzoneManagerProtocolClientSideTranslatorPB>
      mockedOm;
  private TestClient client;

  @BeforeEach
  void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_S3G_OBJECT_CACHE_METRICS_ENABLED, false);
    // long TTLs so that only explicit evictions are observed in tests
    conf.setLong(OZONE_S3G_OBJECT_CACHE_VOLUME_ENTRY_TTL_MS, 600_000L);
    conf.setLong(OZONE_S3G_OBJECT_CACHE_BUCKET_ENTRY_TTL_MS, 600_000L);
    conf.setLong(OZONE_S3G_OBJECT_CACHE_KEY_ENTRY_TTL_MS, 600_000L);
    mockedOm = mockConstruction(
        OzoneManagerProtocolClientSideTranslatorPB.class,
        (om, context) -> stubOm(om));
    client = new TestClient(conf);
    client.setThreadLocalS3Auth(S3_AUTH);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (client != null) {
      client.close();
    }
    if (mockedOm != null) {
      mockedOm.close();
    }
  }

  @Test
  void readOperationsAreServedFromCache() throws Exception {
    client.getS3VolumeContext();
    client.getS3VolumeContext();
    verify(om(), times(1)).getS3VolumeContext();

    client.getBucketDetails(VOLUME, BUCKET);
    client.getBucketDetails(VOLUME, BUCKET);
    verify(om(), times(1)).getBucketInfo(VOLUME, BUCKET);

    client.headKey(BUCKET, KEY);
    client.headKey(BUCKET, KEY);
    verify(om(), times(1)).getKeyInfo(argThat(OmKeyArgs::isHeadOp), eq(true));

    client.getKey(BUCKET, KEY);
    client.getKey(BUCKET, KEY);
    verify(om(), times(1)).getKeyInfo(
        argThat(args -> !args.isHeadOp()
            && args.getMultipartUploadPartNumber() == 0), eq(true));

    client.getPartKey(BUCKET, KEY, 1);
    client.getPartKey(BUCKET, KEY, 1);
    verify(om(), times(1)).getKeyInfo(
        argThat(args -> args.getMultipartUploadPartNumber() == 1), eq(true));
  }

  @Test
  void deleteBucketEvictsBucketCache() throws Throwable {
    assertBucketCacheEvictedBy(() -> client.deleteBucket(VOLUME, BUCKET));
  }

  @Test
  void createBucketEvictsBucketCache() throws Throwable {
    assertBucketCacheEvictedBy(() -> client.createBucket(VOLUME, BUCKET));
  }

  @Test
  void setBucketOwnerEvictsBucketCache() throws Throwable {
    assertBucketCacheEvictedBy(
        () -> client.setBucketOwner(VOLUME, BUCKET, "owner2"));
  }

  @Test
  void setBucketVersioningEvictsBucketCache() throws Throwable {
    assertBucketCacheEvictedBy(
        () -> client.setBucketVersioning(VOLUME, BUCKET, true));
  }

  @Test
  void setBucketStorageTypeEvictsBucketCache() throws Throwable {
    assertBucketCacheEvictedBy(
        () -> client.setBucketStorageType(VOLUME, BUCKET, StorageType.SSD));
  }

  @Test
  void setBucketQuotaEvictsBucketCache() throws Throwable {
    // setBucketQuota itself reads the bucket info before updating it,
    // which adds one getBucketInfo call that does not go through the cache
    assertBucketCacheEvictedBy(1,
        () -> client.setBucketQuota(VOLUME, BUCKET, 1000, 1024));
  }

  @Test
  void setReplicationConfigEvictsBucketCache() throws Throwable {
    assertBucketCacheEvictedBy(() -> client.setReplicationConfig(
        VOLUME, BUCKET, ReplicationConfig.fromTypeAndFactor(
            ReplicationType.RATIS, ReplicationFactor.ONE)));
  }

  @SuppressWarnings("deprecation")
  @Test
  void setEncryptionKeyEvictsBucketCache() throws Throwable {
    assertBucketCacheEvictedBy(
        () -> client.setEncryptionKey(VOLUME, BUCKET, "encryptionKey"));
  }

  @Test
  void setAclOnBucketEvictsBucketCache() throws Throwable {
    assertBucketCacheEvictedBy(() -> client.setAcl(
        ozoneObj(OzoneObj.ResourceType.BUCKET, null),
        Collections.emptyList()));
  }

  @Test
  void addAclOnBucketEvictsBucketCache() throws Throwable {
    assertBucketCacheEvictedBy(() -> client.addAcl(
        ozoneObj(OzoneObj.ResourceType.BUCKET, null), userAcl()));
  }

  @Test
  void removeAclOnBucketEvictsBucketCache() throws Throwable {
    assertBucketCacheEvictedBy(() -> client.removeAcl(
        ozoneObj(OzoneObj.ResourceType.BUCKET, null), userAcl()));
  }

  @Test
  void setAclOnVolumeEvictsVolumeCache() throws Throwable {
    assertVolumeCacheEvictedBy(() -> client.setAcl(
        ozoneObj(OzoneObj.ResourceType.VOLUME, null),
        Collections.emptyList()));
  }

  @Test
  void addAclOnVolumeEvictsVolumeCache() throws Throwable {
    assertVolumeCacheEvictedBy(() -> client.addAcl(
        ozoneObj(OzoneObj.ResourceType.VOLUME, null), userAcl()));
  }

  @Test
  void removeAclOnVolumeEvictsVolumeCache() throws Throwable {
    assertVolumeCacheEvictedBy(() -> client.removeAcl(
        ozoneObj(OzoneObj.ResourceType.VOLUME, null), userAcl()));
  }

  @Test
  void setAclOnVolumeDoesNotEvictBucketCache() throws Exception {
    client.getBucketDetails(VOLUME, BUCKET);
    client.setAcl(ozoneObj(OzoneObj.ResourceType.VOLUME, null),
        Collections.emptyList());
    client.getBucketDetails(VOLUME, BUCKET);
    verify(om(), times(1)).getBucketInfo(VOLUME, BUCKET);
  }

  @Test
  void deleteKeyEvictsKeyCache() throws Throwable {
    assertKeyCacheEvictedBy(
        () -> client.deleteKey(VOLUME, BUCKET, KEY, false));
  }

  @Test
  void deleteKeysEvictsKeyCache() throws Throwable {
    assertKeyCacheEvictedBy(() -> client.deleteKeys(
        VOLUME, BUCKET, Collections.singletonList(KEY)));
  }

  @Test
  void deleteKeysQuietEvictsKeyCache() throws Throwable {
    assertKeyCacheEvictedBy(() -> client.deleteKeys(
        VOLUME, BUCKET, Collections.singletonList(KEY), true));
  }

  @Test
  void renameKeyEvictsKeyCache() throws Throwable {
    assertKeyCacheEvictedBy(
        () -> client.renameKey(VOLUME, BUCKET, KEY, "key2"));
  }

  @SuppressWarnings("deprecation")
  @Test
  void renameKeysEvictsKeyCache() throws Throwable {
    assertKeyCacheEvictedBy(() -> client.renameKeys(
        VOLUME, BUCKET, Collections.singletonMap(KEY, "key2")));
  }

  @Test
  void completeMultipartUploadEvictsKeyCache() throws Throwable {
    assertKeyCacheEvictedBy(() -> client.completeMultipartUpload(
        VOLUME, BUCKET, KEY, "uploadId",
        Collections.singletonMap(1, "etag")));
  }

  @Test
  void putObjectTaggingEvictsKeyCache() throws Throwable {
    assertKeyCacheEvictedBy(() -> client.putObjectTagging(
        VOLUME, BUCKET, KEY, Collections.singletonMap("tag", "value")));
  }

  @Test
  void deleteObjectTaggingEvictsKeyCache() throws Throwable {
    assertKeyCacheEvictedBy(
        () -> client.deleteObjectTagging(VOLUME, BUCKET, KEY));
  }

  @Test
  void createKeyEvictsKeyCache() throws Throwable {
    // the mocked OM rejects openKey, but eviction happens before the RPC
    assertKeyCacheEvictedBy(() -> assertThrows(OMException.class,
        () -> client.createKey(VOLUME, BUCKET, KEY, 10,
            ReplicationConfig.fromTypeAndFactor(
                ReplicationType.RATIS, ReplicationFactor.ONE),
            Collections.emptyMap(), Collections.emptyMap())));
  }

  @Test
  void setAclOnKeyEvictsKeyCache() throws Throwable {
    assertKeyCacheEvictedBy(() -> client.setAcl(
        ozoneObj(OzoneObj.ResourceType.KEY, KEY), Collections.emptyList()));
  }

  @Test
  void addAclOnKeyEvictsKeyCache() throws Throwable {
    assertKeyCacheEvictedBy(() -> client.addAcl(
        ozoneObj(OzoneObj.ResourceType.KEY, KEY), userAcl()));
  }

  @Test
  void removeAclOnKeyEvictsKeyCache() throws Throwable {
    assertKeyCacheEvictedBy(() -> client.removeAcl(
        ozoneObj(OzoneObj.ResourceType.KEY, KEY), userAcl()));
  }

  @Test
  void deleteKeyDoesNotEvictPartEntries() throws Exception {
    // part number cache entries are evicted only by TTL, see
    // CachingS3OzoneRpcClient#evictKeyCacheEntry
    client.getPartKey(BUCKET, KEY, 1);
    client.deleteKey(VOLUME, BUCKET, KEY, false);
    client.getPartKey(BUCKET, KEY, 1);
    verify(om(), times(1)).getKeyInfo(
        argThat(args -> args.getMultipartUploadPartNumber() == 1), eq(true));
  }

  private OzoneManagerProtocolClientSideTranslatorPB om() {
    assertEquals(1, mockedOm.constructed().size());
    return mockedOm.constructed().get(0);
  }

  private static void stubOm(
      OzoneManagerProtocolClientSideTranslatorPB om) throws IOException {
    when(om.getServiceInfo())
        .thenReturn(new ServiceInfoEx(Collections.emptyList(), null, null));
    // the mocked OM has no thread-local storage, a constant stub keeps
    // the cache keys consistent within a test
    when(om.getThreadLocalS3Auth()).thenReturn(S3_AUTH);
    when(om.getS3VolumeContext()).thenReturn(new S3VolumeContext(
        OmVolumeArgs.newBuilder()
            .setVolume(VOLUME)
            .setAdminName("admin")
            .setOwnerName("user1")
            .setCreationTime(1)
            .setModificationTime(1)
            .build(),
        "user1"));
    when(om.getBucketInfo(VOLUME, BUCKET)).thenReturn(OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setIsVersionEnabled(false)
        .setStorageType(StorageType.DISK)
        .setCreationTime(1)
        .setModificationTime(1)
        .build());
    when(om.getKeyInfo(any(OmKeyArgs.class), anyBoolean()))
        .thenReturn(new KeyInfoWithVolumeContext.Builder()
            .setKeyInfo(new OmKeyInfo.Builder()
                .setVolumeName(VOLUME)
                .setBucketName(BUCKET)
                .setKeyName(KEY)
                .setReplicationConfig(ReplicationConfig.fromTypeAndFactor(
                    ReplicationType.RATIS, ReplicationFactor.ONE))
                .setCreationTime(1)
                .setModificationTime(1)
                .build())
            .setUserPrincipal("user1")
            .build());
    // createKey/rewriteKey are expected to fail fast; the eviction under
    // test happens before the openKey RPC anyway
    when(om.openKey(any(OmKeyArgs.class)))
        .thenThrow(new OMException(OMException.ResultCodes.VOLUME_NOT_FOUND));
  }

  private static OzoneObj ozoneObj(OzoneObj.ResourceType type, String key) {
    OzoneObjInfo.Builder builder = OzoneObjInfo.Builder.newBuilder()
        .setVolumeName(VOLUME)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setResType(type);
    if (type != OzoneObj.ResourceType.VOLUME) {
      builder.setBucketName(BUCKET);
    }
    if (key != null) {
      builder.setKeyName(key);
    }
    return builder.build();
  }

  private static OzoneAcl userAcl() {
    return OzoneAcl.of(IAccessAuthorizer.ACLIdentityType.USER,
        "user1", OzoneAcl.AclScope.ACCESS, IAccessAuthorizer.ACLType.READ);
  }

  private void assertVolumeCacheEvictedBy(Executable writeOp)
      throws Throwable {
    client.getS3VolumeContext();
    client.getS3VolumeContext();
    verify(om(), times(1)).getS3VolumeContext();
    writeOp.execute();
    client.getS3VolumeContext();
    verify(om(), times(2)).getS3VolumeContext();
  }

  private void assertBucketCacheEvictedBy(Executable writeOp)
      throws Throwable {
    assertBucketCacheEvictedBy(0, writeOp);
  }

  /**
   * Asserts that a write operation evicts the cached bucket entry.
   *
   * @param getBucketInfoCallsByWriteOp getBucketInfo calls that the write
   *        operation itself performs (bypassing the cache), if any
   */
  private void assertBucketCacheEvictedBy(int getBucketInfoCallsByWriteOp,
      Executable writeOp) throws Throwable {
    client.getBucketDetails(VOLUME, BUCKET);
    client.getBucketDetails(VOLUME, BUCKET);
    verify(om(), times(1)).getBucketInfo(VOLUME, BUCKET);
    writeOp.execute();
    client.getBucketDetails(VOLUME, BUCKET);
    verify(om(), times(2 + getBucketInfoCallsByWriteOp))
        .getBucketInfo(VOLUME, BUCKET);
  }

  private void assertKeyCacheEvictedBy(Executable writeOp) throws Throwable {
    client.headKey(BUCKET, KEY);
    client.headKey(BUCKET, KEY);
    client.getKey(BUCKET, KEY);
    client.getKey(BUCKET, KEY);
    verify(om(), times(1)).getKeyInfo(argThat(OmKeyArgs::isHeadOp), eq(true));
    verify(om(), times(1)).getKeyInfo(
        argThat(args -> !args.isHeadOp()
            && args.getMultipartUploadPartNumber() == 0), eq(true));
    writeOp.execute();
    client.headKey(BUCKET, KEY);
    client.getKey(BUCKET, KEY);
    verify(om(), times(2)).getKeyInfo(argThat(OmKeyArgs::isHeadOp), eq(true));
    verify(om(), times(2)).getKeyInfo(
        argThat(args -> !args.isHeadOp()
            && args.getMultipartUploadPartNumber() == 0), eq(true));
  }

  /**
   * A {@link CachingS3OzoneRpcClient} exposing the protected S3 key lookups
   * for tests. Both transport factories are replaced with mocks: the OM
   * protocol translator built on top of the transport is mocked via
   * {@link #mockedOm}, so no RPC ever leaves the client.
   */
  private static final class TestClient extends CachingS3OzoneRpcClient {

    private TestClient(OzoneConfiguration conf) throws IOException {
      super(conf, null);
    }

    @Override
    protected OmTransport createOmTransport(String omServiceId) {
      return Mockito.mock(OmTransport.class);
    }

    @Override
    protected XceiverClientFactory createXceiverClientFactory(
        ServiceInfoEx serviceInfo) {
      return Mockito.mock(XceiverClientFactory.class);
    }

    KeyInfoWithVolumeContext headKey(String bucket, String key)
        throws IOException {
      return getS3KeyInfoWithS3Ctx(bucket, key, true);
    }

    KeyInfoWithVolumeContext getKey(String bucket, String key)
        throws IOException {
      return getS3KeyInfoWithS3Ctx(bucket, key, false);
    }

    KeyInfoWithVolumeContext getPartKey(String bucket, String key, int part)
        throws IOException {
      return getS3PartKeyInfoWithS3Ctx(bucket, key, part);
    }
  }
}
