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

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_BUCKET_ENTRY_TTL_MS;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_BUCKET_MAX_SIZE;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_ENABLED;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_KEY_ENTRY_TTL_MS;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_KEY_MAX_SIZE;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_VOLUME_ENTRY_TTL_MS;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_VOLUME_MAX_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecordImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.s3.client.cache.S3ObjectCacheMetrics;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * Integration tests for the S3 Gateway object cache (ADH-8124).
 *
 * The gateway runs with single-entry volume/bucket/key caches and short
 * TTLs, so write, size and TTL evictions are verified through the S3 API.
 * Volume cache eviction is not observable through the S3 API and is
 * verified via the published cache metrics instead.
 *
 * Each test uses dedicated S3 users (cache keys are scoped by access ID)
 * and starts with empty caches. A direct Ozone client performs out-of-band
 * operations that must not evict cache entries.
 */
public class TestS3GatewayObjectCache {

  private static final String BUCKET = "s3gcachetest";
  private static final String S3_VOLUME_NAME = "s3v";
  private static final String CONTENT_A = "s3g-object-cache-test-content-a";
  private static final String CONTENT_B = "s3g-object-cache-test-content-b";
  private static final String TAG_KEY = "test-tag-key";
  private static final String TAG_VALUE = "test-tag-value";
  private static final long TTL_CONFIG_TIMEOUT = 3_000L;
  private static final Duration TTL_WAIT_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration BEFORE_TTL_WAIT_TIMEOUT = Duration.ofSeconds(2);

  private static MiniOzoneCluster cluster;
  private static OzoneClient directClient;
  private static OzoneBucket s3OzoneBucket;
  private static URI s3gEndpoint;

  private S3Client s3;
  private S3Client s3User2;

  @BeforeAll
  static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_S3G_OBJECT_CACHE_ENABLED, true);
    conf.setInt(OZONE_S3G_OBJECT_CACHE_KEY_MAX_SIZE, 1);
    conf.setLong(OZONE_S3G_OBJECT_CACHE_KEY_ENTRY_TTL_MS, TTL_CONFIG_TIMEOUT);
    conf.setInt(OZONE_S3G_OBJECT_CACHE_BUCKET_MAX_SIZE, 1);
    conf.setLong(OZONE_S3G_OBJECT_CACHE_BUCKET_ENTRY_TTL_MS, TTL_CONFIG_TIMEOUT);
    conf.setInt(OZONE_S3G_OBJECT_CACHE_VOLUME_MAX_SIZE, 1);
    conf.setLong(OZONE_S3G_OBJECT_CACHE_VOLUME_ENTRY_TTL_MS, TTL_CONFIG_TIMEOUT);
    S3GatewayService s3g = new S3GatewayService();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .addService(s3g)
        .build();
    cluster.waitForClusterToBeReady();
    OzoneConfiguration s3gConf = s3g.getConf();
    s3gEndpoint = URI.create(
        "http://" + s3gConf.get(OZONE_S3G_HTTP_ADDRESS_KEY));
    directClient = cluster.newClient();
    try (S3Client bucketOwner = newS3Client("user-init")) {
      bucketOwner.createBucket(b -> b.bucket(BUCKET));
    }
    s3OzoneBucket = directClient.getObjectStore()
        .getVolume(S3_VOLUME_NAME).getBucket(BUCKET);
  }

  @AfterAll
  static void shutdown() throws IOException {
    if (directClient != null) {
      directClient.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @BeforeEach
  void setUp() {
    s3 = newS3Client("user-" + UUID.randomUUID());
    s3User2 = newS3Client("user2-" + UUID.randomUUID());
  }

  @AfterEach
  void tearDown() {
    if (s3 != null) {
      s3.close();
    }
    if (s3User2 != null) {
      s3User2.close();
    }
  }

  @Test
  void testObjectReadsAreServedFromCache() {
    final String key = uniqueKey();
    putObject(key, CONTENT_A);
    final HeadObjectResponse head = head(key);
    assertThat(head.contentLength()).isEqualTo(CONTENT_A.length());
    assertThat(getObjectAsBytes(key).asUtf8String()).isEqualTo(CONTENT_A);
  }

  @Test
  void testOverwriteObjectEvictsKeyCache() {
    final String key = uniqueKey();
    putObject(key, CONTENT_A);
    final HeadObjectResponse firstHead = head(key);
    putObject(key, CONTENT_B);
    final HeadObjectResponse secondHead = head(key);
    assertThat(secondHead.contentLength()).isEqualTo(CONTENT_B.length());
    assertThat(secondHead.eTag()).isNotEqualTo(firstHead.eTag());
    assertThat(getObjectAsBytes(key).asUtf8String()).isEqualTo(CONTENT_B);
  }

  @Test
  void testDeleteObjectEvictsKeyCache() {
    final String key = uniqueKey();
    putObject(key, CONTENT_A);
    head(key);
    s3.deleteObject(b -> b.bucket(BUCKET).key(key));
    assertThatThrownBy(() -> head(key)).isInstanceOf(NoSuchKeyException.class);
  }

  @Test
  void testDeleteObjectsEvictAllKeyCacheEntries() {
    final String key1 = uniqueKey();
    final String key2 = uniqueKey();
    putObject(key1, CONTENT_A);
    putObject(key2, CONTENT_A);
    head(key1);
    head(key2);
    s3.deleteObjects(DeleteObjectsRequest.builder()
        .bucket(BUCKET)
        .delete(Delete.builder().objects(
            ObjectIdentifier.builder().key(key1).build(),
            ObjectIdentifier.builder().key(key2).build()).build())
        .build());
    assertThatThrownBy(() -> head(key1))
        .isInstanceOf(NoSuchKeyException.class);
    assertThatThrownBy(() -> head(key2))
        .isInstanceOf(NoSuchKeyException.class);
  }

  @Test
  void testCompleteMultipartUploadEvictsKeyCache() {
    final String key = uniqueKey();
    putObject(key, CONTENT_A);
    final HeadObjectResponse firstHead = head(key);
    final String uploadId = s3.createMultipartUpload(
        b -> b.bucket(BUCKET).key(key)).uploadId();
    final UploadPartResponse part = s3.uploadPart(
        b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(1),
        RequestBody.fromString(CONTENT_B));
    s3.completeMultipartUpload(b -> b.bucket(BUCKET).key(key)
        .uploadId(uploadId)
        .multipartUpload(CompletedMultipartUpload.builder().parts(
                CompletedPart.builder().partNumber(1).eTag(part.eTag()).build())
            .build()));
    final HeadObjectResponse secondHead = head(key);
    assertThat(secondHead.contentLength()).isEqualTo(CONTENT_B.length());
    assertThat(secondHead.eTag()).isNotEqualTo(firstHead.eTag());
    assertThat(getObjectAsBytes(key).asUtf8String()).isEqualTo(CONTENT_B);
  }

  @Test
  void testPutObjectTaggingEvictsKeyCache() {
    final String key = uniqueKey();
    putObject(key, CONTENT_A);
    assertThat(getObjectAsBytes(key).response().tagCount()).isNull();
    s3.putObjectTagging(b -> b.bucket(BUCKET).key(key)
        .tagging(t -> t.tagSet(
            Tag.builder().key(TAG_KEY).value(TAG_VALUE).build())));
    assertThat(getObjectAsBytes(key).response().tagCount()).isEqualTo(1);
  }

  @Test
  void testDeleteObjectTaggingEvictsKeyCache() {
    final String key = uniqueKey();
    putObject(key, CONTENT_A);
    s3.putObjectTagging(b -> b.bucket(BUCKET).key(key)
        .tagging(t -> t.tagSet(
            Tag.builder().key(TAG_KEY).value(TAG_VALUE).build())));
    assertThat(getObjectAsBytes(key).response().tagCount()).isEqualTo(1);
    s3.deleteObjectTagging(b -> b.bucket(BUCKET).key(key));
    assertThat(getObjectAsBytes(key).response().tagCount()).isNull();
  }

  @Test
  void testDeleteBucketEvictsBucketCache() {
    final String bucket = uniqueBucket();
    s3.createBucket(b -> b.bucket(bucket));
    s3.headBucket(b -> b.bucket(bucket));
    s3.deleteBucket(b -> b.bucket(bucket));
    assertThatThrownBy(() -> s3.headBucket(b -> b.bucket(bucket)))
        .isInstanceOf(NoSuchBucketException.class);
  }

  @Test
  void testKeyCacheSizeLimitEvictsOldestEntry() throws Exception {
    final String keyA = uniqueKey();
    final String keyB = uniqueKey();
    putObject(keyA, CONTENT_A);
    putObject(keyB, CONTENT_A);
    head(keyA);
    head(keyB);
    s3OzoneBucket.deleteKey(keyA);
    assertThatThrownBy(() -> head(keyA)).isInstanceOf(NoSuchKeyException.class);
    s3OzoneBucket.deleteKey(keyB);
    assertThatCode(() -> head(keyB)).doesNotThrowAnyException();
  }

  @Test
  void testKeyCacheTtlEvictionReloadsEntry() throws Exception {
    final String key = uniqueKey();
    putObject(key, CONTENT_A);
    head(key);
    s3OzoneBucket.deleteKey(key);
    Awaitility.await("Key should exist in cache until evicted by TTL timeout")
        .during(BEFORE_TTL_WAIT_TIMEOUT)
        .atMost(TTL_WAIT_TIMEOUT)
        .pollInterval(Duration.ofMillis(250))
        .untilAsserted(() -> assertThat(head(key).contentLength())
            .isEqualTo(CONTENT_A.length()));
    Awaitility.await("Key should not exist after TTL timeout")
        .atMost(TTL_WAIT_TIMEOUT)
        .pollInterval(Duration.ofMillis(250))
        .untilAsserted(() -> assertThatThrownBy(() -> head(key))
            .isInstanceOf(NoSuchKeyException.class));
  }

  @Test
  void testBucketCacheSizeLimitEvictsOldestEntry() throws Exception {
    final String bucketA = uniqueBucket();
    final String bucketB = uniqueBucket();
    s3.createBucket(b -> b.bucket(bucketA));
    s3.createBucket(b -> b.bucket(bucketB));
    s3.headBucket(b -> b.bucket(bucketA));
    s3.headBucket(b -> b.bucket(bucketB));
    final OzoneVolume s3Volume = directClient.getObjectStore()
        .getVolume(S3_VOLUME_NAME);
    s3Volume.deleteBucket(bucketA);
    assertThatThrownBy(() -> s3.headBucket(b -> b.bucket(bucketA)))
        .isInstanceOf(NoSuchBucketException.class);
    s3Volume.deleteBucket(bucketB);
    assertThatCode(() -> s3.headBucket(b -> b.bucket(bucketB)))
        .doesNotThrowAnyException();
  }

  @Test
  void testBucketCacheTtlEvictionReloadsEntry() throws Exception {
    final String bucket = uniqueBucket();
    s3.createBucket(b -> b.bucket(bucket));
    s3.headBucket(b -> b.bucket(bucket));
    directClient.getObjectStore().getVolume(S3_VOLUME_NAME)
        .deleteBucket(bucket);
    Awaitility.await("Bucket should exist in cache until evicted by TTL timeout")
        .during(BEFORE_TTL_WAIT_TIMEOUT)
        .atMost(TTL_WAIT_TIMEOUT)
        .pollInterval(Duration.ofMillis(250))
        .untilAsserted(() -> assertThatCode(
            () -> s3.headBucket(b -> b.bucket(bucket)))
            .doesNotThrowAnyException());
    Awaitility.await("Bucket should not exist after TTL timeout")
        .atMost(TTL_WAIT_TIMEOUT)
        .pollInterval(Duration.ofMillis(250))
        .untilAsserted(() ->
            assertThatThrownBy(() -> s3.headBucket(b -> b.bucket(bucket)))
                .isInstanceOf(NoSuchBucketException.class));
  }

  @Test
  void testVolumeCacheSizeLimitEvictsOldestEntry() {
    // load the volume cache entry for the first user to increase evictionCount
    s3.headBucket(b -> b.bucket(BUCKET));
    final long evictionsBefore = cacheGauge("s3VolumeCache", "evictionCount");
    s3User2.headBucket(b -> b.bucket(BUCKET));
    Awaitility.await("The oldest volume cache entry should be evicted by size")
        .atMost(TTL_WAIT_TIMEOUT)
        .pollInterval(Duration.ofMillis(250))
        .untilAsserted(() ->
            assertThat(cacheGauge("s3VolumeCache", "evictionCount"))
                .isGreaterThan(evictionsBefore));
  }

  @Test
  void testVolumeCacheTtlEvictionReloadsEntry() {
    s3.headBucket(b -> b.bucket(BUCKET));
    final long missesBefore = cacheGauge("s3VolumeCache", "missCount");
    Awaitility.await("Volume missCount should not increase before evict miss")
        .during(BEFORE_TTL_WAIT_TIMEOUT)
        .atMost(TTL_WAIT_TIMEOUT)
        .pollInterval(Duration.ofMillis(250))
        .untilAsserted(() -> {
          s3.headBucket(b -> b.bucket(BUCKET));
          assertThat(cacheGauge("s3VolumeCache", "missCount"))
              .isEqualTo(missesBefore);
        });
    Awaitility.await("Volume missCount should increase after evict miss")
        .atMost(TTL_WAIT_TIMEOUT)
        .pollInterval(Duration.ofMillis(250))
        .untilAsserted(() -> {
          s3.headBucket(b -> b.bucket(BUCKET));
          assertThat(cacheGauge("s3VolumeCache", "missCount"))
              .isGreaterThan(missesBefore + 1);
        });
  }

  @Test
  void testGetObjectByPartNumberUsesKeyDetailsCache() throws Exception {
    final String key = uniqueKey();
    // Non-last parts must be at least 5 MB in size. S3 limitation.
    final byte[] part1Content = new byte[5 * 1024 * 1024 + 1];
    Arrays.fill(part1Content, (byte) 'a');
    putMultipartObject(key, part1Content,
        CONTENT_B.getBytes(StandardCharsets.UTF_8));
    assertThat(getObjectAsBytesByPartNum(key, 1).asByteArray())
        .containsExactly(part1Content);
    assertThat(getObjectAsBytesByPartNum(key, 2).asUtf8String())
        .isEqualTo(CONTENT_B);
    s3OzoneBucket.deleteKey(key);
    assertThat(getObjectAsBytesByPartNum(key, 2).asUtf8String())
        .isEqualTo(CONTENT_B);
    assertThatThrownBy(() -> getObjectAsBytesByPartNum(key, 1))
        .isInstanceOf(NoSuchKeyException.class);
  }

  @Test
  void testPerUserKeyCacheIsolation() throws Exception {
    final String key = uniqueKey();
    putObject(key, CONTENT_A);
    head(key);
    s3OzoneBucket.deleteKey(key);
    assertThat(head(key).contentLength()).isEqualTo(CONTENT_A.length());
    assertThatThrownBy(() ->
        s3User2.headObject(b -> b.bucket(BUCKET).key(key)))
        .isInstanceOf(NoSuchKeyException.class);
  }

  @Test
  void testCacheMetricsPublished() {
    final String key = uniqueKey();
    putObject(key, CONTENT_A); // volume and bucket cache loads
    head(key);                 // miss + load on the head key cache
    head(key);                 // hit on the head key cache
    getObjectAsBytes(key);     // key details cache
    final MetricsSource source = DefaultMetricsSystem.instance()
        .getSource(S3ObjectCacheMetrics.class.getSimpleName());
    assertThat(source)
        .as("S3ObjectCacheMetrics source is not registered in the metrics")
        .isNotNull();
    final MetricsCollectorImpl collector = new MetricsCollectorImpl();
    source.getMetrics(collector, true);
    final Map<String, MetricsRecordImpl> recordsByCache = new HashMap<>();
    for (MetricsRecordImpl record : collector.getRecords()) {
      recordsByCache.put(tagValue(record, "cacheName"), record);
    }
    assertThat(recordsByCache).containsOnlyKeys(
        "s3VolumeCache", "s3BucketCache", "s3HeadKeyCache", "s3KeyInfoCache");
    recordsByCache.forEach((cache, record) -> {
      for (String gauge : new String[]{"hitCount", "hitRate", "missCount",
          "loadSuccessCount", "loadFailureCount", "loadFailureRate",
          "evictionCount", "averageLoadPenalty"}) {
        assertThat(metric(record, gauge))
            .as("%s does not publish the %s gauge", cache, gauge)
            .isNotNull();
      }
    });
    final MetricsRecordImpl headCache =
        recordsByCache.get("s3HeadKeyCache");
    assertThat(metric(headCache, "missCount").value().longValue())
        .isGreaterThanOrEqualTo(1L);
    assertThat(metric(headCache, "hitCount").value().longValue())
        .isGreaterThanOrEqualTo(1L);
    assertThat(metric(headCache, "loadSuccessCount").value().longValue())
        .isGreaterThanOrEqualTo(1L);
    assertThat(metric(recordsByCache.get("s3VolumeCache"), "missCount")
        .value().longValue()).isGreaterThanOrEqualTo(1L);
    assertThat(metric(recordsByCache.get("s3BucketCache"), "missCount")
        .value().longValue()).isGreaterThanOrEqualTo(1L);
    assertThat(metric(recordsByCache.get("s3KeyInfoCache"), "missCount")
        .value().longValue()).isGreaterThanOrEqualTo(1L);
  }

  private static long cacheGauge(String cacheName, String gaugeName) {
    final MetricsSource source = DefaultMetricsSystem.instance()
        .getSource(S3ObjectCacheMetrics.class.getSimpleName());
    assertThat(source)
        .as("S3ObjectCacheMetrics source is not registered in the metrics")
        .isNotNull();
    final MetricsCollectorImpl collector = new MetricsCollectorImpl();
    source.getMetrics(collector, true);
    for (MetricsRecordImpl record : collector.getRecords()) {
      if (cacheName.equals(tagValue(record, "cacheName"))) {
        final AbstractMetric gauge = metric(record, gaugeName);
        assertThat(gauge)
            .as("%s does not publish the %s gauge", cacheName, gaugeName)
            .isNotNull();
        return gauge.value().longValue();
      }
    }
    throw new AssertionError("No metrics record for cache " + cacheName);
  }

  private static String tagValue(MetricsRecordImpl record, String tag) {
    for (MetricsTag metricsTag : record.tags()) {
      if (metricsTag.name().equals(tag)) {
        return metricsTag.value();
      }
    }
    return null;
  }

  private static AbstractMetric metric(
      MetricsRecordImpl record, String name) {
    for (AbstractMetric m : record.metrics()) {
      if (m.name().equals(name)) {
        return m;
      }
    }
    return null;
  }

  private static S3Client newS3Client(String accessKeyId) {
    return S3Client.builder()
        .region(Region.US_EAST_1)
        .endpointOverride(s3gEndpoint)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKeyId, "password")))
        .forcePathStyle(true)
        .build();
  }

  private void putObject(String key, String content) {
    s3.putObject(b -> b.bucket(BUCKET).key(key),
        RequestBody.fromString(content));
  }

  private void putMultipartObject(
      String key, byte[] part1Content, byte[] part2Content) {
    final String uploadId = s3.createMultipartUpload(
        b -> b.bucket(BUCKET).key(key)).uploadId();
    final UploadPartResponse part1 = s3.uploadPart(
        b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(1),
        RequestBody.fromBytes(part1Content));
    final UploadPartResponse part2 = s3.uploadPart(
        b -> b.bucket(BUCKET).key(key).uploadId(uploadId).partNumber(2),
        RequestBody.fromBytes(part2Content));
    s3.completeMultipartUpload(b -> b.bucket(BUCKET).key(key)
        .uploadId(uploadId)
        .multipartUpload(CompletedMultipartUpload.builder().parts(
                CompletedPart.builder().partNumber(1).eTag(part1.eTag()).build(),
                CompletedPart.builder().partNumber(2).eTag(part2.eTag()).build())
            .build()));
  }

  private HeadObjectResponse head(String key) {
    return s3.headObject(b -> b.bucket(BUCKET).key(key));
  }

  private ResponseBytes<GetObjectResponse> getObjectAsBytes(String key) {
    return s3.getObjectAsBytes(b -> b.bucket(BUCKET).key(key));
  }

  private ResponseBytes<GetObjectResponse> getObjectAsBytesByPartNum(
      String key, int partNumber) {
    return s3.getObjectAsBytes(b -> b.bucket(BUCKET).key(key).partNumber(partNumber));
  }

  private static String uniqueKey() {
    return "key-" + UUID.randomUUID();
  }

  private static String uniqueBucket() {
    return ("bucket-" + UUID.randomUUID()).toLowerCase(Locale.ROOT);
  }
}
