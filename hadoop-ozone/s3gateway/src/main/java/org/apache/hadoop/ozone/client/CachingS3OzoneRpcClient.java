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

package org.apache.hadoop.ozone.client;

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_BUCKET_ENTRY_TTL_SECONDS;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_BUCKET_ENTRY_TTL_SECONDS_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_BUCKET_MAX_SIZE;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_BUCKET_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_KEY_ENTRY_TTL_SECONDS;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_KEY_ENTRY_TTL_SECONDS_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_KEY_MAX_SIZE;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_KEY_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_METRICS_ENABLED;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_METRICS_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_VOLUME_ENTRY_TTL_SECONDS;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_VOLUME_ENTRY_TTL_SECONDS_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_VOLUME_MAX_SIZE;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_OBJECT_CACHE_VOLUME_MAX_SIZE_DEFAULT;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.client.cache.BucketCacheCacheKey;
import org.apache.hadoop.ozone.client.cache.LocalOzoneObjectCache;
import org.apache.hadoop.ozone.client.cache.OzoneObjectCache;
import org.apache.hadoop.ozone.client.cache.S3KeyDetailsCacheCacheKey;
import org.apache.hadoop.ozone.client.cache.S3KeyInfoCacheCacheKey;
import org.apache.hadoop.ozone.client.cache.S3ObjectCacheKey;
import org.apache.hadoop.ozone.client.cache.S3ObjectCacheMetrics;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachingS3OzoneRpcClient extends RpcClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(CachingS3OzoneRpcClient.class);

  private final OzoneObjectCache<S3ObjectCacheKey, S3VolumeContext> volumeCache;
  private final OzoneObjectCache<BucketCacheCacheKey, OzoneBucket> bucketCache;
  private final OzoneObjectCache<S3KeyInfoCacheCacheKey, KeyInfoWithVolumeContext> headKeyInfoCache;
  private final OzoneObjectCache<S3KeyDetailsCacheCacheKey, KeyInfoWithVolumeContext> keyInfoCache;

  private final S3ObjectCacheMetrics cacheMetrics;

  public CachingS3OzoneRpcClient(ConfigurationSource conf, String omServiceId) throws IOException {
    super(conf, omServiceId);

    this.volumeCache = new LocalOzoneObjectCache<>(
        this::loadVolumeContext,
        conf.getInt(OZONE_S3G_OBJECT_CACHE_VOLUME_MAX_SIZE,
            OZONE_S3G_OBJECT_CACHE_VOLUME_MAX_SIZE_DEFAULT),
        Duration.ofMillis(conf.getLong(OZONE_S3G_OBJECT_CACHE_VOLUME_ENTRY_TTL_SECONDS,
            OZONE_S3G_OBJECT_CACHE_VOLUME_ENTRY_TTL_SECONDS_DEFAULT))
    );

    this.bucketCache = new LocalOzoneObjectCache<>(
        this::loadBucket,
        conf.getInt(OZONE_S3G_OBJECT_CACHE_BUCKET_MAX_SIZE,
            OZONE_S3G_OBJECT_CACHE_BUCKET_MAX_SIZE_DEFAULT),
        Duration.ofMillis(conf.getLong(OZONE_S3G_OBJECT_CACHE_BUCKET_ENTRY_TTL_SECONDS,
            OZONE_S3G_OBJECT_CACHE_BUCKET_ENTRY_TTL_SECONDS_DEFAULT))
    );

    int keyCacheMaxSize = conf.getInt(OZONE_S3G_OBJECT_CACHE_KEY_MAX_SIZE,
        OZONE_S3G_OBJECT_CACHE_KEY_MAX_SIZE_DEFAULT);
    Duration keyCacheTtl = Duration.ofMillis(conf.getLong(OZONE_S3G_OBJECT_CACHE_KEY_ENTRY_TTL_SECONDS,
        OZONE_S3G_OBJECT_CACHE_KEY_ENTRY_TTL_SECONDS_DEFAULT));

    this.headKeyInfoCache = new LocalOzoneObjectCache<>(
        this::loadHeadKeyInfoWithCtx,
        keyCacheMaxSize,
        keyCacheTtl
    );

    this.keyInfoCache = new LocalOzoneObjectCache<>(
        this::loadKeyInfoWithCtx,
        keyCacheMaxSize,
        keyCacheTtl
    );

    boolean cacheMetricsEnabled = conf.getBoolean(
        OZONE_S3G_OBJECT_CACHE_METRICS_ENABLED,
        OZONE_S3G_OBJECT_CACHE_METRICS_ENABLED_DEFAULT);

    this.cacheMetrics = cacheMetricsEnabled ? initCacheMetrics() : null;
  }

  @Override
  public S3VolumeContext getS3VolumeContext() throws IOException {
    S3VolumeContext s3VolumeContext = volumeCache.get(new S3ObjectCacheKey(getThreadLocalS3Auth()));
    updateS3Principal(s3VolumeContext.getUserPrincipal());
    return s3VolumeContext;
  }

  @Override
  public OzoneBucket getBucketDetails(String volumeName, String bucketName) throws IOException {
    return bucketCache.get(new BucketCacheCacheKey(volumeName, bucketName, getThreadLocalS3Auth()));
  }

  // no need to update S3Principal, because it's done in the parent client

  @Override
  protected KeyInfoWithVolumeContext getS3KeyInfoWithS3Ctx(
      String bucketName, String keyName, boolean isHeadOp) throws IOException {
    return isHeadOp
        ? headKeyInfoCache.get(new S3KeyInfoCacheCacheKey(bucketName, keyName, getThreadLocalS3Auth()))
        : keyInfoCache.get(new S3KeyDetailsCacheCacheKey(bucketName, keyName, null, getThreadLocalS3Auth()));
  }
  // no need to update S3Principal, because it's done in the parent client

  @Override
  protected KeyInfoWithVolumeContext getS3PartKeyInfoWithS3Ctx(
      String bucketName, String keyName, int partNumber) throws IOException {
    return keyInfoCache.get(new S3KeyDetailsCacheCacheKey(bucketName, keyName, partNumber, getThreadLocalS3Auth()));
  }

  @Override
  public void deleteBucket(String volumeName, String bucketName) throws IOException {
    try {
      super.deleteBucket(volumeName, bucketName);
    } finally {
      bucketCache.evict(new BucketCacheCacheKey(volumeName, bucketName, getThreadLocalS3Auth()));
    }
  }

  @Override
  public void deleteKey(String volumeName, String bucketName, String keyName, boolean recursive) throws IOException {
    try {
      super.deleteKey(volumeName, bucketName, keyName, recursive);
    } finally {
      evictKeyCacheEntry(bucketName, keyName);
    }
  }

  @Override
  public void deleteKeys(String volumeName, String bucketName, List<String> keyNameList) throws IOException {
    try {
      super.deleteKeys(volumeName, bucketName, keyNameList);
    } finally {
      keyNameList.forEach(keyName -> evictKeyCacheEntry(bucketName, keyName));
    }
  }

  @Override
  public Map<String, ErrorInfo> deleteKeys(String volumeName, String bucketName, List<String> keyNameList,
                                           boolean quiet) throws IOException {
    try {
      return super.deleteKeys(volumeName, bucketName, keyNameList, quiet);
    } finally {
      keyNameList.forEach(keyName -> evictKeyCacheEntry(bucketName, keyName));
    }
  }

  @Override
  public void renameKey(String volumeName, String bucketName, String fromKeyName, String toKeyName)
      throws IOException {
    try {
      super.renameKey(volumeName, bucketName, fromKeyName, toKeyName);
    } finally {
      evictKeyCacheEntry(bucketName, fromKeyName);
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public void renameKeys(String volumeName, String bucketName, Map<String, String> keyMap) throws IOException {
    try {
      super.renameKeys(volumeName, bucketName, keyMap);
    } finally {
      keyMap.keySet().forEach(fromKeyName -> evictKeyCacheEntry(bucketName, fromKeyName));
    }
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      if (cacheMetrics != null) {
        cacheMetrics.close();
      }
    }
  }

  private void evictKeyCacheEntry(String bucketName, String keyName) {
    try {
      headKeyInfoCache.evict(new S3KeyInfoCacheCacheKey(bucketName, keyName, getThreadLocalS3Auth()));
      // we don't know the existing keys for multipart loads, so let them be evicted by ttl
      keyInfoCache.evict(new S3KeyDetailsCacheCacheKey(bucketName, keyName, null, getThreadLocalS3Auth()));
    } catch (Exception e) {
      LOG.warn("Failed to evict key cache entry for bucket: {}, key: {}", bucketName, keyName, e);
    }
  }

  private S3VolumeContext loadVolumeContext(S3ObjectCacheKey cacheKey) throws IOException {
    return withS3Auth(cacheKey, key -> super.getS3VolumeContext());
  }

  private OzoneBucket loadBucket(BucketCacheCacheKey cacheKey) throws IOException {
    return withS3Auth(cacheKey,
        key -> super.getBucketDetails(key.getVolume(), key.getBucket()));
  }

  private KeyInfoWithVolumeContext loadHeadKeyInfoWithCtx(S3KeyInfoCacheCacheKey cacheKey) throws IOException {
    return withS3Auth(cacheKey, key ->
        super.getS3KeyInfoWithS3Ctx(cacheKey.getBucket(), cacheKey.getKey(), true));
  }

  private KeyInfoWithVolumeContext loadKeyInfoWithCtx(S3KeyDetailsCacheCacheKey cacheKey) throws IOException {
    return withS3Auth(cacheKey,
        key -> key.getPartNumber() == null
            ? super.getS3KeyInfoWithS3Ctx(key.getBucket(), key.getKey(), false)
            : super.getS3PartKeyInfoWithS3Ctx(key.getBucket(), key.getKey(), key.getPartNumber()));
  }

  // set S3 auth context for the cache loader thread
  private <K extends S3ObjectCacheKey, V> V withS3Auth(K key, CheckedFunction<K, V, IOException> action)
      throws IOException {
    try {
      setThreadLocalS3Auth(key.getS3Auth());
      return action.apply(key);
    } finally {
      clearThreadLocalS3Auth();
    }
  }

  private S3ObjectCacheMetrics initCacheMetrics() {
    return S3ObjectCacheMetrics.register(
        ImmutableMap.of(
            "s3VolumeCache", volumeCache,
            "s3BucketCache", bucketCache,
            "s3HeadKeyCache", headKeyInfoCache,
            "s3KeyInfoCache", keyInfoCache
        )
    );
  }
}
