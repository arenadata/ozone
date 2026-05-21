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

package org.apache.hadoop.ozone.s3.client.cache;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.ratis.util.function.CheckedFunction;

/**
 * In-process implementation of {@link OzoneObjectCache} backed by a
 * Caffeine async loading cache with configurable maximum size and TTL.
 */
public class LocalOzoneObjectCache<K, V> implements OzoneObjectCache<K, V> {

  private final AsyncLoadingCache<K, V> cache;

  public LocalOzoneObjectCache(CheckedFunction<K, V, Exception> valueLoader, int maxSize, Duration entryTll) {
    this.cache = Caffeine.newBuilder()
        .maximumSize(maxSize)
        .expireAfterWrite(entryTll)
        .recordStats()
        .buildAsync(valueLoader::apply);
  }

  @Override
  public V get(K key) throws IOException {
    try {
      return cache.get(key).join();
    } catch (CompletionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(cause);
    }
  }

  @Override
  public void evict(K key) {
    cache.synchronous().invalidate(key);
  }

  @Override
  public void registerMetrics(MetricsRecordBuilder metricsRecordBuilder) {
    CacheStats stats = cache.synchronous().stats();
    metricsRecordBuilder
        .addGauge(Interns.info("hitCount", "Cache hit count"), stats.hitCount())
        .addGauge(Interns.info("hitRate", "Cache hit rate"), stats.hitRate())
        .addGauge(Interns.info("missCount", "Cache miss count"), stats.missCount())
        .addGauge(Interns.info("loadSuccessCount", "Successful cache loads"), stats.loadSuccessCount())
        .addGauge(Interns.info("loadFailureCount", "Failed cache loads"), stats.loadFailureCount())
        .addGauge(Interns.info("loadFailureRate", "Failed cache loads rate"), stats.loadFailureRate())
        .addGauge(Interns.info("evictionCount", "Cache eviction count"), stats.evictionCount())
        .addGauge(Interns.info("averageLoadPenalty", "Average load penalty (ns)"), stats.averageLoadPenalty());
  }
}
