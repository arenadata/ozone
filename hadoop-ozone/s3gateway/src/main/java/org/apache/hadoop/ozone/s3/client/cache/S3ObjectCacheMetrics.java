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

import java.io.Closeable;
import java.util.Map;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;

/**
 * Hadoop Metrics2 source that collects and publishes statistics for all
 * registered S3 object caches (volume, bucket, and key caches).
 */
public class S3ObjectCacheMetrics implements MetricsSource, Closeable {
  private static final String SOURCE_NAME = S3ObjectCacheMetrics.class.getSimpleName();

  private final Map<String, OzoneObjectCache<?, ?>> caches;

  public S3ObjectCacheMetrics(Map<String, OzoneObjectCache<?, ?>> caches) {
    this.caches = caches;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    caches.forEach((name, cache) -> {
      MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME)
          .tag(Interns.info("cacheName", "Cache name"), name);
      cache.registerMetrics(recordBuilder);
    });
  }

  @Override
  public void close() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  public static S3ObjectCacheMetrics register(Map<String, OzoneObjectCache<?, ?>> caches) {
    S3ObjectCacheMetrics metrics = new S3ObjectCacheMetrics(caches);
    DefaultMetricsSystem.instance().register(SOURCE_NAME, "S3 Object Cache Metrics", metrics);
    return metrics;
  }
}
