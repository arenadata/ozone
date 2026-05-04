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

import java.io.IOException;
import java.time.Duration;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.ratis.util.function.CheckedFunction;

/**
 * Generic cache interface for Ozone objects, supporting retrieval, eviction,
 * and metrics registration.
 */
public interface OzoneObjectCache<K, V> {
  V get(K key) throws IOException;

  void evict(K key);

  void registerMetrics(MetricsRecordBuilder metricsRecordBuilder);

  static <K, V> OzoneObjectCache<K, V> local(CheckedFunction<K, V, Exception> valueProvider, int maxSize,
                                             Duration entryTll) {
    return new LocalOzoneObjectCache<>(valueProvider, maxSize, entryTll);
  }
}
