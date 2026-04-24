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

package org.apache.hadoop.ozone.client.cache;

import java.util.Objects;
import org.apache.hadoop.ozone.om.protocol.S3Auth;

public class S3KeyInfoCacheCacheKey extends S3ObjectCacheKey {
  private final String bucket;
  private final String key;

  public S3KeyInfoCacheCacheKey(String bucket, String key, S3Auth s3Auth) {
    super(s3Auth);
    this.bucket = bucket;
    this.key = key;
  }

  public String getBucket() {
    return bucket;
  }

  public String getKey() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    S3KeyInfoCacheCacheKey that = (S3KeyInfoCacheCacheKey) o;
    return Objects.equals(bucket, that.bucket) && Objects.equals(key, that.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), bucket, key);
  }
}
