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

import java.util.Objects;
import org.apache.hadoop.ozone.om.protocol.S3Auth;

/**
 * Cache key identifying an Ozone bucket by volume name, bucket name,
 * and the S3 authentication credentials of the requesting user.
 */
public class BucketCacheCacheKey extends S3ObjectCacheKey {
  private final String volume;
  private final String bucket;

  public BucketCacheCacheKey(String volume, String bucket, S3Auth s3Auth) {
    super(s3Auth);
    this.volume = volume;
    this.bucket = bucket;
  }

  public String getVolume() {
    return volume;
  }

  public String getBucket() {
    return bucket;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    if (!(o instanceof BucketCacheCacheKey)) {
      return false;
    }
    BucketCacheCacheKey that = (BucketCacheCacheKey) o;
    return Objects.equals(volume, that.volume)
        && Objects.equals(bucket, that.bucket);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), volume, bucket);
  }
}
