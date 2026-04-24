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

public class S3KeyDetailsCacheCacheKey extends S3KeyInfoCacheCacheKey {
  private final Integer partNumber;

  public S3KeyDetailsCacheCacheKey(String bucket, String key, Integer part, S3Auth s3Auth) {
    super(bucket, key, s3Auth);
    this.partNumber = part;
  }

  public Integer getPartNumber() {
    return partNumber;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    if (!(o instanceof S3KeyDetailsCacheCacheKey)) {
      return false;
    }
    S3KeyDetailsCacheCacheKey that = (S3KeyDetailsCacheCacheKey) o;
    return Objects.equals(partNumber, that.partNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), partNumber);
  }
}
