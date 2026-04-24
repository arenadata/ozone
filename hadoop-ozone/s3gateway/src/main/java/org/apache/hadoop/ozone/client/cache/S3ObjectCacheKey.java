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

public class S3ObjectCacheKey {
  private final S3Auth s3Auth;

  public S3ObjectCacheKey(S3Auth s3Auth) {
    this.s3Auth = s3Auth;
  }

  public S3Auth getS3Auth() {
    return s3Auth;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof S3ObjectCacheKey)) {
      return false;
    }
    S3ObjectCacheKey that = (S3ObjectCacheKey) o;
    // deliberately use only the immutable part of the S3Auth
    return Objects.equals(s3Auth.getAccessID(), that.s3Auth.getAccessID());
  }

  @Override
  public int hashCode() {
    // deliberately use only the immutable part of the S3Auth
    return Objects.hash(s3Auth.getAccessID());
  }
}
