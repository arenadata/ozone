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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdds.server.http.HttpServer2;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests S3 Gateway HTTP filter path mappings.
 */
class TestS3GatewayHttpServerFilterMapping {

  @TempDir
  private Path tempDir;

  @Test
  void s3ApiDoesNotMapAuthenticationFilterToHtmlObjects()
      throws Exception {
    InspectableS3GatewayHttpServer server =
        new InspectableS3GatewayHttpServer(newConfig());
    try {
      WebAppContext context = server.webAppContext();

      assertTrue(hasAuthenticationFilter(context));
      assertFalse(getPathSpecs(context).contains("*.html"));
      assertFalse(getPathSpecs(context).contains("*.jsp"));
    } finally {
      server.stop();
    }
  }

  @Test
  void s3WebAdminKeepsAuthenticationFilterForHtmlPages()
      throws Exception {
    InspectableS3GatewayWebAdminServer server =
        new InspectableS3GatewayWebAdminServer(newConfig());
    try {
      WebAppContext context = server.webAppContext();

      assertTrue(hasAuthenticationFilter(context));
      assertTrue(getPathSpecs(context).contains("*.html"));
      assertTrue(getPathSpecs(context).contains("*.jsp"));
    } finally {
      server.stop();
    }
  }

  private OzoneConfiguration newConfig() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_HTTP_BASEDIR, tempDir.toString());
    conf.set(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY,
        HttpConfig.Policy.HTTP_ONLY.name());
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        AuthenticationFilterInitializer.class.getName());
    conf.set("hadoop.http.authentication.type", "simple");
    conf.set("hadoop.http.authentication.simple.anonymous.allowed", "true");
    conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY, "localhost:0");
    conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTP_BIND_HOST_KEY, "localhost");
    conf.set(S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTP_ADDRESS_KEY,
        "localhost:0");
    conf.set(S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTP_BIND_HOST_KEY,
        "localhost");
    return conf;
  }

  private static boolean hasAuthenticationFilter(WebAppContext context) {
    return Arrays.stream(context.getServletHandler().getFilters())
        .anyMatch(filter -> AuthenticationFilter.class.getName()
            .equals(filter.getClassName()));
  }

  private static List<String> getPathSpecs(WebAppContext context) {
    return Arrays.stream(context.getServletHandler().getFilterMappings())
        .flatMap(mapping -> Arrays.stream(mapping.getPathSpecs() == null
            ? new String[0] : mapping.getPathSpecs()))
        .collect(Collectors.toList());
  }

  private static class InspectableS3GatewayHttpServer
      extends S3GatewayHttpServer {

    InspectableS3GatewayHttpServer(OzoneConfiguration conf)
        throws IOException {
      super(conf, "s3gateway");
    }

    WebAppContext webAppContext() {
      return getWebAppContext();
    }
  }

  private static class InspectableS3GatewayWebAdminServer
      extends S3GatewayWebAdminServer {

    InspectableS3GatewayWebAdminServer(OzoneConfiguration conf)
        throws IOException {
      super(conf, "s3g-web");
    }

    WebAppContext webAppContext() {
      return getWebAppContext();
    }
  }
}
