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

package org.apache.ozone.test;

import java.io.IOException;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.protocol.S3Auth;

/**
 * Helpers for integration tests that submit OM requests with {@link S3Auth}.
 */
public final class S3AuthTestUtils {

  /**
   * Test secret and SigV4 payload that match
   * {@link org.apache.hadoop.ozone.security.TestOzoneDelegationTokenSecretManager}.
   */
  public static final String TEST_AWS_SECRET = "dbaksbzljandlkandlsd";
  public static final String DEFAULT_ACCESS_KEY = "user";
  public static final String DEFAULT_SECRET_KEY = "password";
  public static final String TEST_STR_TO_SIGN = "AWS4-HMAC-SHA256\n" +
      "20190221T002037Z\n" +
      "20190221/us-west-1/s3/aws4_request\n" +
      "c297c080cce4e0927779823d3fd1f5cae71481a8f7dfc7e18d91851294efc47d";
  public static final String TEST_SIGNATURE =
      "56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf4765f46a14cd745ad";

  private S3AuthTestUtils() {
  }

  public static void storeTestS3Secret(MiniOzoneCluster cluster, String accessId)
      throws IOException {
    cluster.getOzoneManager().getS3SecretManager()
        .storeSecret(accessId, S3SecretValue.of(accessId, TEST_AWS_SECRET));
  }

  public static void storeDefaultCredentials(MiniOzoneCluster cluster)
      throws IOException {
    cluster.getOzoneManager().getS3SecretManager().storeSecret(
        DEFAULT_ACCESS_KEY,
        S3SecretValue.of(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY));
  }

  public static S3Auth createValidS3Auth(String accessId) {
    return new S3Auth(TEST_STR_TO_SIGN, TEST_SIGNATURE, accessId, accessId);
  }
}
