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

package org.apache.hadoop.ozone.security;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_TOKEN;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.S3SecretCache;
import org.apache.hadoop.ozone.om.S3SecretLockedManager;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.S3SecretManagerImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for {@link S3SecurityUtil}.
 */
public class TestS3SecurityUtil {

  private static final String STR_TO_SIGN = "AWS4-HMAC-SHA256\n" +
      "20190221T002037Z\n" +
      "20190221/us-west-1/s3/aws4_request\n" +
      "c297c080cce4e0927779823d3fd1f5cae71481a8f7dfc7e18d91851294efc47d";
  private static final String SIGNATURE = "56ec73ba1974f8feda8365c3caef89c5d4a688d" +
      "5f9baccf4765f46a14cd745ad";

  private OzoneManager ozoneManager;

  @TempDir
  private Path folder;

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    ServerUtils.setOzoneMetaDirPath(conf, folder.toString());

    final Map<String, S3SecretValue> s3Secrets = new HashMap<>();
    s3Secrets.put("testuser1",
        S3SecretValue.of("testuser1", "dbaksbzljandlkandlsd"));

    ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.isSecurityEnabled()).thenReturn(false);
    when(ozoneManager.getDelegationTokenMgr()).thenReturn(null);
    OMMetadataManager metadataManager = new OmMetadataManagerImpl(conf, ozoneManager);
    when(ozoneManager.getMetadataManager()).thenReturn(metadataManager);
    S3SecretManager s3SecretManager = new S3SecretLockedManager(
        new S3SecretManagerImpl(new S3SecretStoreMap(s3Secrets),
            mock(S3SecretCache.class)),
        metadataManager.getLock()
    );
    when(ozoneManager.getS3SecretManager()).thenReturn(s3SecretManager);
    doNothing().when(ozoneManager).checkLeaderStatus();
  }

  @Test
  public void testValidateS3CredentialSuccessOnNonSecureCluster() {
    OMRequest omRequest = createOmRequest("testuser1", SIGNATURE, STR_TO_SIGN);
    assertDoesNotThrow(() -> S3SecurityUtil.validateS3Credential(omRequest, ozoneManager));
  }

  @Test
  public void testValidateS3CredentialUnknownAccessKeyOnNonSecureCluster() {
    OMRequest omRequest = createOmRequest("unknown-user", SIGNATURE, STR_TO_SIGN);
    OMException ex = assertThrows(OMException.class,
        () -> S3SecurityUtil.validateS3Credential(omRequest, ozoneManager));
    assertEquals(INVALID_TOKEN, ex.getResult());
  }

  @Test
  public void testValidateS3CredentialBadSignatureOnNonSecureCluster() {
    OMRequest omRequest = createOmRequest("testuser1",
        SIGNATURE + "bad", STR_TO_SIGN);
    OMException ex = assertThrows(OMException.class,
        () -> S3SecurityUtil.validateS3Credential(omRequest, ozoneManager));
    assertEquals(INVALID_TOKEN, ex.getResult());
  }

  private static OMRequest createOmRequest(String accessId, String signature,
      String stringToSign) {
    return OMRequest.newBuilder()
        .setCmdType(Type.InfoVolume)
        .setClientId(UUID.randomUUID().toString())
        .setS3Authentication(S3Authentication.newBuilder()
            .setAccessId(accessId)
            .setSignature(signature)
            .setStringToSign(stringToSign)
            .build())
        .build();
  }
}
