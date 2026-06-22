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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_TOKEN;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto.Type.S3AUTHINFO;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.token.SecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class which holds methods required for parse/validation of
 * S3 Authentication Information which is part of OMRequest.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class S3SecurityUtil {

  private static final Logger LOG = LoggerFactory.getLogger(S3SecurityUtil.class);

  private S3SecurityUtil() {
  }

  /**
   * Validate S3 Credentials which are part of {@link OMRequest}.
   *
   * If validation is successful returns, else throw exception.
   * @throws OMException         validation failure
   *         ServiceException    Server is not leader or not ready
   */
  public static void validateS3Credential(OMRequest omRequest,
      OzoneManager ozoneManager) throws ServiceException, OMException {
    OzoneTokenIdentifier s3Token = constructS3Token(omRequest);
    try {
      if (ozoneManager.isSecurityEnabled()) {
        // authenticate user with signature verification through
        // delegationTokenMgr validateToken via retrievePassword
        ozoneManager.getDelegationTokenMgr().retrievePassword(s3Token);
      } else {
        ozoneManager.checkLeaderStatus();
        validateS3AuthInfo(s3Token, ozoneManager.getS3SecretManager());
      }
    } catch (OMNotLeaderException | OMLeaderNotReadyException e) {
      throw new ServiceException(e);
    } catch (SecretManager.InvalidToken e) {
      if (e.getCause() != null &&
          (e.getCause().getClass() == OMNotLeaderException.class ||
          e.getCause().getClass() == OMLeaderNotReadyException.class)) {
        throw new ServiceException(e.getCause());
      }

      // TODO: Just check are we okay to log entire token in failure case.
      OzoneManagerProtocolServerSideTranslatorPB.getLog().error(
          "signatures do NOT match for S3 identifier:{}", s3Token, e);
      throw new OMException("User " + s3Token.getAwsAccessId()
          + " request authorization failure: signatures do NOT match",
          INVALID_TOKEN);
    }
  }

  /**
   * Validates if a S3 identifier is valid or not.
   */
  static byte[] validateS3AuthInfo(OzoneTokenIdentifier identifier,
      S3SecretManager s3SecretManager) throws SecretManager.InvalidToken {
    LOG.trace("Validating S3AuthInfo for identifier:{}", identifier);
    if (identifier.getOwner() == null) {
      throw new SecretManager.InvalidToken(
          "Owner is missing from the S3 auth token");
    }
    if (!identifier.getOwner().toString().equals(identifier.getAwsAccessId())) {
      LOG.error(
          "Owner and AWSAccessId is different in the S3 token. Possible "
              + " security attack: {}",
          identifier);
      throw new SecretManager.InvalidToken(
          "Invalid S3 identifier: owner=" + identifier.getOwner()
              + ", awsAccessId=" + identifier.getAwsAccessId());
    }
    String awsSecret;
    try {
      awsSecret = s3SecretManager.getSecretString(identifier.getAwsAccessId());
    } catch (IOException e) {
      LOG.warn("S3 identifier validation failed:{}", identifier, e);
      throw new SecretManager.InvalidToken("No S3 secret found for S3 identifier:"
          + identifier);
    }

    if (awsSecret == null) {
      throw new SecretManager.InvalidToken("No S3 secret found for S3 identifier:"
          + identifier);
    }

    if (AWSV4AuthValidator.validateRequest(identifier.getStrToSign(),
        identifier.getSignature(), awsSecret)) {
      return identifier.getSignature().getBytes(UTF_8);
    }
    throw new SecretManager.InvalidToken("Invalid S3 identifier:"
        + identifier);
  }

  /**
   * Construct and return {@link OzoneTokenIdentifier} from {@link OMRequest}.
   */
  private static OzoneTokenIdentifier constructS3Token(OMRequest omRequest) {
    S3Authentication auth = omRequest.getS3Authentication();
    OzoneTokenIdentifier s3Token = new OzoneTokenIdentifier();
    s3Token.setTokenType(S3AUTHINFO);
    s3Token.setStrToSign(auth.getStringToSign());
    s3Token.setSignature(auth.getSignature());
    s3Token.setAwsAccessId(auth.getAccessId());
    s3Token.setOwner(new Text(auth.getAccessId()));
    return s3Token;
  }
}
