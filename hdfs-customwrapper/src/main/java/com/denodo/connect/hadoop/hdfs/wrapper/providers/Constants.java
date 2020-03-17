/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.denodo.connect.hadoop.hdfs.wrapper.providers;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 *  Required by AWSAssumedRoleCredentialProvider.
 *  From https://github.com/apache/hadoop/commit/9a013b255f301c557c3868dc1ad657202e9e7a67,
 *  as in hadoop-aws v2.x (the one compatible with java 7) does not exists.
 *
 * All the constants used with the {@link S3AFileSystem}.
 *
 * Some of the strings are marked as {@code Unstable}. This means
 * that they may be unsupported in future; at which point they will be marked
 * as deprecated and simply ignored.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class Constants {

  private Constants() {
  }

  // aws credentials provider
  public static final String AWS_CREDENTIALS_PROVIDER =
      "fs.s3a.aws.credentials.provider";


  /**
   * AWS Role to request.
   */
  public static final String ASSUMED_ROLE_ARN =
      "fs.s3a.assumed.role.arn";

  /**
   * Session name for the assumed role, must be valid characters according to the AWS APIs. If not set, one is
   * generated from the current Hadoop/Kerberos username.
   */
  public static final String ASSUMED_ROLE_SESSION_NAME =
      "fs.s3a.assumed.role.session.name";

  /**
   * Duration of assumed roles before a refresh is attempted.
   */
  public static final String ASSUMED_ROLE_SESSION_DURATION =
      "fs.s3a.assumed.role.session.duration";

  /**
   * Simple Token Service Endpoint. If unset, uses the default endpoint.
   */
  public static final String ASSUMED_ROLE_STS_ENDPOINT =
      "fs.s3a.assumed.role.sts.endpoint";

  public static final long ASSUMED_ROLE_SESSION_DURATION_DEFAULT = 30 * 60;

  /**
   * list of providers to authenticate for the assumed role.
   */
  public static final String ASSUMED_ROLE_CREDENTIALS_PROVIDER =
      "fs.s3a.assumed.role.credentials.provider";

  /**
   * JSON policy containing the policy to apply to the role.
   */
  public static final String ASSUMED_ROLE_POLICY =
      "fs.s3a.assumed.role.policy";


  // switch to the fast block-by-block upload mechanism
  // this is the only supported upload mechanism
  @Deprecated
  public static final String FAST_UPLOAD = "fs.s3a.fast.upload";
  @Deprecated
  public static final boolean DEFAULT_FAST_UPLOAD = false;

  //initial size of memory buffer for a fast upload
  @Deprecated
  public static final String FAST_BUFFER_SIZE = "fs.s3a.fast.buffer.size";


  /**
   * The standard encryption algorithm AWS supports. Different implementations may support others (or none). Use the
   * S3AEncryptionMethods instead when configuring which Server Side Encryption to use. Value: "{@value}".
   */
  @Deprecated
  public static final String SERVER_SIDE_ENCRYPTION_AES256 =
      "AES256";

}