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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security;

import javax.crypto.SecretKey;

import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.security.client.SecretManagerService;

public class LocalizerTokenSecretManager extends
    SecretManagerService<LocalizerTokenIdentifier> {

  private final SecretKey secretKey;
  
  public LocalizerTokenSecretManager() {
    super(LocalizerTokenSecretManager.class.getName());
    this.secretKey = generateSecret();
  }
  
  @Override
  protected byte[] createPassword(LocalizerTokenIdentifier identifier) {
    return createPassword(identifier.getBytes(), secretKey);
  }

  @Override
  public byte[] retrievePassword(LocalizerTokenIdentifier identifier)
      throws org.apache.hadoop.security.token.SecretManager.InvalidToken {
    return createPassword(identifier.getBytes(), secretKey);
  }

  @Override
  public LocalizerTokenIdentifier createIdentifier() {
    return new LocalizerTokenIdentifier();
  }

}
