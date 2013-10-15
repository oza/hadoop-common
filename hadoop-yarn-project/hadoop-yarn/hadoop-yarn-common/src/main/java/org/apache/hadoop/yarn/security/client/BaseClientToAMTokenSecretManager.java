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

package org.apache.hadoop.yarn.security.client;

import javax.crypto.SecretKey;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.service.LifecycleEvent;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A base {@link SecretManager} for AMs to extend and validate Client-RM tokens
 * issued to clients by the RM using the underlying master-key shared by RM to
 * the AMs on their launch. All the methods are called by either Hadoop RPC or
 * YARN, so this class is strictly for the purpose of inherit/extend and
 * register with Hadoop RPC.
 */
@Public
@Evolving
public abstract class BaseClientToAMTokenSecretManager extends
    SecretManager<ClientToAMTokenIdentifier> implements Service {

  final SecretManagementService secretManagementService;


  public BaseClientToAMTokenSecretManager(String serviceName) {
    secretManagementService = new SecretManagementService(serviceName);
  }

  @Private
  public abstract SecretKey getMasterKey(
      ApplicationAttemptId applicationAttemptId);

  @Private
  @Override
  public synchronized byte[] createPassword(
      ClientToAMTokenIdentifier identifier) {
    return createPassword(identifier.getBytes(),
      getMasterKey(identifier.getApplicationAttemptID()));
  }

  @Private
  @Override
  public byte[] retrievePassword(ClientToAMTokenIdentifier identifier)
      throws SecretManager.InvalidToken {
    SecretKey masterKey = getMasterKey(identifier.getApplicationAttemptID());
    if (masterKey == null) {
      throw new SecretManager.InvalidToken("Illegal client-token!");
    }
    return createPassword(identifier.getBytes(), masterKey);
  }

  @Private
  @Override
  public ClientToAMTokenIdentifier createIdentifier() {
    return new ClientToAMTokenIdentifier();
  }

  @Override
  public void init(Configuration config) {
    secretManagementService.init(config);
  }

  @Override
  public void start() {
    secretManagementService.start();
  }

  @Override
  public void stop() {
    secretManagementService.stop();
  }

  @Override
  public void close() throws IOException {
    secretManagementService.close();
  }

  @Override
  public void registerServiceListener(ServiceStateChangeListener listener) {
    secretManagementService.registerServiceListener(listener);
  }

  @Override
  public void unregisterServiceListener(ServiceStateChangeListener listener) {
    secretManagementService.unregisterServiceListener(listener);
  }

  @Override
  public String getName() {
    return secretManagementService.getName();
  }

  @Override
  public Configuration getConfig() {
    return secretManagementService.getConfig();
  }

  @Override
  public STATE getServiceState() {
    return secretManagementService.getServiceState();
  }

  @Override
  public long getStartTime() {
    return secretManagementService.getStartTime();
  }

  @Override
  public boolean isInState(STATE state) {
    return secretManagementService.isInState(state);
  }

  @Override
  public Throwable getFailureCause() {
    return secretManagementService.getFailureCause();
  }

  @Override
  public STATE getFailureState() {
    return secretManagementService.getFailureState();
  }

  @Override
  public boolean waitForServiceToStop(long timeout) {
    return secretManagementService.waitForServiceToStop(timeout);
  }

  @Override
  public List<LifecycleEvent> getLifecycleHistory() {
    return secretManagementService.getLifecycleHistory();
  }

  @Override
  public Map<String, String> getBlockers() {
    return secretManagementService.getBlockers();
  }
}
