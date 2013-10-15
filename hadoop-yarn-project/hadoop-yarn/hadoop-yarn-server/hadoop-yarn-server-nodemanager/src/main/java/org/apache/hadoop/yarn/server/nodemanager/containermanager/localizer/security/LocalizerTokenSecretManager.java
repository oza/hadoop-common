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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.service.LifecycleEvent;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.yarn.security.client.SecretManagementService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LocalizerTokenSecretManager extends
    SecretManager<LocalizerTokenIdentifier> implements Service {

  private final SecretKey secretKey;
  private final SecretManagementService secretManagementService
    = new SecretManagementService(LocalizerTokenSecretManager.class.getName());
  
  public LocalizerTokenSecretManager() {
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
