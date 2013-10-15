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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import java.io.IOException;
import java.util.*;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.LifecycleEvent;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.SecretManagementService;

/**
 * AMRM-tokens are per ApplicationAttempt. If users redistribute their
 * tokens, it is their headache, god save them. I mean you are not supposed to
 * distribute keys to your vault, right? Anyways, ResourceManager saves each
 * token locally in memory till application finishes and to a store for restart,
 * so no need to remember master-keys even after rolling them.
 */
public class AMRMTokenSecretManager extends
    SecretManager<AMRMTokenIdentifier> implements Service {

  private static final Log LOG = LogFactory
    .getLog(AMRMTokenSecretManager.class);

  private final SecretManagementService secretManagementService
    = new SecretManagementService(AMRMTokenSecretManager.class.getName());
  private SecretKey masterKey;
  private final Timer timer;
  private final long rollingInterval;



  private final Map<ApplicationAttemptId, byte[]> passwords =
      new HashMap<ApplicationAttemptId, byte[]>();

  /**
   * Create an {@link AMRMTokenSecretManager}
   */
  public AMRMTokenSecretManager(Configuration conf) {
    rollMasterKey();
    this.timer = new Timer();
    this.rollingInterval =
        conf
          .getLong(
            YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
            YarnConfiguration.DEFAULT_RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS) * 1000;
  }

  public synchronized void applicationMasterFinished(
      ApplicationAttemptId appAttemptId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Application finished, removing password for " + appAttemptId);
    }
    this.passwords.remove(appAttemptId);
  }

  private class MasterKeyRoller extends TimerTask {
    @Override
    public void run() {
      rollMasterKey();
    }
  }

  @Private
  public synchronized void setMasterKey(SecretKey masterKey) {
    this.masterKey = masterKey;
  }

  @Private
  public synchronized SecretKey getMasterKey() {
    return this.masterKey;
  }

  @Private
  synchronized void rollMasterKey() {
    LOG.info("Rolling master-key for amrm-tokens");
    this.masterKey = generateSecret();
  }

  /**
   * Create a password for a given {@link AMRMTokenIdentifier}. Used to
   * send to the AppicationAttempt which can give it back during authentication.
   */
  @Override
  public synchronized byte[] createPassword(
      AMRMTokenIdentifier identifier) {
    ApplicationAttemptId applicationAttemptId =
        identifier.getApplicationAttemptId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating password for " + applicationAttemptId);
    }
    byte[] password = createPassword(identifier.getBytes(), masterKey);
    this.passwords.put(applicationAttemptId, password);
    return password;
  }

  /**
   * Populate persisted password of AMRMToken back to AMRMTokenSecretManager.
   */
  public synchronized void
      addPersistedPassword(Token<AMRMTokenIdentifier> token) throws IOException {
    AMRMTokenIdentifier identifier = token.decodeIdentifier();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding password for " + identifier.getApplicationAttemptId());
    }
    this.passwords.put(identifier.getApplicationAttemptId(),
      token.getPassword());
  }

  /**
   * Retrieve the password for the given {@link AMRMTokenIdentifier}.
   * Used by RPC layer to validate a remote {@link AMRMTokenIdentifier}.
   */
  @Override
  public synchronized byte[] retrievePassword(
      AMRMTokenIdentifier identifier) throws InvalidToken {
    ApplicationAttemptId applicationAttemptId =
        identifier.getApplicationAttemptId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to retrieve password for " + applicationAttemptId);
    }
    byte[] password = this.passwords.get(applicationAttemptId);
    if (password == null) {
      throw new InvalidToken("Password not found for ApplicationAttempt "
          + applicationAttemptId);
    }
    return password;
  }

  /**
   * Creates an empty TokenId to be used for de-serializing an
   * {@link AMRMTokenIdentifier} by the RPC layer.
   */
  @Override
  public AMRMTokenIdentifier createIdentifier() {
    return new AMRMTokenIdentifier();
  }

  @Override
  public void init(Configuration config) {
    secretManagementService.init(config);
  }

  @Override
  public void start() {
    secretManagementService.start();
    this.timer.scheduleAtFixedRate(new MasterKeyRoller(), 0, rollingInterval);
  }

  @Override
  public void stop() {
    this.timer.cancel();
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
