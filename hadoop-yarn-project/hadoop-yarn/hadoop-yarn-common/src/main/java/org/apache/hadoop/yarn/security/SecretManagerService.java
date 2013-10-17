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

package org.apache.hadoop.yarn.security;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.LifecycleEvent;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.yarn.security.ServiceHandler;

import java.io.IOException;
import java.util.*;

/**
 * SecretManager wrapper class for YARN to treat ServiceManager as Service.
 */
@Public
@Evolving
public abstract class SecretManagerService<T extends TokenIdentifier> extends
    SecretManager<T> implements Service {
  private static Log LOG = LogFactory
    .getLog(SecretManagerService.class);
  private final SecretManagerServiceInternal secretManagerService;

  public SecretManagerService(String serviceName) {
    secretManagerService =
      new SecretManagerServiceInternal(serviceName);
  }

  @Override
  public void init(Configuration config) {
    secretManagerService.init(config);
  }

  @Override
  public void start() {
    secretManagerService.start();
  }

  @Override
  public void stop() {
    secretManagerService.stop();
  }

  @Override
  public void close() throws IOException {
    secretManagerService.close();
  }

  @Override
  public void registerServiceListener(ServiceStateChangeListener listener) {
    secretManagerService.registerServiceListener(listener);
  }

  @Override
  public void unregisterServiceListener(ServiceStateChangeListener listener) {
    secretManagerService.unregisterServiceListener(listener);
  }

  @Override
  public String getName() {
    return secretManagerService.getName();
  }

  @Override
  public Configuration getConfig() {
    return secretManagerService.getConfig();
  }

  @Override
  public STATE getServiceState() {
    return secretManagerService.getServiceState();
  }

  @Override
  public long getStartTime() {
    return secretManagerService.getStartTime();
  }

  @Override
  public boolean isInState(STATE state) {
    return secretManagerService.isInState(state);
  }

  @Override
  public Throwable getFailureCause() {
    return secretManagerService.getFailureCause();
  }

  @Override
  public STATE getFailureState() {
    return secretManagerService.getFailureState();
  }

  @Override
  public boolean waitForServiceToStop(long timeout) {
    return secretManagerService.waitForServiceToStop(timeout);
  }

  @Override
  public List<LifecycleEvent> getLifecycleHistory() {
    return secretManagerService.getLifecycleHistory();
  }

  @Override
  public Map<String, String> getBlockers() {
    return secretManagerService.getBlockers();
  }

    /* ===================================================================== */
  /* Override Points */
  /* ===================================================================== */

  /**
   * All initialization code needed by a service.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #init(Configuration)} prevents re-entrancy.
   *
   * The base implementation checks to see if the subclass has created
   * a new configuration instance, and if so, updates the base class value
   * @param conf configuration
   * @throws Exception on a failure -these will be caught,
   * possibly wrapped, and wil; trigger a service stop
   */
  protected void serviceInit(Configuration conf) throws Exception {
    if (conf != getConfig()) {
      LOG.debug("Config has been overridden during init");
      secretManagerService.setConfig(conf);
    }
  }

  /**
   * Actions called during the INITED to STARTED transition.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #start()} prevents re-entrancy.
   *
   * @throws Exception if needed -these will be caught,
   * wrapped, and trigger a service stop
   */
  protected void serviceStart() throws Exception {

  }

  /**
   * Actions called during the transition to the STOPPED state.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #stop()} prevents re-entrancy.
   *
   * Implementations MUST write this to be robust against failures, including
   * checks for null references -and for the first failure to not stop other
   * attempts to shut down parts of the service.
   *
   * @throws Exception if needed -these will be caught and logged.
   */
  protected void serviceStop() throws Exception {

  }

  public void registerServiceHandler(ServiceHandler handler) {
    secretManagerService.registerServiceHandler(handler);
  }
}
