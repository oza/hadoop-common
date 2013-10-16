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
  private final InternalSecretManagerService secretManagerService;

  class InternalSecretManagerService extends AbstractService {
    ArrayList<ServiceHandler> serviceHandlers;
    /**
     * Construct the service.
     *
     * @param name service name
     */
    public InternalSecretManagerService(String name) {
      super(name);
      serviceHandlers = new ArrayList<ServiceHandler>();
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
      for (ServiceHandler handler :serviceHandlers){
        handler.serviceInit(conf);
      }
    }
    @Override
    protected void serviceStart() throws Exception {
      for (ServiceHandler handler :serviceHandlers){
        handler.serviceStart();
      }
    }
    @Override
    protected void serviceStop() throws Exception {
      // serviceStop callbacks handlers in reverse order of
      // "serviceStart" method.
      Collections.reverse(serviceHandlers);
      try {
        for (ServiceHandler handler : serviceHandlers){
          handler.serviceStop();
        }
      } finally {
        Collections.reverse(serviceHandlers);
      }
    }
  }

  public SecretManagerService(String serviceName) {
    secretManagerService =
      new InternalSecretManagerService(serviceName);
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

}
