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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * SecretManager wrapper class for YARN to treat ServiceManager as Service.
 */
@Public
@Evolving
public class SecretManagerServiceInternal extends AbstractService {
  ArrayList<ServiceHandler> serviceHandlers;
  /**
   * Construct the service.
   *
   * @param name service name
   */
  public SecretManagerServiceInternal(String name) {
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

  public void setConfig(Configuration conf) {
    super.setConfig(conf);
  }

  public void registerServiceHandler(ServiceHandler handler) {
    serviceHandlers.add(handler);
  }
}
