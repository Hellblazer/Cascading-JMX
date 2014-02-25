/** (C) Copyright 2014 Hal Hildebrand, All Rights Reserved
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 */
package com.hellblazer.slp.jmx;

import static com.hellblazer.slp.ServiceScope.SERVICE_TYPE;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hellblazer.jmx.cascading.CascadingService;
import com.hellblazer.nexus.config.GossipScopeModule;
import com.hellblazer.slp.ServiceScope;
import com.hellblazer.slp.config.ServiceScopeConfiguration;

/**
 * @author hhildebrand
 * 
 */
public class JmxDiscoveryConfiguration {

    public static JmxDiscoveryConfiguration fromYaml(InputStream yaml)
                                                                      throws JsonParseException,
                                                                      JsonMappingException,
                                                                      IOException {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        objectMapper.registerModule(new GossipScopeModule());
        return objectMapper.readValue(yaml, JmxDiscoveryConfiguration.class);
    }

    /**
     * The <i>domain path</i> under which the source MBeans will be mounted in
     * the target <tt>MBeanServer</tt>. This string may contain up to 2 %s
     * patterns to accomidate the host and port of the remote MBeanServer.
     */
    public String                    targetPath   = "/[%s/%s]";

    /**
     * An <tt>ObjectName</tt> pattern that must be satisfied by the
     * <tt>ObjectName</tt>s of the source MBeans. A null sourcePattern is
     * equivalent to *:*
     */
    public String                    sourcePattern;

    /**
     * A Map object that will be passed to the
     * {@link JMXConnectorFactory#connect(JMXServiceURL,Map)} method, in order
     * to connect to the source <tt>MBeanServer</tt>.
     */
    public Map<String, ?>            sourceMap;

    /**
     * The JMX object name to register the cascading service
     */
    public String                    name;

    /**
     * The discovery scope configuration
     */
    public ServiceScopeConfiguration discovery;

    /**
     * The list of abstract service names corresponding to desired JMX adapter
     * services
     */
    public List<String>              serviceNames = Collections.emptyList();

    public JmxServerListener construct() throws Exception {
        ServiceScope scope = discovery.construct();
        scope.start();
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        CascadingService cascadingService = new CascadingService();
        mbs.registerMBean(cascadingService, new ObjectName(name));
        JmxServerListener listener = new JmxServerListener(cascadingService,
                                                           sourcePattern,
                                                           sourceMap, scope,
                                                           targetPath);
        for (String serviceType : serviceNames) {
            listener.listenFor("(" + SERVICE_TYPE + "=" + serviceType + ")");
        }
        return listener;
    }
}
