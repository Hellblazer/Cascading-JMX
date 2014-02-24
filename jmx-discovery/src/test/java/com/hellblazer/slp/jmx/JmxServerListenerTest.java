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
import static junit.framework.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.UUID;

import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.hellblazer.jmx.cascading.CascadingServiceMBean;
import com.hellblazer.slp.ServiceEvent;
import com.hellblazer.slp.ServiceEvent.EventType;
import com.hellblazer.slp.jmx.JmxServerListener;
import com.hellblazer.slp.ServiceListener;
import com.hellblazer.slp.ServiceReference;
import com.hellblazer.slp.ServiceScope;
import com.hellblazer.slp.ServiceURL;

/**
 * @author hhildebrand
 * 
 */
public class JmxServerListenerTest {
    public static JMXConnectorServer construct(InetSocketAddress jmxEndpoint,
                                               MBeanServer mbs)
                                                               throws IOException {
        JMXServiceURL url = new JMXServiceURL("rmi", jmxEndpoint.getHostName(),
                                              jmxEndpoint.getPort());
        return JMXConnectorServerFactory.newJMXConnectorServer(url,
                                                               new HashMap<String, Object>(),
                                                               mbs);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testlifecycle() throws Exception {
        MBeanServer remoteMbs = ManagementFactory.getPlatformMBeanServer();
        CascadingServiceMBean cascadingService = mock(CascadingServiceMBean.class);
        String registration = UUID.randomUUID().toString();
        when(
             cascadingService.mount(isA(JMXServiceURL.class),
                                    (Map<String, Object>) eq(null),
                                    (ObjectName) eq(null), (String) eq(null))).thenReturn(registration);
        ServiceScope scope = mock(ServiceScope.class);
        String query = String.format("(%s=%s)", SERVICE_TYPE,
                                     "test._jmx._http._tcp");
        ArgumentCaptor<ServiceListener> listenerCaptor = ArgumentCaptor.forClass(ServiceListener.class);
        JMXConnectorServer server = construct(new InetSocketAddress(0),
                                              remoteMbs);
        server.start();
        JMXServiceURL jmxServiceUrl = server.getAddress();
        ServiceURL url = new ServiceURL(jmxServiceUrl.toString());
        ServiceReference reference = new ServiceReference(
                                                          url,
                                                          Collections.EMPTY_MAP,
                                                          UUID.randomUUID()) {

        };
        Hashtable<String, String> table = new Hashtable<>();
        table.put(JmxServerListener.HOST, url.getHost());
        table.put(JmxServerListener.TYPE,
                  MBeanServerConnection.class.getSimpleName());
        table.put(JmxServerListener.URL, "\"" + url.getServiceURL() + "\"");
        when(
             cascadingService.mount(eq(jmxServiceUrl), (Map<String, ?>) any(),
                                    (ObjectName) any(),
                                    eq("daemon/" + url.getHost()))).thenReturn(registration);

        JmxServerListener listener = new JmxServerListener(cascadingService,
                                                           null, null, scope,
                                                           null);

        listener.listenFor(query);
        verify(scope).addServiceListener(listenerCaptor.capture(), eq(query));

        ServiceListener sl = listenerCaptor.getValue();
        assertNotNull(sl);

        sl.serviceChanged(new ServiceEvent(EventType.REGISTERED, reference));
        sl.serviceChanged(new ServiceEvent(EventType.UNREGISTERED, reference));

        listener.removeQuery(query);
        verify(scope).removeServiceListener(isA(ServiceListener.class),
                                            eq(query));
        verify(cascadingService).unmount(registration);
    }
}
