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

import static com.hellblazer.utils.Utils.allocatePort;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerFactory;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import net.gescobar.jmx.impl.MBeanFactory;

import org.junit.After;
import org.junit.Test;

import com.hellblazer.gossip.Gossip;
import com.hellblazer.gossip.SystemView;
import com.hellblazer.gossip.UdpCommunications;
import com.hellblazer.jmx.cascading.CascadingService;
import com.hellblazer.nexus.GossipScope;
import com.hellblazer.slp.ServiceType;
import com.hellblazer.slp.ServiceURL;
import com.hellblazer.slp.jmx.JmxServerListener;
import com.hellblazer.utils.fd.FailureDetectorFactory;
import com.hellblazer.utils.fd.impl.AdaptiveFailureDetectorFactory;

/**
 * @author hhildebrand
 * 
 */
public class FunctionalTest {

    private static final String DUMMY = "dummy";
    private static final String ID    = "id";
    private static final String TYPE  = "type";

    private static class Listener implements NotificationListener {
        final String         notificationType;
        final CountDownLatch latch;
        final BitSet         registered = new BitSet();

        public Listener(CountDownLatch latch, String notificationType) {
            this.latch = latch;
            this.notificationType = notificationType;
        }

        public void handleNotification(Notification n, Object handback) {
            MBeanServerNotification mbsn = (MBeanServerNotification) n;
            if (n.getType().equals(notificationType)) {
                if (DUMMY.equals(mbsn.getMBeanName().getKeyProperty(TYPE))) {
                    int id = Integer.parseInt(mbsn.getMBeanName().getKeyProperty(ID));
                    registered.flip(id);
                    latch.countDown();
                }
            }
        }
    }

    private List<JMXConnectorServer> servers = new ArrayList<>();
    private List<GossipScope>        scopes  = new ArrayList<>();
    private GossipScope              listeningScope;

    @After
    public void cleanup() {
        if (listeningScope != null) {
            listeningScope.stop();
        }
        for (JMXConnectorServer server : servers) {
            try {
                server.stop();
            } catch (IOException e) {
                // ignored
            }
        }
        for (GossipScope scope : scopes) {
            scope.stop();
        }
    }

    @Test
    public void end2end() throws Exception {
        String abstractServiceType = "yeOldTimeJmx";
        System.setProperty("java.rmi.server.randomIDs", "true");
        int members = 6;
        int maxseeds = 1;
        List<Gossip> gossips = createGossips(members, maxseeds);
        MBeanServer listeningMbs = MBeanServerFactory.newMBeanServer();
        CascadingService cascadingService = new CascadingService();
        String name = "com.hellblazer:type=CascadingService";
        listeningMbs.registerMBean(cascadingService, new ObjectName(name));
        Listener registrationListener = new Listener(
                                                     new CountDownLatch(members),
                                                     MBeanServerNotification.REGISTRATION_NOTIFICATION);
        listeningMbs.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME,
                                             registrationListener, null, null);
        Listener deregistrationListener = new Listener(
                                                       new CountDownLatch(
                                                                          members),
                                                       MBeanServerNotification.UNREGISTRATION_NOTIFICATION);
        listeningMbs.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME,
                                             deregistrationListener, null, null);
        listeningScope = new GossipScope(
                                         createCommunications(Arrays.asList(gossips.get(0).getLocalAddress())));
        listeningScope.start();
        JmxServerListener jmxListener = new JmxServerListener(cascadingService,
                                                              null, null,
                                                              listeningScope,
                                                              "/[%s/%s]");
        jmxListener.listenForService(String.format("service:%s:jmx:rmi",
                                                   abstractServiceType));
        List<MBeanServer> mbeanServers = new ArrayList<>();
        List<ServiceURL> serviceUrls = new ArrayList<>();
        List<UUID> registrations = new ArrayList<>();
        for (int i = 0; i < members; i++) {
            GossipScope scope = new GossipScope(gossips.get(i));
            scopes.add(scope);
            scope.start();
            MBeanServer mbs = MBeanServerFactory.newMBeanServer();
            mbeanServers.add(mbs);
            JMXConnectorServer server = JMXConnectorServerFactory.newJMXConnectorServer(new JMXServiceURL(
                                                                                                          "rmi",
                                                                                                          "localhost",
                                                                                                          allocatePort()),
                                                                                        new HashMap<String, Object>(),
                                                                                        mbs);
            servers.add(server);
            register(mbs, new DummyMBean(i));
            server.start();
            ServiceURL serviceUrl = constructServiceURL(abstractServiceType,
                                                        server.getAddress());
            serviceUrls.add(serviceUrl);
            registrations.add(scope.register(serviceUrl,
                                             new HashMap<String, String>()));

        }
        assertTrue("Did not register all the cascaded mbean servers",
                   registrationListener.latch.await(60, TimeUnit.SECONDS));
        assertEquals(members, registrationListener.registered.cardinality());

        System.out.println("All cascaded mbean servers correctly registered");

        for (int i = 0; i < members; i++) {
            scopes.get(i).unregister(registrations.get(i));
        }

        assertTrue("Did not deregister all the cascaded mbean servers",
                   deregistrationListener.latch.await(60, TimeUnit.SECONDS));
        assertEquals(members, deregistrationListener.registered.cardinality());

        System.out.println("All cascaded mbean servers correctly deregistered");
    }

    protected ServiceURL constructServiceURL(String serviceType,
                                             JMXServiceURL url)
                                                               throws MalformedURLException {
        StringBuilder builder = new StringBuilder();
        builder.append(ServiceType.SERVICE_PREFIX);
        builder.append(serviceType);
        builder.append(':');
        builder.append("jmx:");
        builder.append(url.getProtocol());
        builder.append("://");
        builder.append(url.getHost());
        builder.append(':');
        builder.append(url.getPort());
        builder.append(url.getURLPath());
        ServiceURL jmxServiceURL = new ServiceURL(builder.toString());
        return jmxServiceURL;
    }

    protected Gossip createCommunications(Collection<InetSocketAddress> seedHosts)
                                                                                  throws SocketException {
        ThreadFactory threadFactory = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        e.printStackTrace();
                    }
                });
                return t;
            }
        };
        UdpCommunications communications = new UdpCommunications(
                                                                 new InetSocketAddress(
                                                                                       "127.0.0.1",
                                                                                       0),
                                                                 Executors.newFixedThreadPool(2,
                                                                                              threadFactory));
        SystemView view = new SystemView(new Random(),
                                         communications.getLocalAddress(),
                                         seedHosts, 5000, 500000);
        FailureDetectorFactory fdFactory = new AdaptiveFailureDetectorFactory(
                                                                              0.9,
                                                                              100,
                                                                              0.8,
                                                                              12000,
                                                                              10,
                                                                              3000);
        Gossip gossip = new Gossip(communications, view, fdFactory,
                                   new Random(), 1, TimeUnit.SECONDS, 3);
        return gossip;
    }

    protected List<Gossip> createGossips(int membership, int maxSeeds)
                                                                      throws SocketException {
        Random entropy = new Random(666);
        List<Gossip> members = new ArrayList<Gossip>();
        Collection<InetSocketAddress> seedHosts = new ArrayList<InetSocketAddress>();
        for (int i = 0; i < membership; i++) {
            members.add(createCommunications(seedHosts));
            if (i == 0) { // always add first member
                seedHosts.add(members.get(0).getLocalAddress());
            } else if (seedHosts.size() < maxSeeds) {
                // add the new member with probability of 25%
                if (entropy.nextDouble() < 0.25D) {
                    seedHosts.add(members.get(i).getLocalAddress());
                }
            }
        }
        return members;
    }

    protected void register(MBeanServer mbs, DummyMBean dummy) {
        ObjectName name = dummyObjectName(dummy.getId());
        try {
            mbs.registerMBean(MBeanFactory.createMBean(dummy), name);
        } catch (InstanceAlreadyExistsException | MBeanRegistrationException
                | NotCompliantMBeanException e) {
            throw new IllegalStateException(
                                            String.format("Could not register %s as MBean %s",
                                                          getClass(), name), e);
        }
    }

    /**
     * @return
     */
    private ObjectName dummyObjectName(int id) {
        Hashtable<String, String> properties = new Hashtable<>();
        properties.put(TYPE, DUMMY);
        properties.put(ID, Integer.toString(id));
        ObjectName name;
        try {
            name = new ObjectName("test", properties);
        } catch (MalformedObjectNameException e) {
            throw new IllegalStateException("Could not form MBean object name",
                                            e);
        }
        return name;
    }
}
