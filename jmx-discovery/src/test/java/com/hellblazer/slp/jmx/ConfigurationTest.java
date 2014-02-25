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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.hellblazer.slp.jmx.JmxDiscoveryConfiguration;

/**
 * @author hhildebrand
 * 
 */
public class ConfigurationTest {

    @Test
    public void testYaml() throws Exception {
        JmxDiscoveryConfiguration config = JmxDiscoveryConfiguration.fromYaml(getClass().getResourceAsStream("testConfig.yml"));
        assertNotNull(config.discovery);
        assertEquals("com.hellblazer:type=CascadingService", config.name);
        List<String> serviceNames = config.serviceNames;
        assertEquals(3, serviceNames.size());
        assertTrue(serviceNames.contains("service:daemon:jmx:rmi"));
        assertTrue(serviceNames.contains("service:daemon:jmx:http"));
        assertTrue(serviceNames.contains("service:daemon:jmx:snmp"));
    }
}
