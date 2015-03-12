/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2009-2015 FoundationDB, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.foundationdb.http;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.rest.RestService;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.sql.embedded.EmbeddedJDBCService;

public class HttpMonitorVerifyIT extends ITBase {
    
    private static final Logger LOG = LoggerFactory.getLogger(HttpMonitorVerifyIT.class);

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
            .require(SecurityService.class)
            .require(EmbeddedJDBCService.class)
            .require(RestService.class);
    }

    @Before
    public void setUp() {
        SecurityService securityService = securityService();
        securityService.addRole("rest-user");
        securityService.addUser("user1", "password", Arrays.asList("rest-user"));
    }

    protected SecurityService securityService() {
        return serviceManager().getServiceByClass(SecurityService.class);
    }
    
    protected HttpConductor httpConductor() {
        return serviceManager().getServiceByClass(HttpConductor.class);
    }
    
    protected MonitorService monitorService () {
        return serviceManager().getServiceByClass(MonitorService.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fdbsql.http.login", "basic"); // "digest"
        properties.put("fdbsql.restrict_user_schema", "true");
        return properties;
    }

    private static int openRestURL(HttpClient client, String userInfo, int port, String path) throws Exception {
        URI uri = new URI("http", userInfo, "localhost", port, path, null, null);
        HttpGet get = new HttpGet(uri);
        HttpResponse response = client.execute(get);
        int code = response.getStatusLine().getStatusCode();
        EntityUtils.consume(response.getEntity());
        return code;
    }

    @Test
    public void runTest () throws Exception {
        MonitorService monitor = monitorService();
        
        CloseableHttpClient client = HttpClientBuilder.create().build();
        openRestURL(client, "user1:password", httpConductor().getPort(), "/version");
        assertEquals(monitor.getSessionMonitors().size(), 1);
        
        client.close();
    }

}
