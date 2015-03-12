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

import com.foundationdb.rest.RestService;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public abstract class RestServiceITBase extends ITBase {
    protected static final String SCHEMA = "test";
    protected static final String TABLE = "t";
    protected int port;
    protected String restContext;
    protected HttpResponse response;
    protected CloseableHttpClient client;

    protected abstract String getUserInfo();

    @Before
    public final void setUp() {
        port = serviceManager().getServiceByClass(HttpConductor.class).getPort();
        restContext = serviceManager().getServiceByClass(RestService.class).getContextPath();
        createTable(SCHEMA, TABLE, "id int not null primary key");
        client = HttpClientBuilder.create().build();
    }

    @After
    public final void tearDown() throws IOException {
        if(response != null) {
            EntityUtils.consume(response.getEntity());
        }
        if(client != null) {
            client.close();
        }
    }


    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .require(RestService.class);
    }

    protected static String headerValue(HttpResponse response, String key) {
        Header header = response.getFirstHeader(key);
        return (header != null) ? header.getValue() : null;
    }

    protected String entityEndpoint() {
        return String.format("%s/entity/%s.%s", restContext, SCHEMA, TABLE);
    }

    protected URI defaultURI() throws URISyntaxException {
        return defaultURI("");
    }

    protected URI defaultURI(String entitySuffix) throws URISyntaxException {
        return new URI("http", getUserInfo(), "localhost", port, entityEndpoint() + entitySuffix, null, null);
    }
}
