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
import com.foundationdb.rest.RestServiceImpl;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 *
 * Simple Request:
 *     Method: GET, POST, HEAD
 *     Headers: Origin = foo
 *              [other "simple" headers]
 * Simple Response:
 *     Headers: Access-Control-Allow-Origin = foo
 *              Access-Control-Allow-Credentials = true (if enabled)
 *
 * Non-Simple: Simple + other methods, headers
 *
 * PreFlight Request:
 *     Method: OPTIONS
 *     Headers: Origin = foo
 *              Access-Control-Request-Method = GET
 *              Note: No auth
 * PreFlight Response:
 *     Headers: Access-Control-Allow-Origin = foo
 *              Access-Control-Allow-Methods = ...
 *              Access-Control-Allow-Headers = ...
 */
public abstract class CrossOriginITBase extends RestServiceITBase {
    private static final String ORIGIN = "http://example.com";
    private static final String ALLOWED_METHODS = "GET,POST,PUT";
    private static final String HEADER_ALLOW_ORIGIN = "Access-Control-Allow-Origin";


    @Override
    protected Map<String,String> startupConfigProperties() {
        Map<String,String> config = new HashMap<>(super.startupConfigProperties());
        config.put("fdbsql.http.cross_origin.enabled", "true");
        config.put("fdbsql.http.cross_origin.allowed_methods", ALLOWED_METHODS);
        config.put("fdbsql.http.cross_origin.allowed_origins", "*");
        config.put("fdbsql.http.csrf_protection.type", "none");
        return config;
    }


    @Test
    public void preFlightAllowedMethod() throws Exception {
        URI uri = new URI("http", null /*preflight requires no auth*/, "localhost", port, entityEndpoint(), null, null);

        HttpUriRequest request = new HttpOptions(uri);
        request.setHeader("Origin", ORIGIN);
        request.setHeader("Access-Control-Request-Method", "PUT");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        assertEquals("Allow-Origin", ORIGIN, headerValue(response, HEADER_ALLOW_ORIGIN));
    }

    @Test
    public void preFlightDisallowedMethod() throws Exception {
        URI uri = new URI("http", null /*preflight requires no auth*/, "localhost", port, entityEndpoint(), null, null);

        HttpUriRequest request = new HttpOptions(uri);
        request.setHeader("Origin", ORIGIN);
        request.setHeader("Access-Control-Request-Method", "DELETE");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        assertEquals("Allow-Origin", null, headerValue(response, HEADER_ALLOW_ORIGIN));
    }

    @Test
    public void simpleMethod() throws Exception {
        HttpUriRequest request = new HttpGet(defaultURI());
        request.setHeader("Origin", ORIGIN);

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        assertEquals("Allow-Origin", ORIGIN, headerValue(response, HEADER_ALLOW_ORIGIN));
    }

    @Test
    public void nonSimpleMethod() throws Exception {
        HttpUriRequest request = new HttpDelete(defaultURI("/1"));
        request.setHeader("Origin", ORIGIN);

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_NO_CONTENT, response.getStatusLine().getStatusCode());
        assertEquals("Allow-Origin", ORIGIN, headerValue(response, HEADER_ALLOW_ORIGIN));
    }
}
