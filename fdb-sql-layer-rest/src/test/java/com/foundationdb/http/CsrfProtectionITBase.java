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
import com.foundationdb.server.test.it.ITBase;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;

public abstract class CsrfProtectionITBase extends RestServiceITBase
{

    protected abstract String getUserInfo();

    @Override
    protected Map<String,String> startupConfigProperties() {
        Map<String,String> config = new HashMap<>(super.startupConfigProperties());
        config.put("fdbsql.http.csrf_protection.allowed_referers", "http://somewhere.com,https://coolest.site.edu:4320");
        return config;
    }

    @Test
    public void requestBlockedWithMissingReferer() throws Exception{
        HttpUriRequest request = new HttpPut(defaultURI());

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
    }

    @Test
    public void requestBlockedWithEmptyReferer() throws Exception{
        HttpUriRequest request = new HttpPut(defaultURI());
        request.setHeader("Referer","");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
    }

    @Test
    public void getBlockedWithBadHost() throws Exception{
        // Although we let blank & empty referers through for get requests, there is no benefit to
        // letting incorrect referers through, so those are always blocked.
        HttpUriRequest request = new HttpGet(defaultURI());
        request.setHeader("Referer", "https://coolest.site.edu.fake.com:4320");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
    }

    @Test
    public void postBlockedWithBadHost() throws Exception{
        HttpUriRequest request = new HttpPost(defaultURI());
        request.setHeader("Referer", "https://coolest.site.edu.fake.com:4320");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
    }

    @Test
    public void putBlockedWithMissingReferer() throws Exception{
        HttpUriRequest request = new HttpPut(defaultURI());

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
    }

    @Test
    public void putBlockedWithBlankReferer() throws Exception{
        HttpUriRequest request = new HttpPut(defaultURI());
        request.setHeader("Referer","");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
    }

    @Test
    public void getBlockedHasJsonResponse() throws Exception{
        HttpUriRequest request = new HttpGet(defaultURI());
        request.setHeader("Referer", "https://coolest.site.edu.fake.com:4320");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
        assertThat("body", EntityUtils.toString(response.getEntity()), containsString("Referer"));
    }

    @Test
    public void putBlockedHasJsonResponse() throws Exception{
        HttpUriRequest request = new HttpPut(defaultURI());

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
        assertThat("body", EntityUtils.toString(response.getEntity()), containsString("Referer"));
    }

    @Test
    public void patchBlockedHasJsonResponse() throws Exception{
        HttpUriRequest request = new HttpPatch(defaultURI());

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
        assertThat("body", EntityUtils.toString(response.getEntity()), containsString("Referer"));
    }

    @Test
    public void postBlockedHasJsonResponse() throws Exception{
        HttpUriRequest request = new HttpPost(defaultURI());

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
        assertThat("body", EntityUtils.toString(response.getEntity()), containsString("Referer"));
    }

    @Test
    public void deleteBlockedHasJsonResponse() throws Exception{
        HttpUriRequest request = new HttpDelete(defaultURI());

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_FORBIDDEN, response.getStatusLine().getStatusCode());
        assertThat("reason", response.getStatusLine().getReasonPhrase(), containsString("Referer"));
        assertThat("body", EntityUtils.toString(response.getEntity()), containsString("Referer"));
    }

    @Test
    public void getAllowedWithNoReferer() throws Exception{
        // Since GET requests don't have side effects, the cross-origin header will prevent
        // third-party javascript from viewing the result, meaning that we can allow this through.
        HttpUriRequest request = new HttpGet(defaultURI());

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void getAllowedWithBlankReferer() throws Exception{
        // Since GET requests don't have side effects, the cross-origin header will prevent
        // third-party javascript from viewing the result, meaning that we can allow this through.
        HttpUriRequest request = new HttpGet(defaultURI());
        request.setHeader("Referer","");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void getAllowed1() throws Exception{
        HttpUriRequest request = new HttpGet(defaultURI());
        request.setHeader("Referer","http://somewhere.com");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void getAllowed2() throws Exception{
        HttpUriRequest request = new HttpGet(defaultURI());
        request.setHeader("Referer","https://coolest.site.edu:4320");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void postAllowed() throws Exception{
        HttpPost request = new HttpPost(defaultURI());
        request.setHeader("Referer","http://somewhere.com");
        request.setHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity("{\"id\": \"1\"}"));

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void putAllowed() throws Exception{
        HttpPut request = new HttpPut(defaultURI("/1"));
        request.setHeader("Referer","http://somewhere.com");
        request.setHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity("{\"id\": \"1\"}"));

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    }

    @Test
    public void deleteAllowed() throws Exception{
        HttpUriRequest request = new HttpDelete(defaultURI("/1"));
        request.setHeader("Referer","http://somewhere.com");

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_NO_CONTENT, response.getStatusLine().getStatusCode());
    }
}
