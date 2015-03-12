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

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class HttpThreadedLoginIT extends ITBase
{
    private static final Logger LOG = LoggerFactory.getLogger(HttpThreadedLoginIT.class);

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
            .require(RestService.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fdbsql.http.login", "basic");
        return properties;
    }

    private static void checkOpenRestURL(String userInfo, int port, String path, int expectedCode) throws Exception {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        URI uri = new URI("http", userInfo, "localhost", port, path, null, null);
        HttpGet get = new HttpGet(uri);
        HttpResponse response = client.execute(get);
        int code = response.getStatusLine().getStatusCode();
        String content = EntityUtils.toString(response.getEntity());
        client.close();
        if(code != expectedCode) {
            fail(String.format("expected code %d but got %d for userInfo=%s and content=%s",
                               expectedCode, code, userInfo, content));
        }
    }

    @Test
    public void oneThread() throws InterruptedException {
        run(1);
    }

    @Test
    public void fiveThreads() throws InterruptedException {
        run(5);
    }

    @Test
    public void tenThreads() throws InterruptedException {
        run(10);
    }

    @Test
    public void twentyThreads() throws InterruptedException {
        run(20);
    }


    private void run(int count) throws InterruptedException {
        final int port = serviceManager().getServiceByClass(HttpConductor.class).getPort();
        final String context = serviceManager().getServiceByClass(RestService.class).getContextPath();
        final String path = context + "/version";
        final UncaughtHandler uncaughtHandler = new UncaughtHandler();

        Thread threads[] = new Thread[count];
        for(int i = 0; i < count; ++i) {
            threads[i] = new Thread(new TestRunnable(port, path, i), "Thread"+i);
            threads[i].setUncaughtExceptionHandler(uncaughtHandler);
        }
        for(int i = 0; i < count; ++i) {
            threads[i].start();
        }
        for(int i = 0; i < count; ++i) {
            threads[i].join();
        }

        for(Throwable entry : uncaughtHandler.uncaught.values()) {
            LOG.error("Uncaught exception", entry);
        }
        assertEquals("uncaught exception count", 0, uncaughtHandler.uncaught.size());
    }

    private static class UncaughtHandler implements Thread.UncaughtExceptionHandler {
        public final Map<Thread,Throwable> uncaught = Collections.synchronizedMap(new HashMap<Thread,Throwable>());

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            uncaught.put(t, e);
        }
    }

    private static class TestRunnable implements Runnable {
        private final int port;
        private final String url;
        private final int userNum;

        public TestRunnable(int port, String url, int userNum) {
            this.port = port;
            this.url = url;
            this.userNum = userNum;
        }

        @Override
        public void run() {
            String userInfo = String.format("user_%d:password", userNum);
            try {
                checkOpenRestURL(userInfo, port, url, HttpStatus.SC_UNAUTHORIZED);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
