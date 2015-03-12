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

import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;

@RunWith(SelectedParameterizedRunner.class)
public class AuthRealmIT extends RestServiceITBase {
    private static final String LOGIN_PROPERTY = "fdbsql.http.login";
    private static final String REALM_PROPERTY = "fdbsql.security.realm";

    private static final String ROLE = "rest-user";
    private static final String USER = "u";
    private static final String PASS = "p";

    private final String authType;
    private final String realm;
    private String expectedRealm;


    @Parameterized.Parameters(name="{0} auth with realm={1}")
    public static Iterable<Object[]> queries() throws Exception {
        // null in list below means use system defaults
        return Arrays.asList(
                new Object[] {"basic", null},
                new Object[] {"basic", ""},
                new Object[] {"basic", "My realm"},
                new Object[] {"digest", null},
                new Object[] {"digest", ""},
                new Object[] {"digest", "My realm"});
    }

    public AuthRealmIT(String authType, String realm) {
        this.authType = authType;
        this.realm = realm;
    }

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .require(SecurityService.class);
    }

    @Override
    protected Map<String,String> startupConfigProperties() {
        Map<String,String> config = new HashMap<>(super.startupConfigProperties());
        if (authType != null) {
            config.put(LOGIN_PROPERTY, authType);
        }
        if (realm != null) {
            config.put(REALM_PROPERTY, realm);
        }

        return config;
    }

    @Override
    protected String getUserInfo() {
        return USER + ":" + PASS;
    }

    @Before
    public final void createUser() {
        if(realm == null) {
            expectedRealm = configService().getProperty(REALM_PROPERTY);
        } else {
            expectedRealm = realm;
        }
        SecurityService securityService = securityService();
        securityService.addRole(ROLE);
        securityService.addUser(USER, PASS, Arrays.asList(ROLE));
    }

    @After
    public final void clearUser() {
        securityService().clearAll(session());
    }

    @Test
    public void testRealmIsSetInHeader() throws Exception{
        URI uri = new URI("http", null, "localhost", port, entityEndpoint() + "", null, null);
        HttpUriRequest request = new HttpGet(uri);

        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_UNAUTHORIZED, response.getStatusLine().getStatusCode());
            assertThat("reason", headerValue(response, "WWW-Authenticate"),
                    containsString("realm=\"" + expectedRealm + "\""));
    }

    @Test
    public void testGet() throws Exception {
        HttpUriRequest request = new HttpGet(defaultURI());
        response = client.execute(request);
        assertEquals("status", HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        assertThat("response", EntityUtils.toString(response.getEntity()), is(not("")));
    }
}
