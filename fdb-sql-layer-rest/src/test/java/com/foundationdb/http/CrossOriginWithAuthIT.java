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

import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CrossOriginWithAuthIT extends CrossOriginITBase
{
    private static final String ROLE = "rest-user";
    private static final String USER = "u";
    private static final String PASS = "p";


    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .require(SecurityService.class);
    }

    @Override
    protected Map<String,String> startupConfigProperties() {
        Map<String,String> config = new HashMap<>(super.startupConfigProperties());
        config.put("fdbsql.http.login", "basic");
        return config;
    }

    @Override
    protected String getUserInfo() {
        return USER + ":" + PASS;
    }

    @Before
    public final void createUser() {
        SecurityService securityService = securityService();
        securityService.addRole(ROLE);
        securityService.addUser(USER, PASS, Arrays.asList(ROLE));
    }

    @After
    public final void clearUser() {
        securityService().clearAll(session());
    }
}
