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
package com.foundationdb.server.service.security;

import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.sql.embedded.EmbeddedJDBCService;
import com.foundationdb.sql.embedded.EmbeddedJDBCServiceImpl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class SecurityServiceITBase extends ITBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
            .bindAndRequire(EmbeddedJDBCService.class, EmbeddedJDBCServiceImpl.class)
            .bindAndRequire(SecurityService.class, SecurityServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fdbsql.restrict_user_schema", "true");
        return properties;
    }

    @Before
    public void setUp() {
        int t1 = createTable("user1", "utable", "id int primary key not null");
        int t2 = createTable("user2", "utable", "id int primary key not null");        
        writeRow(t1, 1L);
        writeRow(t2, 2L);

        createTable("user1", "utable2", "id int primary key not null");
        createView("user1", "v1", "SELECT * FROM utable2");
        createIndex("user1", "utable", "ind", "id");
        createSequence("user1", "s1", "START WITH 1 INCREMENT BY 1 NO CYCLE");
        createFromDDL("user1", "CREATE PROCEDURE user1.proc1(IN x INT) LANGUAGE javascript PARAMETER STYLE variables AS '[1]'");
        
        SecurityService securityService = securityService();
        securityService.addRole("rest-user");
        securityService.addRole("admin");
        securityService.addRole("standard");
        securityService.addUser("user1", "password", Arrays.asList("rest-user"));
        securityService.addUser("user2", "password", Arrays.asList("standard"));
        securityService.addUser("akiban", "topsecret", Arrays.asList("rest-user", "admin"));
    }

    @After
    public void cleanUp() {
        securityService().clearAll(session());
    }

    @Test
    public void getUser() {
        SecurityService securityService = securityService();
        User user = securityService.getUser("user1");
        assertNotNull("user found", user);
        assertTrue("user has role", user.hasRole("rest-user"));
        assertFalse("user does not have role", user.hasRole("admin"));
        assertEquals("users roles", "[rest-user]", user.getRoles().toString());
        assertEquals("user password basic", "MD5:5F4DCC3B5AA765D61D8327DEB882CF99", user.getBasicPassword());
        assertEquals("user password digest", "MD5:BDAA29D9E7DCE23995599F595AA8832D", user.getDigestPassword());
    }

    @Test
    public void authenticate() {
        assertEquals("user1", securityService().authenticateLocal(session(), "user1", "password").getName());
    }

}
