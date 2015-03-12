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

import com.foundationdb.server.service.session.Session;

import java.security.Principal;
import java.util.Collection;

public class DummySecurityService implements SecurityService {
    @Override
    public Principal authenticateLocal(Session session, String name, String password) {
        return null;
    }

    @Override
    public Principal authenticateLocal(Session session, String name, String password, byte[] salt) {
        return null;
    }

    @Override
    public Principal authenticateJaas(Session session, String name, String password,
                                      String configName, Class<? extends Principal> userClass, Collection<Class<? extends Principal>> roleClasses) {
        return null;
    }

    @Override
    public boolean isAccessible(Session session, String schema) {
        return true;
    }

    @Override
    public boolean isAccessible(java.security.Principal user, boolean inAdminRole, String schema) {
        return true;
    }

    @Override
    public boolean hasRestrictedAccess(Session session) {
        return true;
    }

    @Override
    public void setAuthenticated(Session session, Principal user, boolean inAdminRole) {
    }

    @Override
    public void addRole(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRole(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public User getUser(String name) {
        return null;
    }

    @Override
    public User addUser(String name, String password, java.util.Collection<String> roles) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteUser(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeUserPassword(String name, String password) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearAll(Session session) {
    }
}
