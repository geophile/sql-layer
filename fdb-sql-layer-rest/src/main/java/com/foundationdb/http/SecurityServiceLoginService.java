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

import java.util.List;

import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.security.User;
import com.foundationdb.util.ArgumentValidation;
import org.eclipse.jetty.security.MappedLoginService;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.security.Credential;

public class SecurityServiceLoginService extends MappedLoginService
{

    public enum CredentialType { BASIC, DIGEST }

    private final SecurityService securityService;
    private final CredentialType credentialType;
    private final long cacheMillis;
    private volatile long lastCachePurge;
    private String realm;

    public SecurityServiceLoginService(SecurityService securityService, CredentialType credentialType, int cacheSeconds,
                                       String realm) {
        ArgumentValidation.isGTE("cacheSeconds", cacheSeconds, 0);
        if(credentialType != CredentialType.BASIC && credentialType != CredentialType.DIGEST) {
            throw new IllegalArgumentException("Unknown credential: " + credentialType);
        }
        this.securityService = securityService;
        this.credentialType = credentialType;
        this.cacheMillis = cacheSeconds * 1000;
        this.realm = realm;
    }

    @Override
    public UserIdentity login(String username, Object credentials) {
        long now = System.currentTimeMillis();
        if((now - lastCachePurge) > cacheMillis) {
            super._users.clear();
            lastCachePurge = now;
        }
        return super.login(username, credentials);
    }

    @Override
    public String getName() {
        return realm;
    }

    @Override
    protected void loadUsers() {
    }

    @Override
    protected UserIdentity loadUser(String username) {
        User user = securityService.getUser(username);
        if(user != null) {
            String password = (credentialType == CredentialType.BASIC) ? user.getBasicPassword() : user.getDigestPassword();
            List<String> roles = user.getRoles();
            return putUser(username, Credential.getCredential(password), roles.toArray(new String[roles.size()]));
        }
        return null;
    }
}
