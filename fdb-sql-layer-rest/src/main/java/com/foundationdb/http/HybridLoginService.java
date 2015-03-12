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
import com.foundationdb.server.service.security.User;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;
import java.security.Principal;
import javax.security.auth.Subject;

public class HybridLoginService extends AbstractLifeCycle implements LoginService
{
    private final LoginService delegate;
    private final SecurityService securityService;

    public HybridLoginService(LoginService delegate, SecurityService securityService) {
        this.delegate = delegate;
        this.securityService = securityService;
    }

    /* AbstractLifeCycle */

    @Override
    protected void doStart() throws Exception {
        if (delegate instanceof LifeCycle)
            ((LifeCycle)delegate).start();
    }

    @Override
    protected void doStop() throws Exception {
        if (delegate instanceof LifeCycle)
            ((LifeCycle)delegate).stop();
    }

    /* LoginService */

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public UserIdentity login(String username, Object credentials) {
        UserIdentity inner = delegate.login(username, credentials);
        if (inner == null)
            return null;
        String userName = inner.getUserPrincipal().getName();
        int at = userName.indexOf('@');
        if (at >= 0) userName = userName.substring(0, at);
        User user = securityService.getUser(userName);
        if (user == null)
            return inner;
        else
            return new WrappedUserIdentity(inner, user);
    }

    @Override
    public boolean validate(UserIdentity user) {
        return delegate.validate(unwrap(user));
    }

    @Override
    public IdentityService getIdentityService() {
        return delegate.getIdentityService();
    }

    @Override
    public void setIdentityService(IdentityService service) {
        delegate.setIdentityService(service);
    }

    @Override
    public void logout(UserIdentity user) {
        delegate.logout(unwrap(user));
    }

    protected static class WrappedUserIdentity implements UserIdentity {
        private final UserIdentity delegate;
        private final User user;

        public WrappedUserIdentity(UserIdentity delegate, User user) {
            this.delegate = delegate;
            this.user = user;
        }

        @Override
        public Subject getSubject() {
            return delegate.getSubject();
        }

        @Override
        public Principal getUserPrincipal() {
            return delegate.getUserPrincipal();
        }

        @Override
        public boolean isUserInRole(String role, UserIdentity.Scope scope) {
            return delegate.isUserInRole(role, scope) || user.hasRole(role);
        }
    }

    protected UserIdentity unwrap(UserIdentity user) {
        if (user instanceof WrappedUserIdentity)
            return ((WrappedUserIdentity)user).delegate;
        else
            return user;
    }
}
