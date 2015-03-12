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

public interface SecurityService
{
    /** This role allows access to all data. */
    public static final String ADMIN_ROLE = "admin";

    /** The current {@link Principal} for the {@link Session}. */
    public static final Session.Key<Principal> SESSION_PRINCIPAL_KEY = 
        Session.Key.named("SECURITY_PRINCIPAL");

    /** The current roles for the {@link Session}. */
    public static final Session.Key<Collection<String>> SESSION_ROLES_KEY = 
        Session.Key.named("SECURITY_ROLES");

    /** Authenticate user using local security database and set into {@link Session}.
     * @return the logged in {@link Principal}.
     * Throws an error if authentication fails.
     */
    public Principal authenticateLocal(Session session, String name, String password);

    /** Authenticate user using local security database and set in {@link Session}.
     * @param salt a salt to use when hashing the password
     */
    public Principal authenticateLocal(Session session, String name, String password,
                                       byte[] salt);

    /** If this {@link Session} is authenticated, does it have access to the given schema?
     *
     * NOTE: If authentication is enabled, caller must not call this (that is, allow
     * any queries) without authentication, since that is indistinguishable from
     * authentication disabled.
     *
     * @see com.foundationdb.sql.pg.PostgresServerConnection#authenticationOkay
     */
    public boolean isAccessible(Session session, String schema);
    
    /** Does the given {@link Principal} have access to the given scheam?
     * NOTE: If authentication is enabled, caller must not call this (that is, allow
     * any queries) with <code>null</code>, since that is indistinguishable from
     * authentication disabled.
     *
     * @see com.foundationdb.http.HttpConductorImpl.AuthenticationType
     */
    public boolean isAccessible(Principal user, boolean inAdminRole, String schema);

    /** If this {@link Session} is authenticated, does it administrative access?
     */
    public boolean hasRestrictedAccess(Session session);

    /** Set {@link Session}'s authentication directly. */
    public void setAuthenticated(Session session, Principal user, boolean inAdminRole);

    /** Authenticate user using given JAAS configuration and set in {@link Session}.
     * @param configName name of the JAAS configuration to use
     * @param roleClasses list of {@link Principal} classes that represent roles or <code>null</code> to get from corresponding user in local database.
     */
    public Principal authenticateJaas(Session session, String name, String password,
                                      String configName, Class<? extends Principal> userClass, Collection<Class<? extends Principal>> roleClasses);

    public void addRole(String name);
    public void deleteRole(String name);
    public User getUser(String name);
    public User addUser(String name, String password, Collection<String> roles);
    public void deleteUser(String name);
    public void changeUserPassword(String name, String password);
    public void clearAll(Session session);
}
