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
package com.foundationdb.server.service.monitor;

import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;
import com.foundationdb.server.service.session.Session;

import java.util.Collection;
import java.util.Map;

public interface MonitorService {
    /** Register the given server monitor. */
    void registerServerMonitor(ServerMonitor serverMonitor);

    /** Deregister the given server monitor. */
    void deregisterServerMonitor(ServerMonitor serverMonitor);

    /** Get all registered server monitors. */
    Map<String,ServerMonitor> getServerMonitors();

    /** Allocate a unique id for a new session. */
    int allocateSessionId();

    /** Register the given session monitor. */
    void registerSessionMonitor(SessionMonitor sessionMonitor, Session session);

    /** Deregister the given session monitor. */
    void deregisterSessionMonitor(SessionMonitor sessionMonitor, Session session);

    /** Get the session monitor for the given session id. */
    SessionMonitor getSessionMonitor(int sessionId);

    /** Get the session monitor for the given session. */
    SessionMonitor getSessionMonitor(Session session);

    /** Get all registered session monitors. */
    Collection<SessionMonitor> getSessionMonitors();

    /** Log the given SQL to the query log. */
    void logQuery(int sessionId, String sqlText,
                  long duration, int rowsProcessed, Throwable failure);

    /** Log last statement from given monitor. */
    void logQuery(SessionMonitor sessionMonitor, Throwable failure);
    
    /** Register the given User monitor. */
    void registerUserMonitor (UserMonitor userMonitor);
    
    /** Deregister the given user montitor. */
    void deregisterUserMonitor (UserMonitor userMonitor);
    
    /** Deregister the montor for the given user */
    void deregisterUserMonitor (String userName);
    
    /** Get the user monitor for the given user name. */
    UserMonitor getUserMonitor(String userName);
    
    /** Get the user monitor for the session user */
    UserMonitor getUserMonitor(Session session);
    
    /** Get all the user monitors. */
    Collection<UserMonitor> getUserMonitors();

    /** Get statisics counter for statement types */
    long getCount(StatementTypes type);
    

    //
    // Query Log Control
    //

    /** Is query logging turned on? */
    boolean isQueryLogEnabled();

    /** Turn query log on or off. */
    void setQueryLogEnabled(boolean enabled);

    /** Set the filename for the query log. */
    void setQueryLogFileName(String fileName);

    /** Get the current query log or an empty string if unset. */
    String getQueryLogFileName();

    /** Set minimum number of milliseconds for a query to be logged or {@code -1} if no limit. */
    void setQueryLogThresholdMillis(long threshold);

    /** Get minimum number of milliseconds for a query to be logged or {@code -1} if no limit. */
    long getQueryLogThresholdMillis();
}
