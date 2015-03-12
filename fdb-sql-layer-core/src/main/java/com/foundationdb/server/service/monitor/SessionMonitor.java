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

import java.util.List;

public interface SessionMonitor {
    /** The id of the session being monitored. */
    int getSessionId();

    /** The id of the session that called this one or -1 if none. */
    int getCallerSessionId();

    /** What kind of server is this a session for? */
    String getServerType();

    /** The remote IP address of a network connection or <code>null</code>. */
    String getRemoteAddress();

    /** The system time at which this session started. */
    long getStartTimeMillis();
    
    /** The number of queries executed. */
    long getStatementCount();

    /** Get number of statements executes of a given type */
    long getCount(StatementTypes type);
    
    /** Count an statement execution */
    void countEvent(StatementTypes type);
    
    /** The SQL of the current / last statement. */
    String getCurrentStatement();    

    /** The time at which the current statement began executing or <code>-1</code>. */
    long getCurrentStatementStartTimeMillis();

    /** The time at which the current statement completed or <code>-1</code>. */
    long getCurrentStatementEndTimeMillis();

    /** The time for which current statement ran. */
    long getCurrentStatementDurationMillis();

    /** The prepared statement name if current statement was prepared. */
    String getCurrentStatementPreparedName();

    /** The number of rows returned / affected by the last statement
     * or <code>-1</code> if unknown, not applicable or in
     * progress. 
     */
    int getRowsProcessed();

    /** The current stage of the session. */
    MonitorStage getCurrentStage();
    
    /** The time in nanoseconds last spent in the given stage. */
    long getLastTimeStageNanos(MonitorStage stage);

    /** The total time in nanoseconds spent in the given stage. */
    long getTotalTimeStageNanos(MonitorStage stage);

    /** Get total time in nanoseconds not spent idle. */
    long getNonIdleTimeNanos();

    /** Get any open cursors. */
    List<CursorMonitor> getCursors();

    /** Get any prepared statements. */
    List<PreparedStatementMonitor> getPreparedStatements();
    
    /** Get the user monitor for this session */
    UserMonitor getUserMonitor();

    public void addSessionEventListener(SessionEventListener listener);
    public void removeSessionEventListener (SessionEventListener listener);
    
    
    public enum StatementTypes {
        STATEMENT,
        FAILED ,
        FROM_CACHE,
        LOGGED,
        
        CALL_STMT,
        DDL_STMT,
        DML_STMT,
        SELECT,
        OTHER_STMT;
    }
}
