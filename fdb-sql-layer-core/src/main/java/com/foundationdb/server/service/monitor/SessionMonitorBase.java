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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class SessionMonitorBase implements SessionMonitor {
    private final int sessionID;
    private final long startTimeMillis;
    private MonitorStage currentStage;
    private long currentStageStartNanos;
    private long[] lastNanos = new long[MonitorStage.values().length];
    private long[] totalNanos = new long[MonitorStage.values().length];
    private String currentStatement, currentStatementPreparedName;
    private long currentStatementStartTime = -1;
    private long currentStatementEndTime = -1;
    private int rowsProcessed = 0;
    private UserMonitor user = null; 
    
    private long[] statementCounters = new long[StatementTypes.values().length];
    // TODO: In theory this needs to be a thread-safe data structure for adding/removing listeners
    // In practice, this is only executed in one thread.
    private Set<SessionEventListener> eventListeners = new HashSet<>();
    
    protected SessionMonitorBase(int sessionID) {
        this.sessionID = sessionID;
        this.startTimeMillis = System.currentTimeMillis();
    }


    /* SessionMonitorBase methods */

    public void startStatement(String statement) {
        startStatement(statement, System.currentTimeMillis());
    }

    public void startStatement(String statement, long startTime) {
        startStatement(statement, null, startTime);
    }

    public void startStatement(String statement, String preparedName) {
        startStatement(statement, preparedName, System.currentTimeMillis());
    }

    public void startStatement(String statement, String preparedName, long startTime) {
        countEvent(StatementTypes.STATEMENT);
        currentStatement = statement;
        currentStatementPreparedName = preparedName;
        currentStatementStartTime = startTime;
        currentStatementEndTime = -1;
        rowsProcessed = -1;
    }

    public void endStatement() {
        endStatement(-1);
    }

    public void endStatement(int rowsProcessed) {
        currentStatementEndTime = System.currentTimeMillis();
        this.rowsProcessed = rowsProcessed;
        if (user != null) {
            user.statementRun();
        }
    }
    
    public void countEvent(StatementTypes type) {
        statementCounters[type.ordinal()]++;
        for (SessionEventListener listen : eventListeners) {
            listen.countEvent(type);
        }
    }
    
    public void failStatement(Throwable failure) {
        countEvent(StatementTypes.FAILED);
        endStatement();
    }

    // Caller can sequence all stages and avoid any gaps at the cost of more complicated
    // exception handling, or just enter & leave and accept a tiny bit
    // unaccounted for.
    public void enterStage(MonitorStage stage) {
        long now = System.nanoTime();
        if (currentStage != null) {
            long delta = now - currentStageStartNanos;
            lastNanos[currentStage.ordinal()] = delta;
            totalNanos[currentStage.ordinal()] += delta;
        }
        currentStage = stage;
        currentStageStartNanos = now;
    }

    public void leaveStage() {
        enterStage(null);
    }


    /* SessionMonitor */

    @Override
    public int getSessionId() {
        return sessionID;
    }

    @Override
    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    @Override
    public long getStatementCount() {
        return statementCounters[StatementTypes.STATEMENT.ordinal()];
    }
    
    @Override 
    public long getCount(StatementTypes type) {
        return statementCounters[type.ordinal()];
    }
    
    @Override
    public String getCurrentStatement() {
        return currentStatement;
    }

    @Override
    public String getCurrentStatementPreparedName() {
        return currentStatementPreparedName;
    }

    @Override
    public long getCurrentStatementStartTimeMillis() {
        return currentStatementStartTime;
    }

    @Override
    public long getCurrentStatementEndTimeMillis() {
        return currentStatementEndTime;
    }

    @Override
    public long getCurrentStatementDurationMillis() {
        if (currentStatementEndTime < 0)
            return -1;
        else
            return currentStatementEndTime - currentStatementStartTime;
    }

    @Override
    public int getRowsProcessed() {
        return rowsProcessed;
    }

    @Override
    public MonitorStage getCurrentStage() {
        return currentStage;
    }

    @Override
    public long getLastTimeStageNanos(MonitorStage stage) {
        return lastNanos[stage.ordinal()];
    }

    @Override
    public long getTotalTimeStageNanos(MonitorStage stage) {
        return lastNanos[stage.ordinal()];
    }

    @Override
    public long getNonIdleTimeNanos() {
        long total = 0;
        for (int i = 1; i < totalNanos.length; i++) {
            total += totalNanos[i];
        }
        return total;
    }

    @Override
    public void addSessionEventListener(SessionEventListener listener) {
        eventListeners.add(listener);
    }
    
    @Override
    public void removeSessionEventListener (SessionEventListener listener) {
        eventListeners.remove(listener);
    }

    
    public List<CursorMonitor> getCursors() {
        return Collections.emptyList();
    }

    public List<PreparedStatementMonitor> getPreparedStatements() {
        return Collections.emptyList();
    }
    
    public void setUserMonitor(UserMonitor monitor) {
        this.user = monitor;
    }
    
    public UserMonitor getUserMonitor() {
        return this.user;
    }
}
