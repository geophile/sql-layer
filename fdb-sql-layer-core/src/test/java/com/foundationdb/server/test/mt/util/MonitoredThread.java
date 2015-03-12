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
package com.foundationdb.server.test.mt.util;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.mt.util.ThreadMonitor.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public abstract class MonitoredThread extends Thread implements HasTimeMarker
{
    protected static final Logger LOG = LoggerFactory.getLogger(MonitoredThread.class);

    private final ServiceHolder services;
    private final ThreadMonitor monitor;
    private final Collection<Stage> stageMarks;

    private final Collection<Stage> completedStages;
    private final TimeMarker timeMarker;
    private final List<Row> scannedRows;
    private boolean hadRollback;
    private Throwable failure;

    protected MonitoredThread(String name,
                              ServiceHolder services,
                              ThreadMonitor monitor,
                              Collection<Stage> stageMarks) {
        super(name);
        this.services = services;
        this.monitor = monitor;
        this.stageMarks = stageMarks;
        this.completedStages = new HashSet<>();
        this.timeMarker = new TimeMarker();
        this.scannedRows = new ArrayList<>();
    }

    //
    // MonitoredThread
    //

    protected abstract boolean doRetryOnRollback();

    public final boolean hadRollback() {
        return hadRollback;
    }

    public final boolean hadFailure() {
        return (failure != null);
    }

    public final Throwable getFailure() {
        return failure;
    }

    public final List<Row> getScannedRows() {
        return scannedRows;
    }

    protected final ServiceHolder getServiceHolder() {
        return services;
    }

    protected void mark(String msg) {
        timeMarker.mark(getName() + ":" + msg);
    }

    protected void to(Stage stage) {
        try {
            boolean firstTime = !completedStages.contains(stage);
            if(firstTime) {
                monitor.at(stage);
            }
            completedStages.add(stage);
            LOG.trace("to: {}", stage);
            if(firstTime && stageMarks.contains(stage)) {
                mark(stage.name());
            }
        } catch(Throwable t) {
            wrap(t);
        }
    }

    protected abstract void runInternal(Session session) throws Exception;

    //
    // HasTimeMarker
    //

    @Override
    public final TimeMarker getTimeMarker() {
        return timeMarker;
    }

    //
    // Thread
    //

    @Override
    public final void run() {
        to(ThreadMonitor.Stage.START);
        try(Session session = services.createSession()) {
            for(;;) {
                try {
                    runInternal(session);
                    break;
                } catch(InvalidOperationException e) {
                    hadRollback |= e.getCode().isRollbackClass();
                    if(!e.getCode().isRollbackClass() || !doRetryOnRollback()) {
                        throw e;
                    }
                    LOG.debug("retrying due to", e);
                    getScannedRows().clear();
                }
            }
        } catch(Throwable t) {
            wrap(t);
        } finally {
            to(Stage.FINISH);
        }
    }

    //
    // Internal
    //

    private void wrap(Throwable t) {
        LOG.debug("wrapping", t);
        if(failure == null) {
            failure = t;
        }
        mark(t.getClass().getSimpleName());
        if(t instanceof Error) {
            throw (Error)t;
        }
        if(t instanceof RuntimeException) {
            throw (RuntimeException)t;
        }
        throw new RuntimeException(t);
    }
}
