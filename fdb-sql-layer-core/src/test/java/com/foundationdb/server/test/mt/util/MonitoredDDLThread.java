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

import com.foundationdb.server.SchemaFactory;
import com.foundationdb.server.service.dxl.OnlineDDLMonitor;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.mt.util.ThreadMonitor.Stage;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.Collection;
import java.util.List;

public class MonitoredDDLThread extends MonitoredThread
{
    private final OnlineDDLMonitor onlineDDLMonitor;
    private final Collection<OnlineDDLMonitor.Stage> onlineStageMarks;
    private final String schema;
    private final String ddl;
    private final List<DataTypeDescriptor> descriptors;
    private final List<String> columnNames;
    private final OnlineCreateTableAsBase.TestSession server;

    public MonitoredDDLThread(String name,
                              ServiceHolder services,
                              ThreadMonitor monitor,
                              Collection<Stage> threadStageMarks,
                              OnlineDDLMonitor onlineDDLMonitor,
                              Collection<OnlineDDLMonitor.Stage> onlineStageMarks,
                              String schema,
                              String ddl,
                              List<DataTypeDescriptor> descriptors,
                              List<String> columnNames,
                              OnlineCreateTableAsBase.TestSession server) {
        super(name, services, monitor, threadStageMarks);
        this.onlineDDLMonitor = new OnlineDDLMonitorShim(onlineDDLMonitor);
        this.onlineStageMarks = onlineStageMarks;
        this.schema = schema;
        this.ddl = ddl;
        this.descriptors = descriptors;
        this.columnNames = columnNames;
        this.server = server;
    }

    public MonitoredDDLThread(String name,
                              ServiceHolder services,
                              ThreadMonitor monitor,
                              Collection<Stage> threadStageMarks,
                              OnlineDDLMonitor onlineDDLMonitor,
                              Collection<OnlineDDLMonitor.Stage> onlineStageMarks,
                              String schema,
                              String ddl) {
        this(name, services, monitor, threadStageMarks, onlineDDLMonitor, onlineStageMarks, schema, ddl, null, null, null);
    }

    //
    // MonitoredThread
    //

    @Override
    protected boolean doRetryOnRollback() {
        return true;
    }

    @Override
    protected void runInternal(Session session) {
        getServiceHolder().getDDLFunctions().setOnlineDDLMonitor(onlineDDLMonitor);
        try {
            SchemaFactory schemaFactory = new SchemaFactory(schema);
            if(server != null) {
                server.setSession(session);
                schemaFactory.ddl(getServiceHolder().getDDLFunctions(), session, descriptors, columnNames, server, ddl);
            } else {
                schemaFactory.ddl(getServiceHolder().getDDLFunctions(), session, ddl);
            }



        } finally {
            getServiceHolder().getDDLFunctions().setOnlineDDLMonitor(null);
        }
    }

    //
    // Internal
    //

    private class OnlineDDLMonitorShim implements OnlineDDLMonitor
    {
        private final OnlineDDLMonitor delegate;

        private OnlineDDLMonitorShim(OnlineDDLMonitor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void at(OnlineDDLMonitor.Stage stage) {
            delegate.at(stage);
            LOG.trace("at: {}", stage);
            if(onlineStageMarks.contains(stage)) {
                mark(stage.name());
            }
        }
    }
}
