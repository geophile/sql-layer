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

import com.foundationdb.server.service.dxl.OnlineDDLMonitor;
import com.foundationdb.server.test.mt.OnlineCreateTableAsMT;
import com.foundationdb.sql.server.ServerSession;
import com.foundationdb.sql.server.ServerSessionBase;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.List;

public interface ConcurrentTestBuilder
{
    ConcurrentTestBuilder add(String name, OperatorCreator creator);
    ConcurrentTestBuilder mark(ThreadMonitor.Stage... stages);
    ConcurrentTestBuilder sync(String name, ThreadMonitor.Stage stage);
    ConcurrentTestBuilder rollbackRetry(boolean doRetry);
    ConcurrentTestBuilder sync(String testName, String syncName, ThreadMonitor.Stage stage);

    ConcurrentTestBuilder add(String name, String schema, String ddl);
    ConcurrentTestBuilder mark(OnlineDDLMonitor.Stage... stages);
    ConcurrentTestBuilder sync(String name, OnlineDDLMonitor.Stage stage);
    ConcurrentTestBuilder sync(String testName, String syncName, OnlineDDLMonitor.Stage stage);

    List<MonitoredThread> build(ServiceHolder serviceHolder);
    List<MonitoredThread> build(ServiceHolder serviceHolder, List<DataTypeDescriptor> descriptors,
                                List<String> columnNames, OnlineCreateTableAsBase.TestSession server);
}
