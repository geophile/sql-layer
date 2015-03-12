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
package com.foundationdb.server.service.routines;

import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerQueryContext;

@SuppressWarnings("unused") // reflection
public class QueryLoggingRoutines
{
    private QueryLoggingRoutines() {
    }

    private static MonitorService monitorService() {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        return context.getServer().getServiceManager().getMonitorService();
    }

    public static void setEnabled(boolean enabled) {
        monitorService().setQueryLogEnabled(enabled);
    }

    public static boolean isEnabled() {
        return monitorService().isQueryLogEnabled();
    }

    public static void setFile(String filename) {
        monitorService().setQueryLogFileName(filename);
        // Convenience: If enabled, close and reopen
        if(monitorService().isQueryLogEnabled()) {
            monitorService().setQueryLogEnabled(false);
            monitorService().setQueryLogEnabled(true);
        }
    }

    public static String getFile() {
        return monitorService().getQueryLogFileName();
    }

    public static void setMillis(long millis) {
        monitorService().setQueryLogThresholdMillis(millis);
    }

    public static long getMillis() {
        return monitorService().getQueryLogThresholdMillis();
    }
}
