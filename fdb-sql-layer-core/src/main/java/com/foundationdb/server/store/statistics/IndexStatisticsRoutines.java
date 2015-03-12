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
package com.foundationdb.server.store.statistics;

import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerQueryContext;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.concurrent.Callable;

@SuppressWarnings("unused") // reflection
public class IndexStatisticsRoutines
{
    public static void delete(final String schema) {
        txnService().run(session(), new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                indexService().deleteIndexStatistics(session(), getSchema(schema));
                return null;
            }
        });
    }

    public static String dumpToFile(String schema, String toFile) throws IOException {
        File file = new File(toFile);
        try (FileWriter writer = new FileWriter(file)) {
            dumpInternal(writer, getSchema(schema));
        }
        return file.getAbsolutePath();
    }

    public static String dumpToString(String schema) throws IOException {
        StringWriter writer = new StringWriter();
        dumpInternal(writer, getSchema(schema));
        writer.close();
        return writer.toString();
    }

    public static void loadFromFile(final String schema, final String fromFile)  throws IOException {
        txnService().run(session(), new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                File file = new File(fromFile);
                indexService().loadIndexStatistics(session(), getSchema(schema), file);
                return null;
            }
        });
    }

    //
    // Internal
    //

    private IndexStatisticsRoutines() {
    }

    private static IndexStatisticsService indexService() {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        return context.getServer().getServiceManager().getServiceByClass(IndexStatisticsService.class);
    }

    private static Session session() {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        return context.getServer().getSession();
    }

    private static TransactionService txnService() {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        return context.getServer().getTransactionService();
    }

    private static String getSchema(String schemaInput) {
        return (schemaInput != null) ? schemaInput : ServerCallContextStack.getCallingContext().getCurrentSchema();
    }

    private static void dumpInternal(final Writer writer, final String schema) throws IOException {
        txnService().run(session(), new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                indexService().dumpIndexStatistics(session(), getSchema(schema), writer);
                return null;
            }
        });
    }
}
