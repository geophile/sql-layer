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
package com.foundationdb.server.test.it.bugs.bug1043377;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.listener.TableListener;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.it.ITBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public final class FailureDuringIndexBuildingIT extends ITBase implements TableListener {
    private static final AssertionError EXPECTED_EXCEPTION = new AssertionError();

    @Before
    public void registerListener() {
        serviceManager().getServiceByClass(ListenerService.class).registerTableListener(this);
    }

    @After
    public void deregisterListener() {
        serviceManager().getServiceByClass(ListenerService.class).deregisterTableListener(this);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void injectedFailure() throws Throwable {
        final String SCHEMA = "test";
        final String TABLE = "t1";
        final String INDEX = "lat_lon";
        int tid = createTable(SCHEMA, TABLE, "userID int not null primary key, lat decimal(11,7), lon decimal(11,7)");
        writeRows(
                row(tid, 1L, "20.5", "11.0"),
                row(tid, 2L, "90.0", "90.0"),
                row(tid, 3L, "60.2", "5.34")
        );

        try {
            createIndex(SCHEMA, TABLE, INDEX, "lat", "lon");
            fail("Expected exception");
        } catch(Throwable t) {
            if(t != EXPECTED_EXCEPTION) {
                throw t;
            }
        }

        Table table = getTable(SCHEMA, TABLE);
        assertNull("Index should not be present", table.getIndex(INDEX));
    }


    //
    // TableListener
    //

    @Override
    public void onCreate(Session session, Table table) {
    }

    @Override
    public void onDrop(Session session, Table table) {
    }

    @Override
    public void onTruncate(Session session, Table table, boolean isFast) {
    }

    @Override
    public void onCreateIndex(Session session, Collection<? extends Index> indexes) {
        throw EXPECTED_EXCEPTION;
    }

    @Override
    public void onDropIndex(Session session, Collection<? extends Index> indexes) {
    }
}
