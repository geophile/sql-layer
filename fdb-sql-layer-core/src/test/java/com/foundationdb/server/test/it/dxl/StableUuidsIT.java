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
package com.foundationdb.server.test.it.dxl;

import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public final class StableUuidsIT extends ITBase {
    @Test
    public void uuidsSurviveRestart() throws Exception {
        final int tableId = createTable("testSchema", "customer", "id int not null primary key, name varchar(32)");
        UUID origUuid = ais().getTable(tableId).getUuid();
        assertNotNull("original UUID is null", origUuid);
        safeRestartTestServices();
        UUID afterRestartUuid = ais().getTable(tableId).getUuid();
        assertNotNull("original UUID is null", afterRestartUuid);
        assertEquals("UUIDs for customer", origUuid, afterRestartUuid);
        assertNotSame("UUIDs are same object", origUuid, afterRestartUuid);
    }
}
