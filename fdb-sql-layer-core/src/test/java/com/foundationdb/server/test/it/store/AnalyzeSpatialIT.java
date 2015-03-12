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
package com.foundationdb.server.test.it.store;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.spatial.Spatial;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.Collections;

public final class AnalyzeSpatialIT extends ITBase {
    @Test
    public void onlyGeo() {
        int cid = createTable("schem", "tab", "id int not null primary key", "lat decimal(11,07)", "lon decimal(11,7)");

        createSpatialTableIndex("schem", "tab", "idxgeo", 0, 2, "lat", "lon");
        writeRow(cid, 10L, "10", "11");
        getStats(cid);
        serviceManager().getDXL().ddlFunctions().updateTableStatistics(session(),
                new TableName("schem", "tab"), Collections.singleton("idxgeo"));
        getStats(cid);
    }

    @Test
    public void geoAndOther() {
        int cid = createTable("schem", "tab", "id int not null primary key", "lat decimal(11,07)", "lon decimal(11,7)",
                                              "name varchar(32)");

        createSpatialTableIndex("schem", "tab", "idxgeo", 0, 2, "lat", "lon", "name");
        writeRow(cid, 10L, "10", "11", "foo");
        getStats(cid);
        serviceManager().getDXL().ddlFunctions().updateTableStatistics(session(),
                new TableName("schem", "tab"), Collections.singleton("idxgeo"));
        getStats(cid);
    }

    @Test
    public void geoGroup() {
        int cid = createTable("schem", "cust", "id int not null primary key", "lat decimal(11,07)", "lon decimal(11,7)",
                "name varchar(32)");
        int oid = createTable("schem", "orders", "id int not null primary key", "cid int not null", "colour varchar(3)",
                akibanFK("cid", "cust", "id"));

        TableName groupName = ais().getTable("schem", "cust").getGroup().getName();

        createSpatialGroupIndex(groupName, "idxgeogrp", 0, Spatial.LAT_LON_DIMENSIONS, Index.JoinType.LEFT,
                                "cust.lat", "cust.lon", "orders.colour");
        writeRow(cid, 10L, "10", "11", "foo");
        writeRow(oid, 20L, 10L, "red");
        getStats(cid);
        serviceManager().getDXL().ddlFunctions().updateTableStatistics(session(),
                new TableName("schem", "orders"), Collections.singleton("idxgeogrp"));
        getStats(cid);
        getStats(cid);
    }

    private void getStats(final int tableID) {
        txnService().run(session(), new Runnable()
        {
            @Override
            public void run() {
                Table table = getTable(tableID);
                for(Index index : table.getIndexesIncludingInternal()) {
                    indexStatsService().getIndexStatistics(session(), index);
                }
            }
        });
    }
}
