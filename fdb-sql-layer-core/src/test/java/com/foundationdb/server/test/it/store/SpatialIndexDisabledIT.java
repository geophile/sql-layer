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

import com.foundationdb.ais.model.Index.JoinType;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.NotAllowedByConfigException;
import com.foundationdb.server.service.config.TestConfigService;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SpatialIndexDisabledIT extends ITBase
{
    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> map = new HashMap<>(super.startupConfigProperties());
        map.put(TestConfigService.FEATURE_SPATIAL_INDEX_KEY, "false");
        return map;
    }

    @Test(expected=NotAllowedByConfigException.class)
    public void tableIndex() {
        createTable("test", "t", "id int not null primary key, lat decimal(11,7), lon decimal(11,7)");
        createSpatialTableIndex("test", "t", "s", 0, 2, "lat", "lon");
    }

    @Test(expected=NotAllowedByConfigException.class)
    public void groupIndex() {
        createTable("test", "c", "id int not null primary key, x int");
        createTable("test", "a", "id int not null primary key, cid int, lat decimal(11,7), lon decimal(11,7), grouping foreign key(cid) references c(id)");
        createSpatialGroupIndex(new TableName("test", "c"), "s", 1, 2, JoinType.LEFT, "c.x", "a.lat", "a.lon");
    }
}
