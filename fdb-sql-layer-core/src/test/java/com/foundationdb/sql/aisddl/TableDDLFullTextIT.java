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
package com.foundationdb.sql.aisddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Map;

import org.junit.Test;

import com.foundationdb.ais.model.FullTextIndex;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.service.text.FullTextIndexService;
import com.foundationdb.server.service.text.FullTextIndexServiceImpl;

public class TableDDLFullTextIT extends AISDDLITBase {

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(FullTextIndexService.class, FullTextIndexServiceImpl.class);        
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(TableDDLFullTextIT.class);
    }

    @Test
    public void createFullTextIndexTable() throws Exception {
        String sql = "CREATE TABLE test.t17 (c1 varchar(1000), INDEX t17_ft (FULL_TEXT(c1)))";
        executeDDL(sql);
        Table table = ais().getTable("test","t17");
        assertNull (table.getIndex("t17_ft"));
        assertEquals (1, table.getFullTextIndexes().size());
        FullTextIndex index = table.getFullTextIndexes().iterator().next();
        assertNotNull (table.getFullTextIndex("t17_ft"));
    }
}
