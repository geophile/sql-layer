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
package com.foundationdb.sql.pg;

import com.foundationdb.ais.model.*;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.server.store.statistics.IndexStatisticsYamlTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.*;
import java.io.File;

public class IndexStatisticsLifecycleIT extends PostgresServerFilesITBase
{
    public static final File RESOURCE_DIR = new File(PostgresServerITBase.RESOURCE_DIR, "stats");

    @Before
    public void loadDatabase() throws Exception {
        loadDatabase(RESOURCE_DIR);
    }

    protected Statement executeStatement;
    protected Statement checkStatement;
    protected final String CHECK_SQL = "SELECT header.table_id, header.index_id, COUNT(detail.column_count) AS ndetail FROM "+
            IndexStatisticsService.INDEX_STATISTICS_TABLE_NAME.getDescription() + " header LEFT JOIN " +
            IndexStatisticsService.INDEX_STATISTICS_ENTRY_TABLE_NAME.getDescription() + " detail USING (table_id, index_id) GROUP BY header.table_id, header.index_id";

    @Before
    public void prepareStatements() throws Exception {
        executeStatement = getConnection().createStatement();
        checkStatement = getConnection().createStatement();
    }
    
    @After
    public void closeStatements() throws Exception {
        checkStatement.close();
        executeStatement.close();
    }

    // Check what stats are in the database. Do this using the
    // information_schema instead of any IndexStatistics API so
    // as to detect problems with the loader / caches, etc.
    protected Map<Index,Integer> check() throws Exception {
        Map<Index,Integer> result = new HashMap<>();
        AkibanInformationSchema ais = ddl().getAIS(session());
        ResultSet rs = checkStatement.executeQuery(CHECK_SQL);
        while (rs.next()) {
            int tableId = rs.getInt(1);
            int indexId = rs.getInt(2);
            int count = rs.getInt(3);
            Table table = null;
            Index index = null;
            Table aTable = ais.getTable(tableId);
            if (aTable != null) {
                table = aTable;
                for (TableIndex tindex : aTable.getIndexesIncludingInternal()) {
                    if (tindex.getIndexId() == indexId) {
                        index = tindex;
                        break;
                    }
                }
                if (index == null) {
                    for (GroupIndex gindex : aTable.getGroupIndexes()) {
                        if (gindex.getIndexId() == indexId) {
                            index = gindex;
                            break;
                        }
                    }
                }
            }
            assertNotNull("Table id refers to table", table);
            assertNotNull("Index id refers to index", index);
            assertTrue("Stats have some entries", (count > 0));
            if (table.getName().getSchemaName().equals(SCHEMA_NAME))
                result.put(index, count);
        }
        return result;
    }

    @Test
    public void test() throws Exception {
        Map<Index,Integer> entries;

        entries = check();
        assertTrue("No stats before analyze", entries.isEmpty());
        
        executeStatement.executeUpdate("ALTER TABLE parent ALL UPDATE STATISTICS");
        executeStatement.executeUpdate("ALTER TABLE child ALL UPDATE STATISTICS");
        entries = check();
        assertTrue("Some stats before analyze", !entries.isEmpty());
        AkibanInformationSchema ais = ddl().getAIS(session());
        TableIndex parentPK = ais.getTable(SCHEMA_NAME, "parent").getIndex("PRIMARY");
        Integer parentPKCount = entries.get(parentPK);
        assertNotNull("parent PK was analyzed", parentPKCount);
        assertEquals("parent PK two entries", 2, parentPKCount.intValue());
        TableIndex parentName = ais.getTable(SCHEMA_NAME, "parent").getIndex("name");
        Integer parentNameCount = entries.get(parentName);
        assertNotNull("parent name was analyzed", parentNameCount);
        assertEquals("parent name two entries", 2, parentNameCount.intValue());
        GroupIndex bothValue = ais.getGroup(new TableName(SCHEMA_NAME, "parent")).getIndex("value");
        Integer bothValueCount = entries.get(bothValue);
        assertNotNull("group index was analyzed", bothValueCount);
        assertEquals("group index two entries", 6, bothValueCount.intValue());

        executeStatement.executeUpdate("DROP INDEX parent.name");
        entries = check();
        ais = ddl().getAIS(session());
        parentPK = ais.getTable(SCHEMA_NAME, "parent").getIndex("PRIMARY");
        bothValue = ais.getGroup(new TableName(SCHEMA_NAME , "parent")).getIndex("value");
        parentPKCount = entries.get(parentPK);
        bothValueCount = entries.get(bothValue);
        assertEquals("parent PK intact after name drop", 2, parentPKCount.intValue());
        assertEquals("group index intact after name drop", 6, bothValueCount.intValue());

        executeStatement.executeUpdate("DROP INDEX value");
        entries = check();
        ais = ddl().getAIS(session());
        parentPK = ais.getTable(SCHEMA_NAME, "parent").getIndex("PRIMARY");
        parentPKCount = entries.get(parentPK);
        assertEquals("parent PK intact after group indx drop", 2, parentPKCount.intValue());
        
        executeStatement.executeUpdate("DROP TABLE child");
        entries = check();
        ais = ddl().getAIS(session());
        parentPK = ais.getTable(SCHEMA_NAME, "parent").getIndex("PRIMARY");
        parentPKCount = entries.get(parentPK);
        assertEquals("parent PK intact after child drop", 2, parentPKCount.intValue());

        executeStatement.executeUpdate("DROP TABLE parent");
        entries = check();
        assertTrue("No stats after drop group", entries.isEmpty());
    }

}
