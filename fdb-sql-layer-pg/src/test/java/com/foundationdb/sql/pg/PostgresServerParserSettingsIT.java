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

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PostgresServerParserSettingsIT extends PostgresServerITBase
{
    private static final String CONFIG_PREFIX = "fdbsql.postgres.";
    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String,String> settings = new HashMap<>(super.startupConfigProperties());
        // Startup time only config settings
        settings.put(CONFIG_PREFIX + "columnAsFunc", "true");
        settings.put(CONFIG_PREFIX + "parserDoubleQuoted", "string");
        settings.put(CONFIG_PREFIX + "parserInfixBit", "true");
        settings.put(CONFIG_PREFIX + "parserInfixLogical", "true");
        return settings;
    }

    @Test
    public void parserSettings() throws Exception {
        Connection conn = getConnection();
        Statement s = conn.createStatement();
        ResultSet rs;
        ResultSetMetaData md;

        // special-column as functions
        rs = s.executeQuery("SELECT CURRENT_DATE, CURRENT_DATE(), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(), CURRENT_TIME, CURRENT_TIME()");
        md = rs.getMetaData();
        assertEquals(6, md.getColumnCount());
        assertEquals(Types.DATE, md.getColumnType(1));
        assertEquals(Types.DATE, md.getColumnType(2));
        assertEquals(Types.TIMESTAMP, md.getColumnType(3));
        assertEquals(Types.TIMESTAMP, md.getColumnType(4));
        assertEquals(Types.TIME, md.getColumnType(5));
        assertEquals(Types.TIME, md.getColumnType(6));

        // double quote as string
        rs = s.executeQuery("SELECT \"foo\"");
        assertTrue(rs.next());
        assertEquals("foo", rs.getString(1));

        // infix bit operators
        rs = s.executeQuery("SELECT 1|2");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));

        // infix logical operators
        rs = s.executeQuery("SELECT true||false");
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
    }
}
