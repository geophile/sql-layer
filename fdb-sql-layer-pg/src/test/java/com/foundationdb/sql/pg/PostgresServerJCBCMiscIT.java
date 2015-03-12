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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PostgresServerJCBCMiscIT extends PostgresServerITBase
{
    @Test
    public void absDecimalResultType() throws Exception {
        Connection conn = getConnection();
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("SELECT ABS( CAST('1234.567' AS DECIMAL(12,7)) )");
        ResultSetMetaData md = rs.getMetaData();
        assertEquals("column type", Types.DECIMAL, md.getColumnType(1));
        assertEquals("precision", 12, md.getPrecision(1));
        assertEquals("scale", 7, md.getScale(1));
        assertTrue(rs.next());
        assertEquals("1234.5670000", rs.getString(1));
    }
}
