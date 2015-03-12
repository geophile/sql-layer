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

import java.sql.PreparedStatement;
import java.sql.Types;

import org.junit.Before;
import org.junit.Test;
import com.foundationdb.sql.jdbc.PGStatement;

import com.foundationdb.ais.model.TableName;

public class PostgresServerStatementCachePreparedStatementIT extends
        PostgresServerITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "texttable";
    private static final TableName TABLE_NAME = new TableName(SCHEMA, TABLE);

    @Before
    public void createAndInsert() {
        createTable(TABLE_NAME, "te varchar(100)");
    }

    @Test
    public void testSetNull() throws Exception {
        // valid: fully qualified type to setNull()
        PreparedStatement pstmt = getConnection().prepareStatement("INSERT INTO texttable (te) VALUES (?)");

        if(pstmt instanceof PGStatement) {
            PGStatement pgp = (PGStatement) pstmt;
            pgp.setPrepareThreshold(2);
        }

        pstmt.setNull(1, Types.VARCHAR);
        pstmt.executeUpdate();

        // valid: fully qualified type to setObject()
        pstmt.setObject(1, null, Types.VARCHAR);
        pstmt.executeUpdate();

        // valid: setObject() with partial type info and a typed "null object instance"
        com.foundationdb.sql.jdbc.util.PGobject dummy = new com.foundationdb.sql.jdbc.util.PGobject();
        dummy.setType("VARCHAR");
        dummy.setValue(null);
        pstmt.setObject(1, dummy, Types.OTHER);
        pstmt.executeUpdate();

        // setObject() with no type info
        pstmt.setObject(1, null);
        pstmt.executeUpdate();

        // setObject() with insufficient type info
        pstmt.setObject(1, null, Types.OTHER);
        pstmt.executeUpdate();

        // setNull() with insufficient type info
        pstmt.setNull(1, Types.OTHER);
        pstmt.executeUpdate();

        pstmt.close();
    }
    
}
