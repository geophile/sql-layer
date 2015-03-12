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

import org.junit.Before;
import org.junit.Test;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import static org.junit.Assert.assertTrue;

public class PostgresServerFormatBinaryOutputIT extends PostgresServerITBase {

    @Before
    public void createTable() throws Exception {
        String createTable = "CREATE TABLE binaryformat (col_int INT PRIMARY KEY NOT NULL, col_binary CHAR(10) FOR BIT DATA, col_varbinary VARCHAR(10) FOR BIT DATA)";
        executeQuery(createTable);
        String insSql = "INSERT INTO binaryformat VALUES (0, x'41', x'42')";
        executeQuery(insSql);
    }

    @Test
    public void testBinaryFormat() throws Exception {

        PreparedStatement getStmt = getConnection().prepareStatement(String.format("SELECT * FROM binaryformat WHERE col_int = 0"));

        String[] setSt = {"SET binary_output TO OCTAL", "SET binary_output TO HEX", "SET binary_output TO BASE64"};
        ResultSet rs;
        String res1, res2, res3;
        for (int i = 0; i < setSt.length; i++) {
            executeQuery(setSt[i]);
            rs = getStmt.executeQuery();
            assertTrue(rs.next());
            res1 = rs.getString(1);
            assertTrue(res1.equals("0"));
            res2 = rs.getString(2);
            res3 = rs.getString(3);
            if ( i == 0) {
                assertTrue(res2.equals("\\101"));
                assertTrue(res3.equals("\\102"));
            } else if ( i == 1) {
                assertTrue(res2.equals("\\x41"));
                assertTrue(res3.equals("\\x42"));
            } else if ( i == 2) {
                assertTrue(res2.equals("QQ=="));
                assertTrue(res3.equals("Qg=="));
            }
            rs.close();
        }
        getStmt.close();
    }

    private void executeQuery(String sql) throws Exception {
        Statement stmt = getConnection().createStatement();
        stmt.execute(sql);
        stmt.close();
    } 
}
