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
package com.foundationdb.server.test.it.routines;

import java.sql.*;

/** Java stored procedures using JDBC
 * <code><pre>
DROP TABLE test2;
CREATE TABLE test2(n INT NOT NULL, value VARCHAR(10));
INSERT INTO test2 VALUES(1, 'aaa'), (2, 'bbb'), (1, 'xyz');
CALL sqlj.install_jar('target/fdb-sql-layer-x.y.z-tests.jar', 'testjar', 0);
CREATE PROCEDURE test.split_results(IN n INT) LANGUAGE java PARAMETER STYLE java RESULT SETS 2 EXTERNAL NAME 'testjar:com.foundationdb.server.test.it.routines.TestJDBC.splitResults';
CALL test.split_results(1);
 * </pre></code> 
 */
public class TestJDBC
{
    public static void splitResults(int n, ResultSet[] rs1, ResultSet[] rs2) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection", "test", "");
        PreparedStatement ps1 = conn.prepareStatement("SELECT * FROM test2 WHERE n = ?");
        PreparedStatement ps2 = conn.prepareStatement("SELECT * FROM test2 WHERE n <> ?");
        ps1.setInt(1, n);
        ps2.setInt(1, n);
        rs1[0] = ps1.executeQuery();
        rs2[0] = ps2.executeQuery();
    }
}
