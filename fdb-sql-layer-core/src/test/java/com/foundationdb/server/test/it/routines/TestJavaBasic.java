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

/** Basic Java stored procedures
 * <code><pre>
CALL sqlj.install_jar('target/fdb-sql-layer-x.y.z-tests.jar', 'testjar', 0);
CREATE PROCEDURE test.add_sub(IN x INT, IN y INT, OUT "sum" INT, out diff INT) LANGUAGE java PARAMETER STYLE java EXTERNAL NAME 'testjar:com.foundationdb.server.test.it.routines.TestJavaBasic.addSub';
CALL test.add_sub(100,59);
 * </pre></code> 
 */
public class TestJavaBasic
{
    public static void addSub(int x, int y, int[] sum, int[] diff) {
        sum[0] = x + y;
        diff[0] = x - y;
    }
}
