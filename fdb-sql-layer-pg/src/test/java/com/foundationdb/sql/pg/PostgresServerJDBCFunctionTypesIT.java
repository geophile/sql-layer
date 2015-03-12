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


import com.foundationdb.junit.SelectedParameterizedRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.CallableStatement;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@RunWith(SelectedParameterizedRunner.class)
public class PostgresServerJDBCFunctionTypesIT extends PostgresServerITBase{

    private final boolean binary;

    @Before
    public void ensureCorrectConnectionType() throws Exception {
        forgetConnection();
    }

    @Override
    protected String getConnectionURL() {
        // loglevel=2 is also useful for seeing what's really happening.
        return super.getConnectionURL() + "?prepareThreshold=1&binaryTransfer=" + binary;
    }

    @Parameterized.Parameters(name="{0}")
    public static Iterable<Object[]> types() throws Exception {
        return Arrays.asList(new Object[]{"Binary", true}, new Object[]{"Text", false});
    }

    public PostgresServerJDBCFunctionTypesIT(String name, boolean binary) {
        this.binary = binary;
    }

    @Before
    public void setupFunctions() throws Exception {
        Statement stmt = getConnection().createStatement();
        stmt.execute ("CREATE OR REPLACE FUNCTION testspg__stringToInt(a varchar(20)) RETURNS int" +
                " LANGUAGE javascript PARAMETER STYLE variables AS $$ 42 + a.length $$");
        stmt.execute ("CREATE OR REPLACE FUNCTION testspg__intToString(a int) RETURNS varchar(20)" +
                " LANGUAGE javascript PARAMETER STYLE variables AS $$ 'bob' + a $$");
        stmt.close ();
    }

    @After
    public void dropFunctions() throws Exception {
        Statement stmt = getConnection().createStatement();

        stmt.execute("drop FUNCTION testspg__stringToInt");
        stmt.execute("drop FUNCTION testspg__intToString");
        stmt.close ();
    }

    @Test
    public void testStringToInt() throws Exception {
        CallableStatement call = getConnection().prepareCall("{ ? = call testspg__stringToInt (?) }");
        call.setString (2, "foo");
        call.registerOutParameter (1, Types.INTEGER);
        call.execute ();
        assertEquals(45, call.getInt(1));
    }

    @Test
    public void testIntToString() throws Exception {
        CallableStatement call = getConnection().prepareCall("{ ? = call testspg__intToString (?) }");
        call.setInt(2, 42);
        call.registerOutParameter (1, Types.VARCHAR);
        call.execute ();
        assertEquals("bob42", call.getString(1));
    }

    @Test
    public void testIntToStringWithDouble() throws Exception {
        CallableStatement call = getConnection().prepareCall("{ ? = call testspg__intToString (?) }");
        call.setDouble(2, 589.32);
        // JDBC ensures that the OutParameter has the right type
        call.registerOutParameter (1, Types.VARCHAR);
        call.execute ();
        assertEquals("bob589", call.getString(1));
    }

}
