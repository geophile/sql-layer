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
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.Map;

public class PostgresServerProtocolV2IT extends PostgresServerITBase
{
    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(PostgresServerProtocolV2IT.class);
    }

    @Override
    protected String getConnectionURL() {
        return super.getConnectionURL() + "?protocolVersion=2";
    }
    
    @Test
    public void checkRejection() throws Exception {
        try {
            getConnection();
        }
        catch (SQLException ex) {
            if (ex.getMessage().indexOf("Unsupported protocol version 2") < 0) {
                fail("message was not readable: " + ex.getMessage());
            }
            return;
        }
        forgetConnection();
        fail("Expected connection to be rejected.");
    }

}
