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

import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.sql.parser.ExistenceCheck;

public class DDLHelper {
    private DDLHelper() {}

    public static TableName convertName(String defaultSchema, com.foundationdb.sql.parser.TableName parserName) {
        final String schema = parserName.hasSchema() ? parserName.getSchemaName() : defaultSchema;
        return new TableName(schema, parserName.getTableName());
    }

    public static boolean skipOrThrow(QueryContext context, ExistenceCheck check, Object o, InvalidOperationException e) {
        if(check == ExistenceCheck.IF_EXISTS) {
            if(o == null) {
                if(context != null) {
                    context.warnClient(e);
                }
                return true;
            }
            throw e;
        }
        if(check == ExistenceCheck.IF_NOT_EXISTS) {
            if(o != null) {
                if(context != null) {
                    context.warnClient(e);
                }
                return true;
            }
            throw e;
        }
        if((check == ExistenceCheck.NO_CONDITION) || (check == null)) {
            throw e;
        }
        throw new IllegalStateException("Unexpected condition: " + check);
    }
}
