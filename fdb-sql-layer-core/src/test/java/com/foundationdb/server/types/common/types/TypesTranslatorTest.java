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
package com.foundationdb.server.types.common.types;

import com.foundationdb.server.types.TInstance;

import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.types.DataTypeDescriptor;

import org.junit.Ignore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Ignore
public class TypesTranslatorTest
{
    protected final TypesTranslator typesTranslator;

    protected TypesTranslatorTest(TypesTranslator typesTranslator) {
        this.typesTranslator = typesTranslator;
    }

    protected DataTypeDescriptor parseType(String typeString) throws Exception {
        String sql = String.format("SELECT CAST(x AS %s)", typeString);
        StatementNode stmt = new SQLParser().parseStatement(sql);
        return ((DMLStatementNode)stmt).getResultSetNode().getResultColumns()
            .get(0).getExpression().getType();
    }

    protected void testType(String typeString, String expected) throws Exception {
        TInstance type = typesTranslator.typeForSQLType(parseType(typeString));
        assertNotNull(typeString, type);
        assertEquals(typeString, expected, type.toStringIgnoringNullability(false));
    }

}
