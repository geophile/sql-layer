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

import com.foundationdb.sql.pg.PostgresEmulatedSessionStatement.Verb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handle session statements that are not worth cluttering up the grammar with.
 */
public class PostgresEmulatedSessionStatementParser implements PostgresStatementParser
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresEmulatedSessionStatementParser.class);

    private final int possibleLetters;

    public PostgresEmulatedSessionStatementParser(PostgresServerSession server) {
        int letters = 0;
        for (Verb verb : Verb.values()) {
            letters |= (1 << (verb.getSQL().charAt(0) - 'A'));
        }
        possibleLetters = letters;
    }

    @Override
    public PostgresStatement parse(PostgresServerSession server,
                                   String sql, int[] paramTypes)  {
        if (sql.length() == 0)
            return null;
        char ch = sql.charAt(0);
        if ((ch >= 'A') && (ch <= 'Z')) {
            if ((possibleLetters & (1 << (ch - 'A'))) == 0)
                return null;
        }
        else if ((ch >= 'a') && (ch <= 'z')) {
            if ((possibleLetters & (1 << (ch - 'a'))) == 0)
                return null;
        }
        else
            return null;
        // First alpha word terminated by EOF / SP / ;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < sql.length(); i++) {
            ch = sql.charAt(i);
            if ((ch == ' ') || (ch == ';'))
                break;
            if (!(((ch >= 'A') && (ch <= 'Z')) || ((ch >= 'a') && (ch <= 'z'))))
                return null;
            sb.append(ch);
        }
        String str = sb.toString();
        for (Verb verb : Verb.values()) {
            if (verb.getSQL().equalsIgnoreCase(str)) {
                logger.debug("Emulated: {}", verb);
                return new PostgresEmulatedSessionStatement(verb, sql);
            }
        }
        return null;
    }

    @Override
    public void sessionChanged(PostgresServerSession server) {
    }

}
