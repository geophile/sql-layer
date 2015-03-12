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

import com.foundationdb.sql.pg.PostgresEmulatedMetaDataStatement.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/** Handle known system table queries from tools directly.  At some
 * point it may be possible to actually implement <code>pg_</code>
 * tables as views against the SQL Layer's own information schema. But for
 * now, some of the queries do not even parse in our dialect.
 */
public class PostgresEmulatedMetaDataStatementParser implements PostgresStatementParser
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresEmulatedMetaDataStatementParser.class);

    /** Quickly determine whether a given query <em>might</em> be a
     * Postgres system table. */
    public static final String POSSIBLE_PG_QUERY = "FROM\\s+PG_|PG_CATALOG\\.|PG_IS_";
    
    private Pattern possiblePattern;

    public PostgresEmulatedMetaDataStatementParser(PostgresServerSession server) {
        possiblePattern = Pattern.compile(POSSIBLE_PG_QUERY, Pattern.CASE_INSENSITIVE);
    }

    @Override
    public PostgresStatement parse(PostgresServerSession server,
                                   String sql, int[] paramTypes)  {
        if (!possiblePattern.matcher(sql).find())
            return null;
        List<String> groups = new ArrayList<>();
        for (Query query : Query.values()) {
            if (query.matches(sql, groups)) {
                logger.debug("Emulated: {}{}", query, groups.subList(1, groups.size()));
                return new PostgresEmulatedMetaDataStatement(query, groups,
                                                             server.typesTranslator());
            }
        }
        return null;
    }

    @Override
    public void sessionChanged(PostgresServerSession server) {
    }

}
