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

import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Random;

public class SortTimingIT extends PostgresServerITBase
{
    private static final int N = 300000;
    private static final int FILLER_SIZE = 100;
    private static final String FILLER;
    static
    {
        char[] filler = new char[FILLER_SIZE];
        Arrays.fill(filler, 'x');
        FILLER = new String(filler);
    }

    @Test
    @Ignore
    public void test() throws Exception
    {
        loadDB();
        sort(false);
        sort(true);
    }

    private void loadDB() throws Exception
    {
        Statement statement = getConnection().createStatement();
        statement.execute(
            String.format("create table t(id integer not null primary key, foobar int, filler varchar(%s))",
                          FILLER_SIZE));
        Random random = new Random();
        for (int id = 0; id < N; id++) {
            statement.execute(String.format("insert into t values(%s, %s, '%s')", id, random.nextInt(), FILLER));
        }
        statement.execute("select count(*) from t");
        ResultSet resultSet = statement.getResultSet();
        resultSet.next();
        System.out.println(String.format("Loaded %s rows", resultSet.getInt(1)));
        statement.close();
    }

    private void sort(boolean tempVolume) throws Exception
    {
        long start = System.currentTimeMillis();
        System.setProperty("sorttemp", tempVolume ? "true" : "false");
        Statement statement = getConnection().createStatement();
        statement.execute("select * from t order by foobar");
        ResultSet resultSet = statement.getResultSet();
        resultSet.next();
        long stop = System.currentTimeMillis();
        System.out.println(String.format("sort with temp = %s: %s msec", tempVolume, (stop - start)));
        resultSet.close();
        statement.close();
    }
}
