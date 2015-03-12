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
package com.foundationdb.server.test.mt;

import com.foundationdb.server.test.mt.util.ThreadHelper;
import com.foundationdb.util.Exceptions;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

public class ConcurrentStatisticsMT extends PostgresMTBase
{
    private static final double MAX_FAIL_PERCENT = 0.25;
    private static final int LOOP_COUNT = 30;
    private static final String TABLE_NAME = "t";

    private static abstract class BaseRunnable implements Runnable {
        private final PostgresMTBase testBase;
        private int retryable;

        protected BaseRunnable(PostgresMTBase testBase) {
            this.testBase = testBase;
        }

        @Override
        public void run() {
            try(Connection conn = testBase.createConnection()) {
                for(int i = 0; i < LOOP_COUNT; ++i) {
                    try {
                        runInternal(conn);
                        if(!conn.getAutoCommit()) {
                            conn.commit();
                        }
                    } catch(SQLException e) {
                        if(e.getSQLState().startsWith("40")) {
                            ++retryable;
                        } else {
                            throw new RuntimeException(e);
                        }
                        if(!conn.getAutoCommit()) {
                            conn.rollback();
                        }
                    }
                }
            } catch(Exception e) {
                throw Exceptions.throwAlways(e);
            }
        }

        public synchronized int getRetryableCount() {
            return retryable;
        }

        protected abstract void runInternal(Connection conn) throws Exception;
    }

    private static class InsertRunnable extends BaseRunnable
    {
        private PreparedStatement ps;
        private int id;

        private InsertRunnable(PostgresMTBase testBase) {
            super(testBase);
            this.id = 1;
        }

        @Override
        public void runInternal(Connection conn) throws Exception {
            if(ps == null) {
                ps = conn.prepareStatement("INSERT INTO " + TABLE_NAME + " VALUES (?,?)");
                conn.setAutoCommit(false);
            }
            for(int i = 0; i < 50; ++i, ++id) {
                ps.setInt(1, id);
                ps.setString(2, Integer.toString(id));
                ps.executeUpdate();
            }
        }
    }

    private static class UpdateStatisticsRunnable extends BaseRunnable {
        private PreparedStatement s;

        protected UpdateStatisticsRunnable(PostgresMTBase testBase) {
            super(testBase);
        }

        @Override
        protected void runInternal(Connection conn) throws Exception {
            if(s == null) {
                s = conn.prepareStatement("ALTER TABLE "+ TABLE_NAME +" ALL UPDATE STATISTICS");
                conn.setAutoCommit(true);
            }
            s.executeUpdate();
        }
    }

    @Test
    public void concurrentInsertAndStatistics() {
        createTable(SCHEMA_NAME, TABLE_NAME, "id INT NOT NULL PRIMARY KEY, s VARCHAR(32)");
        createIndex(SCHEMA_NAME, TABLE_NAME, "s", "s");

        InsertRunnable insert = new InsertRunnable(this);
        UpdateStatisticsRunnable stats = new UpdateStatisticsRunnable(this);
        List<Thread> threads = Arrays.asList(new Thread(insert), new Thread(stats));
        ThreadHelper.runAndCheck(60000, threads);

        double total = insert.getRetryableCount() + stats.getRetryableCount();
        double percent = total / (LOOP_COUNT * 2);
        if(percent > MAX_FAIL_PERCENT) {
            fail(String.format("Too many failures: %g%% > %g%%", percent*100, MAX_FAIL_PERCENT*100));
        }
    }
}
