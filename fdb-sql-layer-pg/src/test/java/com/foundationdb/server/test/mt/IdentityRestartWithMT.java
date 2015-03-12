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

import com.foundationdb.server.error.ErrorCode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IdentityRestartWithMT extends PostgresMTBase
{
    private static final Logger LOG = LoggerFactory.getLogger(IdentityRestartWithMT.class);

    private static final int TOTAL_MODE_EXECS = 1000;

    private static final String SQL_BEGIN = "BEGIN";
    private static final String SQL_COMMIT = "COMMIT";
    private static final String SQL_ROLLBACK = "ROLLBACK";
    private static final String SQL_INSERT = "INSERT INTO t(id) VALUES (DEFAULT) RETURNING id";
    private static final String SQL_DELETE = "DELETE FROM t";
    private static final String SQL_SELECT = "SELECT MAX(id) FROM t";
    private static final String SQL_ALTER_RESTART = "ALTER TABLE t ALTER COLUMN id RESTART WITH %d";

    // Note: Arranged so RESTART won't happen unless the table is empty (i.e. INSERT takes 3 steps
    // and can't complete between DELETE and ALTER).
    // Avoids needing ti figure out current max without loss of generality.
    private static final String[][] MODES = {
        {
            SQL_BEGIN,
            SQL_INSERT,
            SQL_COMMIT,
        },
        {
            SQL_BEGIN,
            SQL_INSERT,
            SQL_ROLLBACK,
        },
        {
            SQL_BEGIN,
            SQL_SELECT,
            SQL_DELETE,
            SQL_COMMIT,
            SQL_ALTER_RESTART,
        },
    };

    private static class ConnState
    {
        public final String name;
        public final Connection conn;
        public Statement s;
        public String[] mode;
        public int step;

        private ConnState(String name, Connection conn) throws SQLException {
            this.name = name;
            this.conn = conn;
            this.conn.setAutoCommit(true);
        }

        public void step(Random r) throws SQLException {
            String sql = mode[step];
            if(sql.contains("%d")) {
                sql = String.format(sql, 1 + r.nextInt(100));
            }
            exec(sql);
            if(sql.equals(SQL_INSERT)) {
                ResultSet rs = s.getResultSet();
                rs.next();
                LOG.debug("{} inserted: {}", name, rs.getInt(1));
            }
            if(++step == mode.length) {
                changeMode(r);
            }
        }

        public void exec(String sql) throws SQLException {
            for(;;) {
                if(s == null) {
                    s = conn.createStatement();
                }
                try {
                    LOG.debug("{} exec: {}", name, sql);
                    s.execute(sql);
                    break;
                } catch(SQLException e) {
                    if(!ErrorCode.STALE_STATEMENT.getFormattedValue().equals(e.getSQLState())) {
                        throw e;
                    }
                    LOG.debug("{} retrying", name);
                    s.close();
                    s = null;
                }
            }
        }

        public void changeMode(Random r) {
            this.mode = MODES[r.nextInt(MODES.length - 2)];
            this.step = 0;
        }
    }


    @Rule
    public final TestWatcher watcher = new TestWatcher() {
        protected void failed(Throwable e, Description description) {
            LOG.error("Failure with seed: {}" + seed);
        }
    };


    private final int seed = Math.abs((int)System.nanoTime());
    private final Random random = new Random(seed);


    @Test
    public void oneConn() throws Exception {
        run(1);
    }

    @Test
    public void twoConn() throws Exception {
        run(2);
    }

    @Test
    public void tenConn() throws Exception {
        run(10);
    }

    private void run(int connCount) throws Exception {
        createTable(SCHEMA_NAME, "t", "id SERIAL NOT NULL PRIMARY KEY");

        ConnState[] states = new ConnState[connCount];
        for(int i = 0; i < connCount; ++i) {
            states[i] = new ConnState(Integer.toString(i), createConnection());
            states[i].changeMode(random);
        }

        int loops = TOTAL_MODE_EXECS / connCount;
        for(int i = 0; i < loops; ++i) {
            LOG.debug("Loop {}", i);
            for(ConnState ch : states) {
                try {
                    ch.step(random);
                } catch(SQLException e) {
                    if(!ErrorCode.valueOfCode(e.getSQLState()).isRollbackClass()) {
                        fail("Unexpected failure during " + Arrays.toString(ch.mode) + "[" + ch.step + "]: " + e.getMessage());
                    } else {
                        LOG.debug(e.getMessage());
                        ch.s.execute(SQL_ROLLBACK);
                        ch.changeMode(random);
                    }
                }
            }
        }
    }
}
