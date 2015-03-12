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
package com.foundationdb.server.test.it.isolation;

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.sql.embedded.EmbeddedJDBCITBase;

import java.sql.Connection;
import java.sql.SQLException;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public abstract class IsolationITBase extends EmbeddedJDBCITBase
{
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Isolation {
        public int value();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface SQLExceptionExpected {
        public ErrorCode errorCode();
    }

    @Override
    protected boolean retryException(Throwable t) {
        // Turn off default retry in ApiTestBase.
        return false;
    }

    protected static class ExpectingSQLException extends Statement {
        private final Statement base;
        private final SQLExceptionExpected expected;
        
        public ExpectingSQLException(Statement base, SQLExceptionExpected expected) {
            this.base = base;
            this.expected = expected;
        }

        @Override
        public void evaluate() throws Throwable {
            try {
                base.evaluate();
            }
            catch (SQLException ex) {
                ErrorCode errorCode = expected.errorCode();
                if (errorCode != null) {
                    Assert.assertEquals("expected SQL state", errorCode.getFormattedValue(), ex.getSQLState());
                }
                return;
            }
            Assert.fail("Expected a SQL exception, but none was thrown");
        }
    }

    protected static class IsolationWatcher extends TestWatcher {
        private Isolation testIsolation;

        @Override
        protected void starting(Description desc) {
            testIsolation = desc.getAnnotation(Isolation.class);
        }

        @Override
        public Statement apply(Statement base, Description desc) {
            base = super.apply(base, desc);
            SQLExceptionExpected expected = desc.getAnnotation(SQLExceptionExpected.class);
            if (expected == null)
                return base;
            else
                return new ExpectingSQLException(base, expected);
        }

        public int getTestIsolationLevel() {
            if (testIsolation != null)
                return testIsolation.value();
            else
                return Connection.TRANSACTION_NONE;
        }
    }

    @Rule
    public IsolationWatcher watcher = new IsolationWatcher();

    public Connection getAutoCommitConnection() throws SQLException {
        return super.getConnection();
    }    

    @Override
    public Connection getConnection() throws SQLException {
        Connection conn = super.getConnection();
        int isolation = watcher.getTestIsolationLevel();
        if (isolation != Connection.TRANSACTION_NONE) {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(isolation);
            // If the current store does not actually support the isolation
            // level needed for the test, skip it.
            Assume.assumeTrue(isolation == conn.getTransactionIsolation());
        }
        return conn;
    }

}
