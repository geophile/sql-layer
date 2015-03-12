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
package com.foundationdb.server.service.dxl;

import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;

public class DXLTransactionHook implements DXLFunctionsHook {
    private static final Session.StackKey<Boolean> AUTO_TRX_CLOSE = Session.StackKey.stackNamed("AUTO_TRX_CLOSE");
    private final TransactionService txnService;

    public DXLTransactionHook(TransactionService txnService) {
        this.txnService = txnService;
    }

    @Override
    public void hookFunctionIn(Session session, DXLFunction function) {
        if(!needsTransaction(function)) {
            return;
        }
        if(!txnService.isTransactionActive(session)) {
            txnService.beginTransaction(session);
            session.push(AUTO_TRX_CLOSE, Boolean.TRUE);
        } else {
            session.push(AUTO_TRX_CLOSE, Boolean.FALSE);
        }
    }

    @Override
    public void hookFunctionCatch(Session session, DXLFunction function, Throwable throwable) {
        if(!needsTransaction(function)) {
            return;
        }
        Boolean doAuto = session.pop(AUTO_TRX_CLOSE);
        if(doAuto != null && doAuto) {
            txnService.rollbackTransaction(session);
        }
    }

    @Override
    public void hookFunctionFinally(Session session, DXLFunction function, Throwable throwable) {
        if(!needsTransaction(function)) {
            return;
        }
        Boolean doAuto = session.pop(AUTO_TRX_CLOSE);
        if(doAuto != null && doAuto) {
            txnService.commitTransaction(session);
        }
    }
    
    private static boolean needsTransaction(DXLFunction function) {
        switch(function) {
            // DDL modifying existing table(s), locking and manual transaction handling needed
            case DROP_TABLE:
            case ALTER_TABLE:
            case DROP_SCHEMA:
            case DROP_GROUP:
            case CREATE_INDEXES:
            case DROP_INDEXES:
                return false;

            // DDL changing AIS but does not scan or modify existing table (locking not needed)
            case ALTER_SEQUENCE:
            case RENAME_TABLE:
            case CREATE_TABLE:
            case CREATE_VIEW:
            case DROP_VIEW:
            case CREATE_SEQUENCE:
            case DROP_SEQUENCE:
            case CREATE_ROUTINE:
            case DROP_ROUTINE:
            case CREATE_SQLJ_JAR:
            case REPLACE_SQLJ_JAR:
            case DROP_SQLJ_JAR:
                return true;

            // Remaining methods on DDL interface, querying only
            case UPDATE_TABLE_STATISTICS:
            case GET_AIS:
            case GET_TABLE_ID:
            case GET_TABLE_BY_ID:
            case GET_TABLE_BY_NAME:
            case GET_USER_TABLE_BY_ID:
            case GET_ACTIVE_GENERATIONS:
                return true;

            // DML that looks at AIS
            case TRUNCATE_TABLE:
                return true;
        }

        throw new IllegalArgumentException("Unexpected function for hook " + function);
    }
}
