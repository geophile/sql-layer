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
package com.foundationdb.server.service.blob;


import com.foundationdb.server.error.LobUnsupportedException;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.qp.operator.QueryContext;

import java.util.*;

public class LobRoutines {
    
    private LobRoutines() {
    }

    public static String createNewLob() {
        return createNewLob(false, null);
    }
    
    public static String createNewSpecificLob(String lobId) {
        return createNewLob(true, lobId);
    }
    
    private static String createNewLob(boolean specific, String lobId) {
        FDBTransactionService txnService = getTransactionService();
        final ServerQueryContext context = ServerCallContextStack.getCallingContext();
        final Session session = context.getSession();
        boolean startedTransaction = false;
        final UUID id; 
        if (specific) {
            // This is a check for valid format of id 
            id = UUID.fromString(lobId);
        } else {
            id = java.util.UUID.randomUUID();
        }
        
        if (!txnService.isTransactionActive(session)) {
            txnService.beginTransaction(session);
            startedTransaction = true;
        }
        try {
            getLobService().createNewLob(session, id);
            if (startedTransaction) {
                txnService.commitTransaction(session);
            }
        } finally {
            if (startedTransaction) {
                txnService.rollbackTransactionIfOpen(session);
            }
        }
        return id.toString();
    }
    
    
    public static long sizeBlob(final String blobId) {
        FDBTransactionService txnService = getTransactionService();
        final QueryContext context = ServerCallContextStack.getCallingContext();
        final Session session = context.getSession();
        boolean startedTransaction = false;
        long size; 
        if (!txnService.isTransactionActive(session)) {
            txnService.beginTransaction(session);
            startedTransaction = true;
        }
        final UUID id = UUID.fromString(blobId);
        
        try {
            LobService ls = getLobService();
            ls.verifyAccessPermission(session, context, id);
            size = ls.sizeBlob(session, id);
            if (startedTransaction) {
                txnService.commitTransaction(session);
            }
        } finally {
            if (startedTransaction) {
                txnService.rollbackTransactionIfOpen(session);
            }
        }
        return size;
    }
    
    public static void readBlob(final long offset, final int length, final String blobId, byte[][] out) {
        FDBTransactionService txnService = getTransactionService();
        final QueryContext context = ServerCallContextStack.getCallingContext();
        final UUID id = UUID.fromString(blobId);
        final Session session = context.getSession();
        boolean startedTransaction = false;
        byte[] output;
        if (!txnService.isTransactionActive(session)) {
            txnService.beginTransaction(session);
            startedTransaction = true;
        }
        try {
            LobService ls = getLobService();
            ls.verifyAccessPermission(session, context, id);
            output = ls.readBlob(session, id, offset, length);
            if (startedTransaction) {
                txnService.commitTransaction(session);
            }
        } finally {
            if (startedTransaction) {
                txnService.rollbackTransactionIfOpen(session);
            }
        }
        out[0] = output; 
    }
    
    public static void writeBlob(final long offset, final byte[] data, final String blobId){
        FDBTransactionService txnService = getTransactionService();
        final QueryContext context = ServerCallContextStack.getCallingContext();
        final Session session = context.getSession();
        boolean startedTransaction = false;
        final UUID id = UUID.fromString(blobId);
        if (!txnService.isTransactionActive(session)) {
            txnService.beginTransaction(session);
            startedTransaction = true;
        }
        try {
            LobService ls = getLobService();
            ls.verifyAccessPermission(session, context, id);
            ls.writeBlob(session, id, offset, data);

            if (startedTransaction) {
                txnService.commitTransaction(session);
            }
        } finally {
            if (startedTransaction) {
                txnService.rollbackTransactionIfOpen(session);
            }
        }
    }

    public static void appendBlob(final byte[] data, final String blobId) {
        FDBTransactionService txnService = getTransactionService();
        final QueryContext context = ServerCallContextStack.getCallingContext();
        final Session session = context.getSession();
        boolean startedTransaction = false;
        final UUID id = UUID.fromString(blobId);
        if (!txnService.isTransactionActive(session)) {
            txnService.beginTransaction(session);
            startedTransaction = true;
        }
        try {
            LobService ls = getLobService();
            ls.verifyAccessPermission(session, context, id);
            ls.appendBlob(session, id, data);
            if (startedTransaction) {
                txnService.commitTransaction(session);
            }
        } finally {
            if (startedTransaction) {
                txnService.rollbackTransactionIfOpen(session);
            }
        }
    }

    public static void truncateBlob(final long newLength, final String blobId) {
        FDBTransactionService txnService = getTransactionService();
        final QueryContext context = ServerCallContextStack.getCallingContext();
        final Session session = context.getSession();
        boolean startedTransaction = false;
        final UUID id = UUID.fromString(blobId);
        if (!txnService.isTransactionActive(session)) {
            txnService.beginTransaction(session);
            startedTransaction = true;
        }
        try {
            LobService ls = getLobService();
            ls.verifyAccessPermission(session, context, id);
            ls.truncateBlob(session, id, newLength);
            if (startedTransaction) {
                txnService.commitTransaction(session);
            }
        } finally {
            if (startedTransaction) {
                txnService.rollbackTransactionIfOpen(session);
            }
        }
    }
    
    private static LobService getLobService() {
        QueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServiceManager();
        return serviceManager.getServiceByClass(LobService.class);        
    }

    private static FDBTransactionService getTransactionService() {
        QueryContext context = ServerCallContextStack.getCallingContext();
        ServiceManager serviceManager = context.getServiceManager();
        TransactionService txnService = serviceManager.getServiceByClass(TransactionService.class);
        if (txnService instanceof  FDBTransactionService ) {
            return ((FDBTransactionService) txnService);
        } 
        else 
            throw new LobUnsupportedException("Unsupported transaction service");
    }
}
