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

import com.foundationdb.Transaction;
import com.foundationdb.blob.SQLBlob;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.LobNotFoundException;
import com.foundationdb.server.error.LobContentException;
import com.foundationdb.server.error.LobDuplicateException;
import com.foundationdb.TransactionContext;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBHolder;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.tuple.Tuple2;
import com.google.inject.Inject;

import java.util.Arrays;
import java.util.UUID;

public class FDBLobService implements Service, LobService {
    private static byte[] LOB_EXISTS = {0x01};
    private DirectorySubspace lobDirectory;
    private FDBHolder fdbHolder;
    private FDBTransactionService transactionService;
    private final SecurityService securityService;
    private final ServiceManager serviceManager;
    private final String LOB_DIRECTORY = "lobs";
    
    @Inject
    public FDBLobService(ServiceManager serviceManager, SecurityService securityService) {
        this.serviceManager = serviceManager;
        this.securityService = securityService;
    }

    @Override
    public void createNewLob(Session session, final UUID lobId) {
        if (existsLob(session, lobId)) {
            throw new LobDuplicateException(lobId.toString());
        }
        getTxc(session).set(getLobSubspace(lobId).pack(), LOB_EXISTS);
        transactionService.addCallback(session, TransactionService.CallbackType.PRE_COMMIT, new TransactionService.Callback() {
            @Override
            public void run(Session session, long timestamp) {
                try {
                    SQLBlob blob = openBlob(session, lobId);
                    if (!(blob.isLinked(getTxc(session))).get()) {
                        deleteLob(session, lobId);
                    }
                } catch (LobNotFoundException le) {
                    // lob already gone
                }
            }
        });
    }

    @Override
    public boolean existsLob(Session session, UUID lobId) {
        return getTxc(session).get(getLobSubspace(lobId).pack()).get() != null;
    }

    @Override
    public void deleteLob(Session session, UUID lobId) {
        // clear content
        getTxc(session).clear(getLobSubspace(lobId).range());
        // clear existence value
        getTxc(session).clear(getLobSubspace(lobId).pack());
    }

    @Override
    public void linkTableBlob(final Session session, final UUID lobId, final int tableId) {
        Transaction tr = getTxc(session);
        SQLBlob blob = openBlob(session, lobId);
        if (blob.isLinked(tr).get()) {
            if (blob.getLinkedTable(tr).get() != tableId) {
                throw new LobContentException("lob is already linked to a table");
            }
            return;
        }
        blob.setLinkedTable(tr, tableId).get();
    }

    @Override
    public long sizeBlob(final Session session, final UUID lobId) {
        SQLBlob blob = openBlob(session, lobId);
        return blob.getSize(getTxc(session)).get();
    }

    @Override
    public byte[] readBlob(final Session session, final UUID lobId, final long offset, final int length) {
        SQLBlob blob = openBlob(session, lobId);
        byte[] res = blob.read(getTxc(session), offset, length).get();
        return res != null ? res : new byte[]{};
    }

    @Override
    public byte[] readBlob(final Session session, final UUID lobId) {
        SQLBlob blob = openBlob(session, lobId);
        return blob.read(getTxc(session)).get();
    }

    @Override
    public void writeBlob(final Session session, final UUID lobId, final long offset, final byte[] data) {
        SQLBlob blob = openBlob(session, lobId);
        blob.write(getTxc(session), offset, data).get();
    }

    @Override
    public void appendBlob(final Session session, final UUID lobId, final byte[] data) {
        SQLBlob blob = openBlob(session, lobId);
        blob.append(getTxc(session), data).get();
    }

    @Override
    public void truncateBlob(final Session session, final UUID lobId, final long size) {
        SQLBlob blob = openBlob(session, lobId);
        blob.truncate(getTxc(session), size).get();
    }

    @Override
    public void clearAllLobs(Session session) { 
        TransactionContext txc = getTxc(session);
        lobDirectory.removeIfExists(txc).get();
        lobDirectory = fdbHolder.getRootDirectory().create(txc, Arrays.asList(LOB_DIRECTORY)).get();
    }
    
    @Override
    public void verifyAccessPermission(final Session session, final QueryContext context, final UUID lobId) {
        SQLBlob blob = openBlob(session, lobId);
        Integer tableId = blob.getLinkedTable(getTxc(session)).get();
        if (tableId == null) {
            return;
        }
        String schemaName = context.getServiceManager().getSchemaManager().getAis(context.getSession()).getTable(tableId).getName().getSchemaName();
        if (!securityService.isAccessible(context.getSession(), schemaName)) {
            throw new LobNotFoundException(lobId.toString());
        }
    }

    private SQLBlob openBlob(Session session, UUID lobId) {
        if(existsLob(session, lobId)) {
            return new SQLBlob(getLobSubspace(lobId));
        }
        else {
            throw new LobNotFoundException(lobId.toString());
        }
    }
    
    private Subspace getLobSubspace(UUID lobId) {
        Tuple2 tuple = new Tuple2();
        return lobDirectory.get(tuple.add(lobId));
    }
    
    private Transaction getTxc(Session session) {
        return transactionService.getTransaction(session).getTransaction();
    } 
    
    @Override
    public void start() {
        this.fdbHolder = serviceManager.getServiceByClass(FDBHolder.class);
        TransactionService ts = serviceManager.getServiceByClass(TransactionService.class);
        if (ts instanceof FDBTransactionService) {
            this.transactionService = (FDBTransactionService)ts;
        }
        this.lobDirectory = fdbHolder.getRootDirectory().createOrOpen(fdbHolder.getTransactionContext(), Arrays.asList(LOB_DIRECTORY)).get();
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
        stop();
    }
}
