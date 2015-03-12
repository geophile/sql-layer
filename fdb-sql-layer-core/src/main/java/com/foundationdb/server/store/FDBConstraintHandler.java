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
package com.foundationdb.server.store;

import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.server.store.format.FDBStorageDescription;
import com.foundationdb.server.types.service.TypesRegistryService;

public class FDBConstraintHandler extends ConstraintHandler<FDBStore,FDBStoreData,FDBStorageDescription>
{
    private final FDBTransactionService txnService;

    public FDBConstraintHandler(FDBStore store, ConfigurationService config, TypesRegistryService typesRegistryService, ServiceManager serviceManager, FDBTransactionService txnService) {
        super(store, config, typesRegistryService, serviceManager);
        this.txnService = txnService;
    }

    @Override
    protected void checkReferencing(Session session, Index index, FDBStoreData storeData,
            Row row, ForeignKey foreignKey, String operation) {
        assert index.isUnique() : index;
        TransactionState txn = txnService.getTransaction(session);
        FDBPendingIndexChecks.CheckPass finalPass =
            txn.isDeferred(foreignKey) ?
            FDBPendingIndexChecks.CheckPass.TRANSACTION :
            FDBPendingIndexChecks.CheckPass.ROW;
        FDBPendingIndexChecks.PendingCheck<?> check =
            FDBPendingIndexChecks.foreignKeyReferencingCheck(session, txn, index, storeData.persistitKey,
                                                             foreignKey, finalPass, operation);
        if (txn.getForceImmediateForeignKeyCheck() ||
            ((finalPass == FDBPendingIndexChecks.CheckPass.ROW) &&
            ((txn.getIndexChecks(false) == null) || !txn.getIndexChecks(false).isDelayed()))) {
            check.blockUntilReady(txn);
            if (!check.check(session, txn, index)) {
                notReferencing(session, index, storeData, row, foreignKey, operation);
            }
        }
        else {
            txn.getIndexChecks(true).add(session, txn, index, check);
        }
    
    }
    
    @Override
    protected void checkNotReferenced(Session session, Index index, FDBStoreData storeData,
                                        Row row, ForeignKey foreignKey,
                                        boolean selfReference, ForeignKey.Action action,
                                        String operation) {
        TransactionState txn = txnService.getTransaction(session);
        FDBPendingIndexChecks.CheckPass finalPass =
            (action == ForeignKey.Action.RESTRICT) ?
            FDBPendingIndexChecks.CheckPass.ROW :
            txn.isDeferred(foreignKey) ?
            FDBPendingIndexChecks.CheckPass.TRANSACTION :
            FDBPendingIndexChecks.CheckPass.STATEMENT;
        FDBPendingIndexChecks.PendingCheck<?> check =
            FDBPendingIndexChecks.foreignKeyNotReferencedCheck(session, txn, index, storeData.persistitKey, (row == null),
                                                               foreignKey, selfReference, finalPass, operation);
        if (txn.getForceImmediateForeignKeyCheck() ||
            ((finalPass == FDBPendingIndexChecks.CheckPass.ROW) &&
            ((txn.getIndexChecks(false) == null) || !txn.getIndexChecks(false).isDelayed()))) {
            check.blockUntilReady(txn);
            if (!check.check(session, txn, index)) {
                if (row == null) {
                    // Need actual key found for error message.
                    FDBStoreDataHelper.unpackTuple(index, storeData.persistitKey, check.getRawKey());
                }
                stillReferenced(session, index, storeData, row, foreignKey, operation);
            }
        }
        else {
            txn.getIndexChecks(true).add(session, txn, index, check);
        }
    }
}
