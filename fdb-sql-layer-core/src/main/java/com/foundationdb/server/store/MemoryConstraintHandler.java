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
import com.foundationdb.ais.model.ForeignKey.Action;
import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.format.MemoryStorageDescription;
import com.foundationdb.server.types.service.TypesRegistryService;

public class MemoryConstraintHandler extends ConstraintHandler<MemoryStore,MemoryStoreData,MemoryStorageDescription>
{
    private final MemoryTransactionService txnService;

    protected MemoryConstraintHandler(MemoryStore store,
                                      MemoryTransactionService txnService,
                                      ConfigurationService config,
                                      TypesRegistryService typesRegistryService,
                                      ServiceManager serviceManager) {
        super(store, config, typesRegistryService, serviceManager);
        this.txnService = txnService;
    }

    @Override
    protected void checkReferencing(Session session,
                                    Index index,
                                    MemoryStoreData storeData,
                                    Row row,
                                    ForeignKey foreignKey,
                                    String operation) {
        assert index.isUnique() : index;
        MemoryIndexChecks.CheckPass finalPass =
            txnService.isDeferred(session, foreignKey) ?
                MemoryIndexChecks.CheckPass.TRANSACTION :
                MemoryIndexChecks.CheckPass.ROW;
        MemoryIndexChecks.IndexCheck check =
            MemoryIndexChecks.foreignKeyReferencingCheck(storeData, index, foreignKey, finalPass, operation);
        txnService.addPendingCheck(session, check);
    }

    @Override
    protected void checkNotReferenced(Session session,
                                      Index index,
                                      MemoryStoreData storeData,
                                      Row row,
                                      ForeignKey foreignKey,
                                      boolean selfReference,
                                      Action action,
                                      String operation) {
        MemoryIndexChecks.CheckPass finalPass;
        if(action == ForeignKey.Action.RESTRICT) {
            finalPass = MemoryIndexChecks.CheckPass.ROW;
        } else if(txnService.isDeferred(session, foreignKey)) {
            finalPass = MemoryIndexChecks.CheckPass.TRANSACTION;
        } else {
            finalPass = MemoryIndexChecks.CheckPass.STATEMENT;
        }
        MemoryIndexChecks.IndexCheck check =
            MemoryIndexChecks.foreignKeyNotReferencedCheck(storeData,
                                                           index,
                                                           (row == null),
                                                           foreignKey,
                                                           selfReference,
                                                           finalPass,
                                                           operation);
        txnService.addPendingCheck(session, check);
    }
}
