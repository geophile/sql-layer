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
package com.foundationdb.server.store.statistics;

import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.MemoryStore;
import com.google.inject.Inject;

public class MemoryIndexStatisticsService extends AbstractIndexStatisticsService
{
    private final MemoryStore store;

    @Inject
    public MemoryIndexStatisticsService(Store store,
                                        TransactionService txnService,
                                        SchemaManager schemaManager,
                                        SessionService sessionService,
                                        ConfigurationService configurationService,
                                        ListenerService listenerService) {
        super(store, txnService, schemaManager, sessionService, configurationService, listenerService);
        this.store = (MemoryStore)store;
    }

    @Override
    protected AbstractStoreIndexStatistics createStoreIndexStatistics() {
        return new MemoryStoreIndexStatistics(store, this);
    }
}
