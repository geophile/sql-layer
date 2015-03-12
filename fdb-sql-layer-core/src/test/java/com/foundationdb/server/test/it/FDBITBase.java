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
package com.foundationdb.server.test.it;

import com.foundationdb.server.service.metrics.FDBMetricsService;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager.BindingsConfigurationProvider;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBHolder;
import com.foundationdb.server.store.FDBHolderImpl;
import com.foundationdb.server.store.FDBSchemaManager;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.statistics.FDBIndexStatisticsService;
import com.foundationdb.server.store.statistics.IndexStatisticsService;

import java.util.Map;

public class FDBITBase extends ITBase
{
    /** For use by classes that cannot extend this class directly */
    public static BindingsConfigurationProvider doBind(BindingsConfigurationProvider provider) {
        return provider.bind(FDBHolder.class, FDBHolderImpl.class)
                       .bind(MetricsService.class, FDBMetricsService.class)
                       .bind(SchemaManager.class, FDBSchemaManager.class)
                       .bind(Store.class, FDBStore.class)
                       .bind(IndexStatisticsService.class, FDBIndexStatisticsService.class)
                       .bind(TransactionService.class, FDBTransactionService.class);
    }

    @Override
    protected BindingsConfigurationProvider serviceBindingsProvider() {
        return doBind(super.serviceBindingsProvider());
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    protected FDBHolder fdbHolder() {
        return serviceManager().getServiceByClass(FDBHolder.class);
    }

    protected FDBMetricsService fdbMetricsService() {
        return (FDBMetricsService)serviceManager().getServiceByClass(MetricsService.class);
    }

    protected FDBSchemaManager fdbSchemaManager() {
        return (FDBSchemaManager)serviceManager().getServiceByClass(SchemaManager.class);
    }

    protected FDBTransactionService fdbTxnService() {
        return (FDBTransactionService)super.txnService();
    }

    protected FDBStore fdbStore() {
        return (FDBStore)super.store();
    }
}
