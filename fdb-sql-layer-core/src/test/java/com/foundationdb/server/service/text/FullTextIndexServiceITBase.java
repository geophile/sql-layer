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
package com.foundationdb.server.service.text;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Before;

import java.util.Map;

public class FullTextIndexServiceITBase extends ITBase
{
    public static final String SCHEMA = "test";
    protected FullTextIndexServiceImpl fullTextImpl;
    protected Schema schema;
    protected StoreAdapter adapter;
    protected QueryContext queryContext;
    protected QueryBindings queryBindings;
    protected int c;
    protected int o;
    protected int i;
    protected int a;


    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                    .bindAndRequire(FullTextIndexService.class, FullTextIndexServiceImpl.class);
    }

    @Override
    public Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }


    @Before
    public final void castService() {
        fullTextImpl = (FullTextIndexServiceImpl)serviceManager().getServiceByClass(FullTextIndexService.class);
    }

    protected void waitUpdate() {
        fullTextImpl.waitUpdateCycle();
    }
}
