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

import com.foundationdb.server.error.FDBAdapterException;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.server.test.it.FDBITBase;
import com.foundationdb.tuple.Tuple2;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FDBExternalDataChangeIT extends FDBITBase
{
    private static final byte[] BAD_PACKED_VALUE = Tuple2.from(Long.MIN_VALUE).pack();

    // TODO: Remove when clear support is gone (post 1.9.2)
    // Until then, convenient place to test it

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String,String> map = new HashMap<>(super.startupConfigProperties());
        map.put(FDBSchemaManager.CLEAR_INCOMPATIBLE_DATA_PROP, "true");
        return map;
    }

    protected Map<String, String> defaultPropertiesToPreserveOnRestart() {
        Map<String,String> map = new HashMap<>(super.defaultPropertiesToPreserveOnRestart());
        map.putAll(startupConfigProperties());
        return map;
    }

    @Test
    public void autoClearIncompatible() throws Exception {
        // Store some real data
        int tid = createTable("schema", "test", "id int");
        // Break the data
        txnService().run(session(), new Runnable() {
            @Override
            public void run() {
                fdbTxnService().getTransaction(session()).setBytes(fdbSchemaManager().getPackedMetaVerKey(), BAD_PACKED_VALUE);
            }
        });
        safeRestartTestServices();
        // Check that the previous data is gone
        assertEquals("table", null, ais().getTable(tid));
    }

    //
    // Generation is checked for every transaction so a clear will be detected
    //

    @Test
    public void generationCleared() {
        testClear(fdbSchemaManager().getPackedGenKey());
    }

    //
    // Meta and Data Version are only checked when reloading AIS from disk so change generation to cause it
    //

    @Test
    public void dataCleared() {
        testClear(fdbSchemaManager().getPackedDataVerKey());
    }

    @Test
    public void metaCleared() {
        testClear(fdbSchemaManager().getPackedMetaVerKey());
    }

    @Test
    public void dataChange() {
        testChange(fdbSchemaManager().getPackedDataVerKey());
    }

    @Test
    public void metaChange() {
        testChange(fdbSchemaManager().getPackedMetaVerKey());
    }

    private void testClear(final byte[] key) {
        test(key, null, true, FDBSchemaManager.EXTERNAL_CLEAR_MSG);
    }

    private void testChange(final byte[] key) {
        test(key, BAD_PACKED_VALUE, true, FDBSchemaManager.EXTERNAL_VER_CHANGE_MSG);
    }

    private void test(final byte[] key, final byte[] newValue, boolean changeGen, final String expectedMsg) {
        // Ensure latest has been read
        ais();
        // And break it
        txnService().beginTransaction(session());
        try {
            TransactionState txn = fdbTxnService().getTransaction(session());
            if(changeGen) {
                txn.setBytes(fdbSchemaManager().getPackedGenKey(), BAD_PACKED_VALUE);
            }
            if(newValue == null) {
                txn.clearKey(key);
            } else {
                txn.setBytes(key, newValue);
            }
            try {
                fdbSchemaManager().getAis(session());
                fail("expected exception");
            } catch(FDBAdapterException e) {
                if(!e.getMessage().contains(expectedMsg)) {
                    assertEquals("exception message", expectedMsg, e.getMessage());
                }
            }
            // Do not commit broken data
        } finally {
            txnService().rollbackTransaction(session());
        }
    }
}
